/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.data.v2.internal.channels;

import com.google.bigtable.v2.PeerInfo;
import com.google.bigtable.v2.SessionClientConfiguration.ChannelPoolConfiguration;
import com.google.bigtable.v2.SessionRequest;
import com.google.bigtable.v2.SessionResponse;
import com.google.bigtable.v2.TelemetryConfiguration;
import com.google.cloud.bigtable.data.v2.internal.csm.attributes.ClientInfo;
import com.google.cloud.bigtable.data.v2.internal.csm.tracers.DebugTagTracer;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Channel pool that routes sessions to AFE groups based on per-(instance, appProfile) affinity.
 *
 * <p>Each (instance, appProfile) pair is routed to a specific AFE discovered lazily via PeerInfo
 * returned on session start. Multiple instances sharing the same AFE reuse the same channel. AFE
 * groups are sized by aggregate load and pruned when idle.
 *
 * <p>Design:
 *
 * <ul>
 *   <li>{@link SimpleAfePool} - one pool per discovered AFE with a single {@code totalStreams}
 *       counter reflecting aggregate load across all instances using that AFE.
 *   <li>{@link DiscoveryPool} - channels pending AFE discovery; sessions migrate to an AFE pool on
 *       the first {@code onBeforeSessionStart} callback.
 *   <li>{@link PoolRouter} - routing table ({@code instanceAfe}) + service loop (prune/consolidate
 *       only; channel creation is reactive via {@link #newStream}).
 * </ul>
 */
public class PoolRouter implements ChannelPool {
  private static final Logger LOGGER = Logger.getLogger(PoolRouter.class.getName());
  private static final AtomicInteger INDEX = new AtomicInteger();

  private final String poolLogId;

  @VisibleForTesting volatile int minGroups;
  @VisibleForTesting volatile int maxGroups;
  @VisibleForTesting volatile int softMaxPerGroup;

  private final ScheduledExecutorService executor;
  private final DebugTagTracer debugTagTracer;

  // Routing table: (instance, appProfile) → last known AFE
  @GuardedBy("this")
  private final Map<ClientInfo, AfeId> instanceAfe = new HashMap<>();

  // One pool per discovered AFE
  @GuardedBy("this")
  private final Map<AfeId, SimpleAfePool> afePools = new LinkedHashMap<>();

  // Channels pending AFE discovery
  @GuardedBy("this")
  private final DiscoveryPool discoveryPool;

  @GuardedBy("this")
  private int totalStreams = 0;

  @GuardedBy("this")
  private boolean closed = false;

  @Nullable private ScheduledFuture<?> serviceFuture = null;

  public PoolRouter(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor) {
    this(channelSupplier, config, "pool", debugTagTracer, executor, Clock.systemUTC());
  }

  public PoolRouter(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      String logName,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor) {
    this(channelSupplier, config, logName, debugTagTracer, executor, Clock.systemUTC());
  }

  public PoolRouter(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      String logName,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor,
      Clock clock) {
    this.poolLogId = String.format("%d-%s", INDEX.getAndIncrement(), logName);
    this.executor = executor;
    this.debugTagTracer = debugTagTracer;
    this.discoveryPool = new DiscoveryPool(channelSupplier, clock);
    updateConfig(config);
  }

  @Override
  public void updateConfig(ChannelPoolConfiguration config) {
    this.minGroups = config.getMinServerCount();
    this.maxGroups = config.getMaxServerCount();
    this.softMaxPerGroup = config.getPerServerSessionCount();
  }

  @Override
  public synchronized void start() {
    // Pre-create minGroups channels so the pool is not empty on first request.
    for (int i = 0; i < minGroups; i++) {
      discoveryPool.addChannel();
    }
    serviceFuture = executor.scheduleAtFixedRate(this::serviceChannels, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (serviceFuture != null) {
      serviceFuture.cancel(false);
    }
    afePools.values().forEach(SimpleAfePool::close);
    discoveryPool.close();
  }

  @Override
  public synchronized SessionStream newStream(
      MethodDescriptor<SessionRequest, SessionResponse> desc,
      CallOptions callOptions,
      ClientInfo clientInfo) {
    if (closed) {
      debugTagTracer.record(TelemetryConfiguration.Level.WARN, "channel_pool_new_stream_failed");
      return new FailingSessionStream(Status.UNAVAILABLE.withDescription("ChannelPool is closed"));
    }

    // Route to the known AFE pool if it has capacity.
    ChannelWrapper channelWrapper = null;
    boolean isDiscovery = false;
    AfeId knownAfe = instanceAfe.get(clientInfo);

    if (knownAfe != null) {
      SimpleAfePool afePool = afePools.get(knownAfe);
      if (afePool != null && afePool.totalStreams < softMaxPerGroup) {
        channelWrapper = afePool.getChannel();
        if (channelWrapper != null) {
          afePool.totalStreams++;
        }
      }
    }

    if (channelWrapper == null) {
      // No known AFE, pool at capacity, or pool has no channels — route through discovery.
      log(
          Level.FINE,
          "Routing through discovery for %s (known AFE: %s)",
          clientInfo.getInstanceName(),
          knownAfe);
      channelWrapper = discoveryPool.pickOrCreate(softMaxPerGroup);
      discoveryPool.totalStreams++;
      isDiscovery = true;
    }

    channelWrapper.numOutstanding++;
    totalStreams++;

    final ChannelWrapper finalWrapper = channelWrapper;
    final boolean finalIsDiscovery = isDiscovery;

    ClientCall<SessionRequest, SessionResponse> call =
        channelWrapper.channel.newCall(desc, callOptions);

    return new SessionStreamImpl(call) {
      @Nullable AfeId resolvedAfe = null;

      @Override
      public void start(Listener responseListener, Metadata headers) {
        super.start(
            new ForwardingListener(responseListener) {
              @Override
              public void onBeforeSessionStart(PeerInfo peerInfo) {
                AfeId discoveredAfe = AfeId.extract(peerInfo);
                synchronized (PoolRouter.this) {
                  resolvedAfe = discoveredAfe;
                  onAfeResolved(finalWrapper, discoveredAfe, clientInfo, finalIsDiscovery);
                }
                super.onBeforeSessionStart(peerInfo);
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                synchronized (PoolRouter.this) {
                  totalStreams--;
                  finalWrapper.numOutstanding--;
                  if (resolvedAfe != null) {
                    SimpleAfePool pool = afePools.get(resolvedAfe);
                    if (pool != null) {
                      pool.totalStreams--;
                    }
                  } else {
                    discoveryPool.totalStreams--;
                  }
                  if (shouldRecycleChannel(status)) {
                    recycleChannel(finalWrapper, resolvedAfe);
                  }
                }
                super.onClose(status, trailers);
              }
            },
            headers);
      }
    };
  }

  // Update routing table and (for discovery channels) migrate the channel to the AFE pool.
  // Must be called under PoolRouter lock.
  @GuardedBy("this")
  private void onAfeResolved(
      ChannelWrapper wrapper, AfeId discoveredAfe, ClientInfo clientInfo, boolean wasDiscovery) {
    // Always update routing table so future sessions route directly to this AFE.
    instanceAfe.put(clientInfo, discoveredAfe);

    if (!wasDiscovery) {
      // Channel already homed to an AFE pool. If the AFE changed (server rebalance), just update
      // the routing table. The channel stays in its current pool until it drains; future sessions
      // for this instance route to the new AFE via the updated table.
      return;
    }

    // Migrate channel from discovery pool to the resolved AFE pool.
    // Reattribute the stream from discovery to the AFE pool.
    discoveryPool.channels.remove(wrapper);
    discoveryPool.totalStreams--;

    wrapper.afeId = discoveredAfe;
    SimpleAfePool afePool = afePools.computeIfAbsent(discoveredAfe, SimpleAfePool::new);
    afePool.channels.addLast(wrapper);
    afePool.totalStreams++;
  }

  @GuardedBy("this")
  private void recycleChannel(ChannelWrapper wrapper, @Nullable AfeId afeId) {
    if (wrapper.channel.isShutdown()) {
      return;
    }
    if (afeId != null) {
      SimpleAfePool pool = afePools.get(afeId);
      if (pool != null) {
        pool.channels.remove(wrapper);
        if (pool.channels.isEmpty() && pool.totalStreams == 0) {
          afePools.remove(afeId);
          instanceAfe.values().removeIf(v -> Objects.equals(v, afeId));
        }
      }
    } else {
      discoveryPool.channels.remove(wrapper);
    }
    wrapper.channel.shutdown();
  }

  private static boolean shouldRecycleChannel(Status status) {
    if (status.getCode() == Code.UNIMPLEMENTED) {
      return true;
    }
    // TODO: replace this with a flag in ErrorDetails
    if (status.getDescription() != null
        && status.getDescription().toLowerCase(Locale.ENGLISH).contains("server is draining")) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  void serviceChannels() {
    try {
      serviceChannelsSafe();
    } catch (Exception e) {
      debugTagTracer.record(TelemetryConfiguration.Level.WARN, "service_channel_failure");
      log(Level.WARNING, "Failed to service channels", e);
    }
  }

  private synchronized void serviceChannelsSafe() {
    log(Level.FINE, "Servicing channels");
    dumpState();

    // Consolidate each AFE pool to at most 1 channel. Extra channels accumulate when concurrent
    // discovery sessions happen to resolve to the same AFE.
    for (SimpleAfePool pool : afePools.values()) {
      while (pool.channels.size() > 1) {
        // Remove oldest channels first; gRPC graceful shutdown lets in-flight streams complete.
        pool.channels.removeFirst().channel.shutdown();
      }
    }

    // Prune idle AFE pools when there are more than the desired number of groups.
    int desired = computeDesiredGroups();
    if (desired < afePools.size()) {
      List<SimpleAfePool> candidates =
          afePools.values().stream()
              .filter(p -> p.totalStreams == 0 && !p.channels.isEmpty())
              .sorted(Comparator.comparing(p -> p.channels.peek().createdAt))
              .limit(afePools.size() - desired)
              .collect(Collectors.toList());

      for (SimpleAfePool pool : candidates) {
        log(Level.FINE, "Pruning idle AFE pool for %s", pool.afeId);
        AfeId pruned = pool.afeId;
        afePools.remove(pruned);
        instanceAfe.values().removeIf(v -> Objects.equals(v, pruned));
        pool.close();
      }
    }

    log(Level.FINE, "Done servicing channels");
    dumpState();
  }

  @GuardedBy("this")
  private int computeDesiredGroups() {
    // Size by aggregate load per AFE. Applying per-instance floors would inflate channel counts
    // proportionally to the number of factory children even when they share the same AFE.
    int total = 0;
    for (SimpleAfePool pool : afePools.values()) {
      if (pool.totalStreams > 0) {
        int desired = (int) Math.ceil((float) pool.totalStreams / softMaxPerGroup * 2);
        total += Math.min(desired, maxGroups);
      }
    }
    if (discoveryPool.totalStreams > 0) {
      int desired = (int) Math.ceil((float) discoveryPool.totalStreams / softMaxPerGroup * 2);
      total += Math.min(desired, maxGroups);
    }
    // Pool-level minimum floor — not applied per instance or per AFE.
    return Math.max(total, minGroups);
  }

  @GuardedBy("this")
  private void dumpState() {
    if (!LOGGER.isLoggable(Level.FINE)) {
      return;
    }
    String distribution =
        afePools.entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getKey().toString()))
            .map(e -> String.format("%s:%d", e.getKey(), e.getValue().totalStreams))
            .collect(Collectors.joining(", "));
    log(
        Level.FINE,
        "PoolRouter afePools=%d discoveryChannels=%d discoveryStreams=%d totalStreams=%d routing=[%s]",
        afePools.size(),
        discoveryPool.channels.size(),
        discoveryPool.totalStreams,
        totalStreams,
        distribution);
  }

  private void log(Level level, String msg, Throwable throwable) {
    LOGGER.log(level, String.format("[%s] %s", poolLogId, msg), throwable);
  }

  private void log(Level level, String format, Object... args) {
    LOGGER.log(level, String.format("[%s] %s", poolLogId, String.format(format, args)));
  }

  // --- Inner classes ---

  /** Manages channels homed to a single AFE. */
  static class SimpleAfePool {
    final AfeId afeId;
    // Channels ordered oldest-first; new channels are appended to the back.
    final Deque<ChannelWrapper> channels = new ArrayDeque<>();
    // Aggregate stream count across all instances using this AFE.
    int totalStreams = 0;

    SimpleAfePool(AfeId afeId) {
      this.afeId = afeId;
    }

    @Nullable
    ChannelWrapper getChannel() {
      return channels.peekFirst();
    }

    void close() {
      channels.forEach(w -> w.channel.shutdown());
      channels.clear();
    }
  }

  /** Holds channels whose AFE has not yet been discovered. */
  static class DiscoveryPool {
    private final Supplier<ManagedChannel> channelSupplier;
    private final Clock clock;
    final Deque<ChannelWrapper> channels = new ArrayDeque<>();
    int totalStreams = 0;

    DiscoveryPool(Supplier<ManagedChannel> channelSupplier, Clock clock) {
      this.channelSupplier = channelSupplier;
      this.clock = clock;
    }

    ChannelWrapper pickOrCreate(int softMaxPerGroup) {
      return channels.stream()
          .filter(w -> w.numOutstanding < softMaxPerGroup / 2)
          .min(Comparator.comparingInt(w -> w.numOutstanding))
          .orElseGet(this::addChannel);
    }

    private ChannelWrapper addChannel() {
      ChannelWrapper wrapper = new ChannelWrapper(channelSupplier.get(), clock);
      channels.addFirst(wrapper);
      return wrapper;
    }

    void close() {
      channels.forEach(w -> w.channel.shutdown());
      channels.clear();
    }
  }

  static class ChannelWrapper {
    @Nullable AfeId afeId;
    final ManagedChannel channel;
    final Instant createdAt;
    int numOutstanding = 0;

    ChannelWrapper(ManagedChannel channel, Clock clock) {
      this.channel = channel;
      this.createdAt = Instant.now(clock);
    }
  }

  private static class AfeId {
    private final long id;

    static AfeId extract(PeerInfo peerInfo) {
      return new AfeId(peerInfo.getApplicationFrontendId());
    }

    private AfeId(long id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AfeId)) {
        return false;
      }
      return id == ((AfeId) o).id;
    }

    @Override
    public int hashCode() {
      return com.google.common.base.Objects.hashCode(id);
    }

    @Override
    public String toString() {
      return Long.toString(id);
    }
  }
}
