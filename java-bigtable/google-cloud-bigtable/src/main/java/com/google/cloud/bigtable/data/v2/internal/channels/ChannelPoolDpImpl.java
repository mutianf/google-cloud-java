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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
 * Proof of concept channel pool that avoids parallel channels to the same afe. The pool is
 * dynamically sized based on outstanding streams and tries to limit each channel to at most 10
 * streams.
 */
public class ChannelPoolDpImpl implements ChannelPool {
  private static final Logger LOGGER = Logger.getLogger(ChannelPoolDpImpl.class.getName());
  private static final String DEFAULT_LOG_NAME = "pool";
  private static final AtomicInteger INDEX = new AtomicInteger();

  private final String poolLogId;

  @VisibleForTesting volatile int minGroups;
  @VisibleForTesting volatile int maxGroups;
  @VisibleForTesting volatile int softMaxPerGroup;

  private final Clock clock;
  private final Supplier<ManagedChannel> channelSupplier;
  private final ScheduledExecutorService executor;

  private final DebugTagTracer debugTagTracer;

  @GuardedBy("this")
  private final List<AfeChannelGroup> channelGroups;

  @GuardedBy("this")
  private final AfeChannelGroup startingGroup;

  @GuardedBy("this")
  private int totalStreams = 0;

  // Per-instance stream counts for sessions still in the startingGroup (AFE not yet known).
  @GuardedBy("this")
  private final Map<ClientInfo, Integer> startingGroupStreamsPerInstance = new HashMap<>();

  private ScheduledFuture<?> serviceFuture = null;

  @GuardedBy("this")
  private boolean closed = false;

  public ChannelPoolDpImpl(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor) {
    this(channelSupplier, config, DEFAULT_LOG_NAME, debugTagTracer, executor, Clock.systemUTC());
  }

  public ChannelPoolDpImpl(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      String logName,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor) {
    this(channelSupplier, config, logName, debugTagTracer, executor, Clock.systemUTC());
  }

  public ChannelPoolDpImpl(
      Supplier<ManagedChannel> channelSupplier,
      ChannelPoolConfiguration config,
      String logName,
      DebugTagTracer debugTagTracer,
      ScheduledExecutorService executor,
      Clock clock) {
    this.poolLogId = String.format("%d-%s", INDEX.getAndIncrement(), logName);
    this.clock = clock;
    this.channelSupplier = channelSupplier;
    this.executor = executor;
    updateConfig(config);
    channelGroups = new ArrayList<>();
    startingGroup = new AfeChannelGroup(null);
    this.debugTagTracer = debugTagTracer;
  }

  @Override
  public void updateConfig(ChannelPoolConfiguration config) {
    this.minGroups = config.getMinServerCount();
    this.maxGroups = config.getMaxServerCount();
    this.softMaxPerGroup = config.getPerServerSessionCount();
  }

  @Override
  public synchronized void start() {
    serviceChannels();
    serviceFuture = executor.scheduleAtFixedRate(this::serviceChannels, 1, 1, TimeUnit.MINUTES);
  }

  @Override
  public synchronized void close() {
    closed = true;

    if (serviceFuture != null) {
      serviceFuture.cancel(false);
    }

    channelGroups.stream()
        .flatMap(g -> g.channels.stream())
        .forEach(channelWrapper -> channelWrapper.channel.shutdown());
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

    // Find the AFE group with fewest streams for THIS instance that still has headroom.
    Optional<AfeChannelGroup> maybeGroup =
        channelGroups.stream()
            .filter(g -> g.streamsFor(clientInfo) < softMaxPerGroup)
            .min(Comparator.comparingInt(g -> g.streamsFor(clientInfo)));

    Optional<ChannelWrapper> maybeChannel =
        maybeGroup.flatMap(g -> g.channels.stream().findFirst());

    final ChannelWrapper channelWrapper =
        maybeChannel.orElseGet(
            () -> {
              log(
                  Level.FINE,
                  "Couldn't find an existing channel with capacity for %s,"
                      + " num outstanding streams: %d, num groups: %d",
                  clientInfo.getInstanceName(),
                  channelGroups.stream().mapToInt(AfeChannelGroup::totalStreams).sum(),
                  channelGroups.size());

              return startingGroup.channels.stream()
                  .filter(w -> w.numOutstanding < softMaxPerGroup / 2)
                  .min(Comparator.comparingInt(w -> w.numOutstanding))
                  .map(
                      w -> {
                        log(Level.FINE, "Using a channel thats already connecting");
                        return w;
                      })
                  .orElseGet(
                      () -> {
                        log(Level.FINE, "Creating a new channel");
                        return addChannel();
                      });
            });

    channelWrapper.numOutstanding++;
    totalStreams++;
    boolean isStarting = channelWrapper.group == startingGroup;
    if (isStarting) {
      startingGroupStreamsPerInstance.merge(clientInfo, 1, Integer::sum);
    } else {
      channelWrapper.group.incrementStreams(clientInfo);
    }

    ClientCall<SessionRequest, SessionResponse> innerCall =
        channelWrapper.channel.newCall(desc, callOptions);

    return new SessionStreamImpl(innerCall) {
      @Nullable AfeId afeId = null;

      @Override
      public void start(Listener responseListener, Metadata headers) {
        super.start(
            new ForwardingListener(responseListener) {
              @Override
              public void onBeforeSessionStart(PeerInfo peerInfo) {
                afeId = AfeId.extract(peerInfo);
                synchronized (ChannelPoolDpImpl.this) {
                  boolean wasStarting = channelWrapper.group == startingGroup;
                  rehomeChannel(channelWrapper, afeId);
                  if (wasStarting) {
                    // Reattribute from starting group to the resolved AFE group.
                    startingGroupStreamsPerInstance.merge(clientInfo, -1, Integer::sum);
                    startingGroupStreamsPerInstance.remove(clientInfo, 0);
                    channelWrapper.group.incrementStreams(clientInfo);
                  }
                }
                super.onBeforeSessionStart(peerInfo);
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                synchronized (ChannelPoolDpImpl.this) {
                  if (afeId != null) {
                    channelWrapper.group.decrementStreams(clientInfo);
                  } else {
                    startingGroupStreamsPerInstance.merge(clientInfo, -1, Integer::sum);
                    startingGroupStreamsPerInstance.remove(clientInfo, 0);
                  }
                  releaseChannel(channelWrapper, status);
                }
                super.onClose(status, trailers);
              }
            },
            headers);
      }
    };
  }

  @GuardedBy("this")
  private ChannelWrapper addChannel() {
    debugTagTracer.checkPrecondition(
        !closed, "channel_pool_add_channel_failure", "Channel pool is closed");
    log(Level.FINE, "Adding a new channel");
    ChannelWrapper wrapped = new ChannelWrapper(startingGroup, channelSupplier.get(), clock);
    startingGroup.channels.addFirst(wrapped);
    return wrapped;
  }

  @GuardedBy("this")
  private void removeGroup(AfeChannelGroup group) {
    log(Level.FINE, "Removing group: %s", group.afeId);
    group.channels.forEach(c -> c.channel.shutdown());
    channelGroups.remove(group);
    // TODO: need to handle a group being added right after it was removed with lingering sessions
  }

  @GuardedBy("this")
  private void rehomeChannel(ChannelWrapper channelWrapper, AfeId afeId) {
    // No need to rehome recycled channels.
    if (channelWrapper.channel.isShutdown()) {
      return;
    }

    AfeChannelGroup origGroup = channelWrapper.group;

    if (Objects.equals(origGroup.afeId, afeId)) {
      return;
    }

    log(Level.FINE, "Rehoming channel from: %s to %s", origGroup.afeId, afeId);

    // Re-home the channel
    AfeChannelGroup newGroup =
        channelGroups.stream()
            .filter(g -> Objects.equals(g.afeId, afeId))
            .findAny()
            .orElseGet(
                () -> {
                  AfeChannelGroup g = new AfeChannelGroup(afeId);
                  channelGroups.add(g);
                  return g;
                });
    channelWrapper.group = newGroup;
    origGroup.channels.remove(channelWrapper);
    if (origGroup != startingGroup && origGroup.channels.isEmpty()) {
      channelGroups.remove(origGroup);
    }
    newGroup.channels.add(channelWrapper);

    return;
  }

  // Update accounting when a stream is closed and releases its channel.
  // Per-instance stream counts are decremented by the caller (onClose callback in newStream).
  @GuardedBy("this")
  private void releaseChannel(ChannelWrapper channelWrapper, Status status) {
    totalStreams--;
    channelWrapper.numOutstanding--;

    if (shouldRecycleChannel(status)) {
      recycleChannel(channelWrapper);
    }
  }

  private static boolean shouldRecycleChannel(Status status) {
    if (status.getCode() == Code.UNIMPLEMENTED) {
      return true;
    }

    // TODO: replace this with a flag in the ErrorDetails
    if (status.getDescription() != null
        && status.getDescription().toLowerCase(Locale.ENGLISH).contains("server is draining")) {
      return true;
    }

    return false;
  }

  @GuardedBy("this")
  private void recycleChannel(ChannelWrapper channelWrapper) {
    if (channelWrapper.channel.isShutdown()) {
      // Channel is already recycled.
      return;
    }

    channelWrapper.group.channels.remove(channelWrapper);
    channelWrapper.channel.shutdown();
    // Checking for starting group because we don't want to delete the stating group.
    if (channelWrapper.group.channels.isEmpty() && channelWrapper.group != startingGroup) {
      removeGroup(channelWrapper.group);
    }
    addChannel();
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

  /*
  - add new group if existing groups are overflowing
  - shutdown older channels within a group
   */
  private synchronized void serviceChannelsSafe() {
    log(Level.FINE, "Servicing channels");
    dumpState();

    // Thin out the channels in each group, so that each AFEGroup only has 1 channel.
    for (AfeChannelGroup group : channelGroups) {
      if (LOGGER.isLoggable(Level.FINEST) && group.channels.size() > 1) {
        log(
            Level.FINEST,
            "Shutting down %d parallel channels for %s",
            group.channels.size() - 1,
            group.afeId);
      }
      while (group.channels.size() > 1) {
        group.channels.removeFirst().channel.shutdown();
      }
    }

    // Compute desired group count as the sum of per-instance needs. Each instance sizes
    // independently based on its own stream load; since different instances go to different AFEs
    // their desired group counts add up rather than overlap.
    int desiredGroups = computeDesiredGroups();

    // Prune only groups that are completely idle across all instances.
    if (desiredGroups < channelGroups.size()) {
      Iterator<AfeChannelGroup> it =
          channelGroups.stream()
              .filter(g -> g.totalStreams() == 0)
              .sorted(Comparator.comparing(g -> g.channels.peek().createdAt))
              .limit(channelGroups.size() - desiredGroups)
              .iterator();

      while (it.hasNext()) {
        AfeChannelGroup group = it.next();
        log(
            Level.FINE,
            "Removing idle channel for %s due to lack of concurrency",
            group.afeId);
        removeGroup(group);
      }
    } else if (desiredGroups > channelGroups.size() + startingGroup.channels.size()) {
      int toAdd = desiredGroups - channelGroups.size() - startingGroup.channels.size();
      log(Level.FINE, "Adding %d channels", toAdd);
      for (int i = 0; i < toAdd; i++) {
        addChannel();
      }
    }

    log(Level.FINE, "Done servicing channels");
    dumpState();
  }

  @GuardedBy("this")
  private int computeDesiredGroups() {
    // Compute desired groups per AFE based on aggregate load across all instances.
    // Applying minGroups per-instance would inflate channel count proportionally
    // to the number of factory children even when they share the same AFE.
    int total = 0;
    for (AfeChannelGroup group : channelGroups) {
      int groupStreams = group.totalStreams();
      if (groupStreams > 0) {
        int desired = (int) Math.ceil(((float) groupStreams / softMaxPerGroup) * 2);
        total += Math.min(desired, maxGroups);
      }
    }
    // Account for streams still in the starting group (not yet assigned to an AFE).
    int startingStreams =
        startingGroupStreamsPerInstance.values().stream().mapToInt(Integer::intValue).sum();
    if (startingStreams > 0) {
      int desired = (int) Math.ceil(((float) startingStreams / softMaxPerGroup) * 2);
      total += Math.min(desired, maxGroups);
    }
    // Pool-level minimum floor — not applied per instance.
    return Math.max(total, minGroups);
  }

  private void log(Level level, String msg, Throwable throwable) {
    LOGGER.log(level, String.format("[%s] %s", poolLogId, msg), throwable);
  }

  private void log(Level level, String format, Object... args) {
    LOGGER.log(level, String.format("[%s] %s", poolLogId, String.format(format, args)));
  }

  @GuardedBy("this")
  void dumpState() {
    if (!LOGGER.isLoggable(Level.FINE)) {
      return;
    }

    int channels =
        channelGroups.stream().mapToInt((AfeChannelGroup chg) -> chg.channels.size()).sum();
    String distribution =
        channelGroups.stream()
            .sorted(Comparator.comparing(g -> g.afeId.toString()))
            .map(g -> String.format("%d", g.totalStreams()))
            .collect(Collectors.joining(", "));

    log(
        Level.FINE,
        "ChannelPool channelGroups: "
            + channelGroups.size()
            + ", channels: "
            + channels
            + ", starting channels: "
            + startingGroup.channels.size()
            + ", totalStreams: "
            + totalStreams
            + ", AFEs: "
            + channelGroups.size()
            + ", distribution: ["
            + distribution
            + "]");

    if (LOGGER.isLoggable(Level.FINEST)) {
      String afeToSessions =
          channelGroups.stream()
              .sorted(Comparator.comparing(g -> g.afeId.toString()))
              .map(
                  g -> {
                    String perInstance =
                        g.streamsPerInstance.entrySet().stream()
                            .map(e -> String.format("  %s: %d", e.getKey(), e.getValue()))
                            .collect(Collectors.joining("\n"));
                    return String.format("%s (total=%d):\n%s", g.afeId, g.totalStreams(), perInstance);
                  })
              .collect(Collectors.joining("\n"));
      log(Level.FINEST, "ChannelPool session distribution:\n%s", afeToSessions);
    }
  }

  static class AfeChannelGroup {
    private final AfeId afeId;
    private final Deque<ChannelWrapper> channels;
    final Map<ClientInfo, Integer> streamsPerInstance = new HashMap<>();

    public AfeChannelGroup(AfeId afeId) {
      this.afeId = afeId;
      channels = new ArrayDeque<>();
    }

    int streamsFor(ClientInfo clientInfo) {
      return streamsPerInstance.getOrDefault(clientInfo, 0);
    }

    int totalStreams() {
      return streamsPerInstance.values().stream().mapToInt(Integer::intValue).sum();
    }

    void incrementStreams(ClientInfo clientInfo) {
      streamsPerInstance.merge(clientInfo, 1, Integer::sum);
    }

    void decrementStreams(ClientInfo clientInfo) {
      streamsPerInstance.merge(clientInfo, -1, Integer::sum);
      streamsPerInstance.remove(clientInfo, 0);
    }
  }

  static class ChannelWrapper {
    private AfeChannelGroup group;
    private final ManagedChannel channel;
    private final Instant createdAt;
    private int numOutstanding = 0;

    public ChannelWrapper(AfeChannelGroup group, ManagedChannel channel, Clock clock) {
      this.group = group;
      this.channel = channel;
      createdAt = Instant.now(clock);
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
