/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.inflightlogging.InFlightLog;
import org.apache.flink.runtime.inflightlogging.InFlightLogIterator;
import org.apache.flink.runtime.inflightlogging.InMemorySubpartitionInFlightLogger;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which allows to reconnecting after failure. Only one
 * view is allowed at a time to read teh subpartition.
 */
public class PipelinedApproximateSubpartition extends PipelinedSubpartition {

    private static final Logger LOG =
            LoggerFactory.getLogger(PipelinedApproximateSubpartition.class);

    private final InFlightLog inFlightLog = new InMemorySubpartitionInFlightLogger();

    @GuardedBy("buffers")
    private InFlightLogIterator<Buffer> inflightReplayIterator;

    private long downstreamCheckpointId = 0;

    private long repliedInFlightLogSIzeCounter = 0;

    public PipelinedApproximateSubpartition(
            int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
        super(index, receiverExclusiveBuffersPerChannel, parent);
    }

    @Nullable
    @Override
    BufferAndBacklog pollBuffer() {
        if (inflightReplayIterator != null) {
            synchronized (buffers) {
                return getReplayedBufferUnsafe();
            }
        }
        BufferAndBacklog result = super.pollBuffer();
        synchronized (buffers) {
            if (result != null) {
                Buffer buffer = result.buffer();
                if (buffer.getDataType() == Buffer.DataType.EVENT_BUFFER) {
                    Buffer eventBuffer = buffer.retainBuffer();
                    CheckpointBarrier barrier = null;
                    try {
                        final AbstractEvent event =
                                EventSerializer.fromBuffer(
                                        eventBuffer, getClass().getClassLoader());
                        barrier =
                                event instanceof CheckpointBarrier
                                        ? (CheckpointBarrier) event
                                        : null;
                    } catch (IOException e) {
                        throw new IllegalStateException(
                                "Should always be able to deserialize in-memory event", e);
                    } finally {
                        eventBuffer.recycleBuffer();
                    }
                    if (barrier != null) {
                        downstreamCheckpointId = barrier.getId();
                        inFlightLog.close();
                    }
                }

                if (buffer.isBuffer()) {
                    ByteBuf nettyByteBuf = buffer.asByteBuf();
                    byte[] buffBytes = new byte[nettyByteBuf.readableBytes()];
                    nettyByteBuf.getBytes(nettyByteBuf.readerIndex(), buffBytes);
                    inFlightLog.log(
                            new NetworkBuffer(
                                    MemorySegmentFactory.wrap(buffBytes),
                                    FreeingBufferRecycler.INSTANCE,
                                    buffer.getDataType(),
                                    buffer.isCompressed(),
                                    buffer.getSize()),
                            downstreamCheckpointId,
                            false);
                }
            }
        }
        return result;
    }

    private BufferAndBacklog getReplayedBufferUnsafe() {
        Buffer buffer = inflightReplayIterator.next();
        Buffer.DataType nextDataType =
                isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE;
        if (inflightReplayIterator.hasNext()) {
            nextDataType = inflightReplayIterator.peekNext().getDataType();
            repliedInFlightLogSIzeCounter += buffer.getSize();
        } else {
            inflightReplayIterator = null;
            LOG.info("Finished replaying inflight log: {} bytes", repliedInFlightLogSIzeCounter);
            repliedInFlightLogSIzeCounter = 0;
        }
        return new BufferAndBacklog(
                buffer,
                getBuffersInBacklogUnsafe()
                        + (inflightReplayIterator != null
                                ? inflightReplayIterator.numberRemaining()
                                : 0),
                nextDataType,
                sequenceNumber++);
    }

    /**
     * To simply the view releasing threading model, {@link
     * PipelinedApproximateSubpartition#releaseView()} is called only before creating a new view.
     *
     * <p>There is still one corner case when a downstream task fails continuously in a short period
     * of time then multiple netty worker threads can createReadView at the same time. TODO: This
     * problem will be solved in FLINK-19774
     */
    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            checkState(!isReleased);

            releaseView();

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView = new PipelinedApproximateSubpartitionView(this, availabilityListener);
        }

        return readView;
    }

    private void releaseView() {
        assert Thread.holdsLock(buffers);
        if (readView != null) {
            // upon reconnecting, two netty threads may require the same view to release
            LOG.debug(
                    "Releasing view of subpartition {} of {}.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView.releaseAllResources();
            readView = null;

            isBlocked = false;
            sequenceNumber = 0;

            // initiate iterator for inFlightLog
            if (inflightReplayIterator != null) {
                inflightReplayIterator.close();
            }
            inflightReplayIterator = inFlightLog.getInFlightIterator(downstreamCheckpointId, 0);
            if (inflightReplayIterator != null) {
                if (!inflightReplayIterator.hasNext()) inflightReplayIterator = null;
            }
        }
    }

    @Override
    public void finishReadRecoveredState(boolean notifyAndBlockOnCompletion) throws IOException {
        // The Approximate Local Recovery can not work with unaligned checkpoint for now, so no need
        // to recover channel state
    }
}
