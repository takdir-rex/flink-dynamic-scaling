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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.fail;

/** A mocked input channel. */
public class TestInputChannel extends InputChannel {

    private final Queue<BufferAndAvailabilityProvider> buffers = new ConcurrentLinkedQueue<>();

    private final Collection<Buffer> allReturnedBuffers = new ArrayList<>();

    private final boolean reuseLastReturnBuffer;

    private final boolean notifyChannelNonEmpty;

    private BufferAndAvailabilityProvider lastProvider = null;

    private boolean isReleased = false;

    private Runnable actionOnResumed;

    private boolean isBlocked;

    private int sequenceNumber;

    private int currentBufferSize;

    public TestInputChannel(SingleInputGate inputGate, int channelIndex) {
        this(inputGate, channelIndex, true, false);
    }

    public TestInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            boolean reuseLastReturnBuffer,
            boolean notifyChannelNonEmpty) {
        super(
                inputGate,
                channelIndex,
                new ResultPartitionID(),
                0,
                0,
                new SimpleCounter(),
                new SimpleCounter());
        this.reuseLastReturnBuffer = reuseLastReturnBuffer;
        this.notifyChannelNonEmpty = notifyChannelNonEmpty;
    }

    public TestInputChannel read(Buffer buffer) throws IOException, InterruptedException {
        return read(buffer, Buffer.DataType.DATA_BUFFER);
    }

    public TestInputChannel read(Buffer buffer, @Nullable Buffer.DataType nextType)
            throws IOException, InterruptedException {
        addBufferAndAvailability(new BufferAndAvailability(buffer, nextType, 0, sequenceNumber++));
        if (notifyChannelNonEmpty) {
            notifyChannelNonEmpty();
        }
        return this;
    }

    TestInputChannel readBuffer() throws IOException, InterruptedException {
        return readBuffer(Buffer.DataType.DATA_BUFFER);
    }

    TestInputChannel readBuffer(Buffer.DataType nextType) throws IOException, InterruptedException {
        return read(createBuffer(1), nextType);
    }

    TestInputChannel readEndOfData() throws IOException {
        addBufferAndAvailability(
                new BufferAndAvailability(
                        EventSerializer.toBuffer(EndOfData.INSTANCE, false),
                        Buffer.DataType.EVENT_BUFFER,
                        0,
                        sequenceNumber++));
        return this;
    }

    TestInputChannel readEndOfPartitionEvent() {
        addBufferAndAvailability(
                () -> {
                    setReleased();
                    return Optional.of(
                            new BufferAndAvailability(
                                    EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false),
                                    Buffer.DataType.NONE,
                                    0,
                                    sequenceNumber++));
                });
        return this;
    }

    void addBufferAndAvailability(BufferAndAvailability bufferAndAvailability) {
        buffers.add(() -> Optional.of(bufferAndAvailability));
    }

    void addBufferAndAvailability(BufferAndAvailabilityProvider bufferAndAvailability) {
        buffers.add(bufferAndAvailability);
    }

    // ------------------------------------------------------------------------

    /**
     * Creates test input channels and attaches them to the specified input gate.
     *
     * @return The created test input channels.
     */
    static TestInputChannel[] createInputChannels(
            SingleInputGate inputGate, int numberOfInputChannels) {
        checkNotNull(inputGate);
        checkArgument(numberOfInputChannels > 0);

        TestInputChannel[] mocks = new TestInputChannel[numberOfInputChannels];

        for (int i = 0; i < numberOfInputChannels; i++) {
            mocks[i] = new TestInputChannel(inputGate, i);
        }
        inputGate.setInputChannels(mocks);

        return mocks;
    }

    @Override
    void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {}

    @Override
    Optional<BufferAndAvailability> getNextBuffer() throws IOException, InterruptedException {
        checkState(!isReleased);

        BufferAndAvailabilityProvider provider = buffers.poll();

        if (provider != null) {
            if (reuseLastReturnBuffer) {
                lastProvider = provider;
            }
            Optional<BufferAndAvailability> baa = provider.getBufferAvailability();
            baa.ifPresent((v) -> allReturnedBuffers.add(v.buffer()));
            return baa;
        } else if (lastProvider != null) {
            return lastProvider.getBufferAvailability();
        } else {
            return Optional.empty();
        }
    }

    @Override
    void sendTaskEvent(TaskEvent event) throws IOException {}

    @Override
    boolean isReleased() {
        return isReleased;
    }

    void setReleased() {
        this.isReleased = true;
    }

    @Override
    public void releaseAllResources() throws IOException {
        isReleased = true;
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        currentBufferSize = newBufferSize;
    }

    public int getCurrentBufferSize() {
        return currentBufferSize;
    }

    @Override
    int getBuffersInUseCount() {
        return buffers.size();
    }

    @Override
    public void resumeConsumption() {
        isBlocked = false;
        if (actionOnResumed != null) {
            actionOnResumed.run();
        }
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {}

    @Override
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }

    public void assertReturnedEventsAreRecycled() {
        assertReturnedBuffersAreRecycled(false, true);
    }

    private void assertReturnedBuffersAreRecycled(boolean assertBuffers, boolean assertEvents) {
        for (Buffer b : allReturnedBuffers) {
            if (b.isBuffer() && assertBuffers && !b.isRecycled()) {
                fail("Data Buffer " + b + " not recycled");
            }
            if (!b.isBuffer() && assertEvents && !b.isRecycled()) {
                fail("Event Buffer " + b + " not recycled");
            }
        }
    }

    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean isBlocked) {
        this.isBlocked = isBlocked;
    }

    public void setActionOnResumed(Runnable actionOnResumed) {
        this.actionOnResumed = actionOnResumed;
    }

    interface BufferAndAvailabilityProvider {
        Optional<BufferAndAvailability> getBufferAvailability()
                throws IOException, InterruptedException;
    }
}
