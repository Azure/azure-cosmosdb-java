/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Pool of Byte Buffers, this helps in re-using memory
 */
public class ByteBufferPool {

    public class ByteBufferWrapper {
        private final PoolSegment poolSegment;
        private final ByteBuffer byteBuffer;
        private ByteBufferWrapper(ByteBuffer byteBuffer, PoolSegment poolSegment) {
            this.byteBuffer = byteBuffer;
            this.poolSegment = poolSegment;
        }

        public ByteBuffer getByteBuffer() {
            return this.byteBuffer;
        }
    }

    private class PoolSegment {
        public PoolSegment(int byteBufferSize, ConcurrentLinkedDeque<ByteBufferWrapper> byteBuffersPool) {
            this.byteBufferSize = byteBufferSize;
            this.byteBuffersPool = byteBuffersPool;
        }

        private final int byteBufferSize;
        private final ConcurrentLinkedDeque<ByteBufferWrapper> byteBuffersPool;
    }

    private final ArrayList<PoolSegment> poolSegmentList = new ArrayList<>();


    private final static ByteBufferPool instant = new ByteBufferPool();

    public static ByteBufferPool getInstant() {
        return instant;
    }

    private final Logger logger = LoggerFactory.getLogger(ByteBufferPool.class);

    private ByteBufferPool() {
        logger.debug("Initializing ByteBuffer Pool");
        long totalSize = 0;

        for(int byteBufferSize = 1024, segmentSize = 1024; segmentSize > 0; byteBufferSize *= 2, segmentSize /= 2) {
            logger.debug("Creating pool segment: ByteBuffer size {}, pool segment size {}", byteBufferSize, segmentSize);
            poolSegmentList.add(createByteBufferPoolSegment(byteBufferSize, segmentSize));
            totalSize += (byteBufferSize * segmentSize);
        }

        logger.debug("Total ByteBuffer Pool Size {}", totalSize);
    }

    private PoolSegment createByteBufferPoolSegment(int byteBufferSize, int count) {
        ConcurrentLinkedDeque<ByteBufferWrapper> deq = new ConcurrentLinkedDeque<>();
        PoolSegment poolSegment = new PoolSegment(count, deq);

        for(int i = 0; i < count; i++) {
            deq.add(new ByteBufferWrapper(ByteBuffer.allocate(byteBufferSize), poolSegment));
        }

        return new PoolSegment(byteBufferSize, deq);
    }

    private int findLowestIndex(int size) {
        for (int i = 0; i < poolSegmentList.size(); i++) {
            if (poolSegmentList.get(i).byteBufferSize >= size) {
                return i;
            }
        }

        return -1;
    }

    public ByteBufferWrapper lease(int size) {
        int poolIndex = findLowestIndex(size);
        if (poolIndex == -1) {
            logger.info("Requested byte buffer size {} is greater than the max the pool supports, creating a garbage collectable instance.", size);

            return new ByteBufferWrapper(ByteBuffer.allocate(size), null);
        }

        for (int i = poolIndex; i < poolSegmentList.size(); i++) {
            ByteBufferWrapper byteBuffer = poolSegmentList.get(i).byteBuffersPool.poll();
            if (byteBuffer != null) {
                return byteBuffer;
            }
        }

        logger.warn("Configured Byte Buffer Pool is not sufficient, creating new garbage collectable instance");
        return new ByteBufferWrapper(ByteBuffer.allocate(size), null);
    }

    public void release(ByteBufferWrapper byteBufferWrapper) {
        PoolSegment parentPoolSegment = byteBufferWrapper.poolSegment;

        if (parentPoolSegment != null) {
            parentPoolSegment.byteBuffersPool.add(byteBufferWrapper);
            return;
        }

        // else let it get garbage collected
    }
}