/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.map.impl.SizeEstimator;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

/**
 * Size estimator for Near Cache.
 */
public class NearCacheSizeEstimator implements SizeEstimator<NearCacheRecord> {

    private static final AtomicLongFieldUpdater<NearCacheSizeEstimator> SIZE = AtomicLongFieldUpdater
            .newUpdater(NearCacheSizeEstimator.class, "size");

    @SuppressWarnings("unused")
    private volatile long size;

    public NearCacheSizeEstimator() {
    }

    @Override
    public long calculateSize(NearCacheRecord record) {
        // immediate check nothing to do if record is null
        if (record == null) {
            return 0;
        }
        final long cost = record.getCost();
        // if cost is zero, type of cached object is not Data (then omit)
        if (cost == 0) {
            return 0;
        }
        final int numberOfIntegers = 4;
        long size = 0;
        // entry size in CHM
        size += numberOfIntegers * INT_SIZE_IN_BYTES;
        size += cost;
        return size;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void add(long size) {
        SIZE.addAndGet(this, size);
    }

    @Override
    public void reset() {
        SIZE.set(this, 0L);
    }
}
