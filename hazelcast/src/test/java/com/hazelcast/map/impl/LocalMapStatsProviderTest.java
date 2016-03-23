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

package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalMapStatsProviderTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Test
    public void testHitsGenerated() {
        hz = createHazelcastInstance();
        IMap<Integer, String> map = hz.getMap("trial");

        for (int i = 0; i < 1000; i++) {
            map.put(i, Integer.toString(i));
            map.get(i);
        }

        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(1000, localMapStats.getHits());
    }

    @Test
    public void testHitsNotGenerated() {
        Config config = new Config();
        hz = createHazelcastInstance(makeConfig(config));
        IMap<Integer, String> map = hz.getMap("trial");

        for (int i = 0; i < 1000; i++) {
            map.put(i, Integer.toString(i));
            map.get(i);
        }

        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(0, localMapStats.getHits());
    }

    private Config makeConfig(Config config) {
        config.setProperty(GroupProperty.ITERATING_MAP_STATS_ENABLED, String.valueOf(false));
        return config;
    }

}