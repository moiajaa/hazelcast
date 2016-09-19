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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapStandaloneTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    private HazelcastInstance member1;
    private HazelcastInstance member2;
    private String standaloneNearcacheMap = "";

    @Before
    public void setup() {
        setupCluster();
        setupClient();
    }

    private void setupClient() {
        ClientConfig clientConfig = new XmlClientConfigBuilder().build();
        final NearCacheConfig standaloneClientNearcacheConfig = new NearCacheConfig();
        standaloneClientNearcacheConfig.setName(standaloneNearcacheMap);
        standaloneClientNearcacheConfig.setMaxSize(100);
        clientConfig.addNearCacheConfig(standaloneClientNearcacheConfig);
        client = hazelcastFactory.newHazelcastClient(clientConfig);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                switch (event.getState()) {
                    case CLIENT_DISCONNECTED:
                        clientDisconnected.countDown();
                        break;
                    case CLIENT_CONNECTED:
                        clientConnected.countDown();
                        break;
                }
            }
        });
    }

    private void setupCluster() {
        clientConnected = new CountDownLatch(1);
        Config config = getConfig();
        member1 = hazelcastFactory.newHazelcastInstance(config);
        member2 = hazelcastFactory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void standaloneClientShouldGetNearcachedKeysOnClusterFailure() {
        //GIVEN
        fillCluster();
        String nearcachedKey = "key3";
        ensureKeyGetsNearcached(nearcachedKey);
        IMap<Object, Object> clientMap = getStandaloneNearcachedMap();
        shutdownCluster();
        String expectedValue = "value3";

        //WHEN
        Object nearcachedValue = clientMap.get(nearcachedKey);

        //THEN
        assertEquals(expectedValue, nearcachedValue);
    }

    @Test
    public void standaloneMapClientShouldGetNullOnNotNearcachedKeysOnClusterFailure() {
        //GIVEN
        fillCluster();
        String notNearcachedKey = "key5";
        IMap<Object, Object> clientMap = getStandaloneNearcachedMap();
        shutdownCluster();

        //WHEN
        Object cachedValue = clientMap.get(notNearcachedKey);

        //THEN
        assertEquals(null, cachedValue);
    }

    @Test
    public void standaloneMapClientShouldPutIntoNearcacheOnClusterFailure() {
        //GIVEN
        IMap<Object, Object> map = getStandaloneNearcachedMap();
        shutdownCluster();
        Object value = "value";
        Object key = "key";

        //WHEN
        map.put(key, value);

        //THEN
        assertEquals(value, map.get(key));
    }

    @Test
    public void clientShouldReturnStandaloneMapOnClusterFailure() {
        //GIVEN
        shutdownCluster();
        ensureClientIsDisconnected();

        //WHEN
        IMap<Object, Object> standaloneNearcachedMap = getStandaloneNearcachedMap();

        //THEN
        assertNotNull(standaloneNearcachedMap);

        standaloneNearcachedMap.put("key", "value");
        assertEquals("value", standaloneNearcachedMap.get("key"));
    }

    @Test
    public void standaloneMapClientShouldReconnectToCluster() {
        //GIVEN
        IMap<Object, Object> clientMap = getStandaloneNearcachedMap();
        clientMap.put("warmupKey", "warmupValue");
        shutdownCluster();
        clientMap.get("warmupKey");

        //WHEN
        setupCluster();
        ensureClientReconnects();
        clientMap.put("key0", "value0");

        //THEN
        Object valueFromCluster = member1.getMap(standaloneNearcacheMap).get("key0");
        assertEquals("value0", valueFromCluster);
    }

    private IMap<Object, Object> getStandaloneNearcachedMap() {
        return client.getMap(standaloneNearcacheMap);
    }

    private void ensureClientReconnects() {
        try {
            clientConnected.await();
        } catch (InterruptedException e) {
        }
    }

    private CountDownLatch clientConnected;
    private CountDownLatch clientDisconnected = new CountDownLatch(1);

    private void ensureClientIsDisconnected() {
        try {
            clientDisconnected.await();
        } catch (InterruptedException e) {
        }
    }


    private void shutdownCluster() {
        member1.shutdown();
        member2.shutdown();
    }

    private void ensureKeyGetsNearcached(String nearcachedKey) {
        client.getMap(standaloneNearcacheMap).get(nearcachedKey);
    }

    private void fillCluster() {
        for (int i = 0 ; i < 10; i++) {
            member1.getMap(standaloneNearcacheMap).put("key"+i, "value" + i);
        }
    }

    private static class EmptyEntryListener implements EntryListener<String, String> {

        public void entryAdded(EntryEvent event) {
        }

        public void entryRemoved(EntryEvent event) {
        }

        public void entryUpdated(EntryEvent event) {
        }

        public void entryEvicted(EntryEvent event) {
        }

        public void mapEvicted(MapEvent event) {
        }

        public void mapCleared(MapEvent event) {
        }
    }

    private static class FalsePredicate implements Predicate<String, String> {

        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }
}
