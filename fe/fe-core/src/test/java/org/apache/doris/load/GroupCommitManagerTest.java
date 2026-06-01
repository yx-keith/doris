// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.util.SlidingWindowCounter;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

public class GroupCommitManagerTest {

    private GroupCommitManager groupCommitManager;

    @Mocked
    private Env env;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private InternalCatalog internalCatalog;

    @Before
    public void setUp() {
        groupCommitManager = new GroupCommitManager();
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Long> getTableToBeMap() throws Exception {
        Field field = GroupCommitManager.class.getDeclaredField("tableToBeMap");
        field.setAccessible(true);
        return (Map<Long, Long>) field.get(groupCommitManager);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, SlidingWindowCounter> getTableToPressureMap() throws Exception {
        Field field = GroupCommitManager.class.getDeclaredField("tableToPressureMap");
        field.setAccessible(true);
        return (Map<Long, SlidingWindowCounter>) field.get(groupCommitManager);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, StampedLock> getTableLocks() throws Exception {
        Field field = GroupCommitManager.class.getDeclaredField("tableLocks");
        field.setAccessible(true);
        return (Map<Long, StampedLock>) field.get(groupCommitManager);
    }

    private Long invokeGetCachedBackend(long tableId) throws Exception {
        Method method = GroupCommitManager.class.getDeclaredMethod("getCachedBackend", long.class);
        method.setAccessible(true);
        return (Long) method.invoke(groupCommitManager, tableId);
    }

    private Long invokeGetRandomBackend(long tableId, List<Backend> backends) throws Exception {
        Method method = GroupCommitManager.class.getDeclaredMethod("getRandomBackend", long.class, List.class);
        method.setAccessible(true);
        return (Long) method.invoke(groupCommitManager, tableId, backends);
    }

    private void invokeUpdateLoadDataInternal(long tableId, long receiveData) throws Exception {
        Method method = GroupCommitManager.class.getDeclaredMethod("updateLoadDataInternal", long.class, long.class);
        method.setAccessible(true);
        method.invoke(groupCommitManager, tableId, receiveData);
    }

    private long invokeSelectBackendForLocalGroupCommitInternal(long tableId) throws Exception {
        Method method = GroupCommitManager.class.getDeclaredMethod("selectBackendForLocalGroupCommitInternal", long.class);
        method.setAccessible(true);
        return (long) method.invoke(groupCommitManager, tableId);
    }

    // ==================== Unit tests for getCachedBackend ====================

    @Test
    public void testGetCachedBackendTableNotFound() throws Exception {
        long tableId = 99999L;
        getTableToBeMap().put(tableId, 1L);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = null;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
    }

    @Test
    public void testGetCachedBackendPressureMapInconsistent(@Injectable OlapTable table) throws Exception {
        long tableId = 100L;
        long backendId = 1L;

        getTableToBeMap().put(tableId, backendId);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
    }

    @Test
    public void testGetCachedBackendNormal(@Injectable OlapTable table, @Injectable Backend backend) throws Exception {
        long tableId = 100L;
        long backendId = 1L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = backend;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = false;
                backend.getId();
                result = backendId;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertEquals(Long.valueOf(backendId), result);
    }

    // ==================== Unit tests for getRandomBackend ====================

    @Test
    public void testGetRandomBackendTableNotFound() throws Exception {
        long tableId = 99999L;
        List<Backend> backends = new ArrayList<>();

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = null;
            }};

        try {
            invokeGetRandomBackend(tableId, backends);
            Assert.fail("Expected LoadException");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof LoadException);
            Assert.assertTrue(e.getCause().getMessage().contains("Table not found"));
        }
    }

    @Test
    public void testGetRandomBackendPutsBothMapsAtomically(@Injectable OlapTable table,
                                                            @Injectable Backend backend) throws Exception {
        long tableId = 200L;
        long backendId = 2L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitIntervalMs();
                result = 1000L;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = false;
                backend.getId();
                result = backendId;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertEquals(Long.valueOf(backendId), result);
        Assert.assertEquals(backendId, (long) getTableToBeMap().get(tableId));
        Assert.assertNotNull(getTableToPressureMap().get(tableId));
    }

    // ==================== Unit tests for updateLoadDataInternal ====================

    @Test
    public void testUpdateLoadDataInternalPressureMapInconsistent() throws Exception {
        long tableId = 300L;

        getTableToBeMap().put(tableId, 1L);

        invokeUpdateLoadDataInternal(tableId, 100L);

        Assert.assertNull(getTableToPressureMap().get(tableId));
    }

    @Test
    public void testUpdateLoadDataInternalNormal() throws Exception {
        long tableId = 400L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        getTableToPressureMap().put(tableId, counter);

        invokeUpdateLoadDataInternal(tableId, 100L);

        Assert.assertNotNull(getTableToPressureMap().get(tableId));
    }

    // ==================== Concurrency tests ====================

    @Test
    public void testConcurrentGetCachedBackendNoNpe(@Injectable OlapTable table) throws Exception {
        long tableId = 500L;
        long backendId = 5L;
        int threadCount = 20;

        getTableToBeMap().put(tableId, backendId);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
            }};

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger npeCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeGetCachedBackend(tableId);
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                }
            }).start();
        }

        Thread.sleep(2000);
        Assert.assertEquals(0, npeCount.get());
    }

    @Test
    public void testConcurrentUpdateLoadDataNoNpe() throws Exception {
        long tableId = 600L;
        long backendId = 6L;
        int threadCount = 20;

        getTableToBeMap().put(tableId, backendId);

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger npeCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeUpdateLoadDataInternal(tableId, 100L);
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                }
            }).start();
        }

        Thread.sleep(2000);
        Assert.assertEquals(0, npeCount.get());
    }

    @Test
    public void testStripedLockAtomicityGetRandomThenGetCached(@Injectable OlapTable table,
                                                                @Injectable Backend backend) throws Exception {
        long tableId = 700L;
        long backendId = 7L;
        int threadCount = 20;

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                minTimes = 0;
                table.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackend(backendId);
                result = backend;
                minTimes = 0;
                backend.isAlive();
                result = true;
                minTimes = 0;
                backend.isDecommissioned();
                result = false;
                minTimes = 0;
                backend.getId();
                result = backendId;
                minTimes = 0;
            }};

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger inconsistentCount = new AtomicInteger(0);
        AtomicInteger npeCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    barrier.await();
                    if (idx % 2 == 0) {
                        List<Backend> backends = Collections.singletonList(backend);
                        invokeGetRandomBackend(tableId, backends);
                    } else {
                        Long cached = invokeGetCachedBackend(tableId);
                        if (cached != null) {
                            boolean hasBe = getTableToBeMap().containsKey(tableId);
                            boolean hasPressure = getTableToPressureMap().containsKey(tableId);
                            if (hasBe != hasPressure) {
                                inconsistentCount.incrementAndGet();
                            }
                        }
                    }
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                }
            }).start();
        }

        Thread.sleep(3000);
        Assert.assertEquals(0, npeCount.get());
        Assert.assertEquals(0, inconsistentCount.get());
    }

    @Test
    public void testConcurrentPutBeforePressureMapNoNpe(@Injectable OlapTable table,
                                                         @Injectable Backend backend) throws Exception {
        long tableId = 800L;
        long backendId = 8L;
        int readerCount = 20;

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                minTimes = 0;
                table.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackend(backendId);
                result = backend;
                minTimes = 0;
                backend.isAlive();
                result = true;
                minTimes = 0;
                backend.isDecommissioned();
                result = false;
                minTimes = 0;
                backend.getId();
                result = backendId;
                minTimes = 0;
            }};

        CountDownLatch writerReady = new CountDownLatch(1);
        CountDownLatch readersReady = new CountDownLatch(readerCount);
        CountDownLatch readersDone = new CountDownLatch(readerCount);
        AtomicInteger npeCount = new AtomicInteger(0);

        Thread writer = new Thread(() -> {
            try {
                writerReady.await();
                List<Backend> backends = Collections.singletonList(backend);
                invokeGetRandomBackend(tableId, backends);
            } catch (Exception e) {
                // ignore
            }
        });

        for (int i = 0; i < readerCount; i++) {
            new Thread(() -> {
                try {
                    readersReady.countDown();
                    writerReady.await();
                    Thread.sleep(1);
                    invokeGetCachedBackend(tableId);
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } finally {
                    readersDone.countDown();
                }
            }).start();
        }

        readersReady.await();
        writerReady.countDown();
        writer.start();

        readersDone.await(5, java.util.concurrent.TimeUnit.SECONDS);
        Assert.assertEquals(0, npeCount.get());
    }

    @Test
    public void testConcurrentRemoveFromPressureMapWhileRead(@Injectable OlapTable table,
                                                              @Injectable Backend backend) throws Exception {
        long tableId = 900L;
        long backendId = 9L;
        int readerCount = 20;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                minTimes = 0;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackend(backendId);
                result = backend;
                minTimes = 0;
                backend.isAlive();
                result = true;
                minTimes = 0;
                backend.isDecommissioned();
                result = false;
                minTimes = 0;
                backend.getId();
                result = backendId;
                minTimes = 0;
            }};

        CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
        AtomicInteger npeCount = new AtomicInteger(0);

        Thread remover = new Thread(() -> {
            try {
                barrier.await();
                getTableToPressureMap().remove(tableId);
            } catch (Exception e) {
                // ignore
            }
        });

        for (int i = 0; i < readerCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeGetCachedBackend(tableId);
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                }
            }).start();
        }

        remover.start();
        Thread.sleep(3000);
        Assert.assertEquals(0, npeCount.get());
    }

    @Test
    public void testConcurrentRemoveFromPressureMapWhileUpdateLoad() throws Exception {
        long tableId = 1000L;
        long backendId = 10L;
        int updaterCount = 20;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        CyclicBarrier barrier = new CyclicBarrier(updaterCount + 1);
        AtomicInteger npeCount = new AtomicInteger(0);

        Thread remover = new Thread(() -> {
            try {
                barrier.await();
                getTableToPressureMap().remove(tableId);
            } catch (Exception e) {
                // ignore
            }
        });

        for (int i = 0; i < updaterCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeUpdateLoadDataInternal(tableId, 100L);
                } catch (AssertionError e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        npeCount.incrementAndGet();
                    }
                }
            }).start();
        }

        remover.start();
        Thread.sleep(3000);
        Assert.assertEquals(0, npeCount.get());
    }

    @Test
    public void testSelectBackendEndToEndUnderConcurrency(@Injectable OlapTable table,
                                                           @Injectable Backend backend) throws Exception {
        long tableId = 1100L;
        long backendId = 11L;
        int threadCount = 20;

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                minTimes = 0;
                table.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getAllBackends();
                result = Collections.singletonList(backend);
                minTimes = 0;
                systemInfoService.getBackend(backendId);
                result = backend;
                minTimes = 0;
                backend.isAlive();
                result = true;
                minTimes = 0;
                backend.isDecommissioned();
                result = false;
                minTimes = 0;
                backend.getId();
                result = backendId;
                minTimes = 0;
            }};

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    long beId = invokeSelectBackendForLocalGroupCommitInternal(tableId);
                    if (beId == backendId) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                }
            }).start();
        }

        Thread.sleep(3000);
        Assert.assertEquals(threadCount, successCount.get());
        Assert.assertEquals(0, failCount.get());
    }

    @Test
    public void testGetCachedBackendPressureExceededCleansBothMaps(@Injectable OlapTable table,
                                                                    @Injectable Backend backend) throws Exception {
        long tableId = 1300L;
        long backendId = 13L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        counter.add(Long.MAX_VALUE / 2);
        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, counter);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = 1;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeUnavailableCleansBothMaps(@Injectable OlapTable table,
                                                                 @Injectable Backend backend) throws Exception {
        long tableId = 1400L;
        long backendId = 14L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = backend;
                backend.isAlive();
                result = false;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeDecommissionedCleansBothMaps(@Injectable OlapTable table,
                                                                    @Injectable Backend backend) throws Exception {
        long tableId = 1500L;
        long backendId = 15L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = backend;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = true;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeNullCleansBothMaps(@Injectable OlapTable table) throws Exception {
        long tableId = 1600L;
        long backendId = 16L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = null;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendReadLockToWriteLockUpgradeBeRecovered(
            @Injectable OlapTable table, @Injectable Backend backend) throws Exception {
        long tableId = 1700L;
        long backendId = 17L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = backend;
                // First call in readLock: BE is not alive, triggers writeLock upgrade
                // Second call in writeLock: BE is alive again (recovered), should return backendId
                backend.isAlive();
                result = false;
                result = true;
                backend.isDecommissioned();
                result = false;
                backend.getId();
                result = backendId;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertEquals(Long.valueOf(backendId), result);
        Assert.assertTrue(getTableToBeMap().containsKey(tableId));
        Assert.assertTrue(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetRandomBackendNoAliveBackend(@Injectable OlapTable table,
                                                    @Injectable Backend backend) throws Exception {
        long tableId = 1800L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                backend.isAlive();
                result = false;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testGetRandomBackendAllBackendsDecommissioned(@Injectable OlapTable table,
                                                               @Injectable Backend backend) throws Exception {
        long tableId = 1900L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = true;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
    }

    @Test
    public void testTableLocksNotRemovedAfterPressureExceeded(@Injectable OlapTable table) throws Exception {
        long tableId = 2000L;
        long backendId = 20L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        counter.add(Long.MAX_VALUE / 2);
        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, counter);

        Method getTableLockMethod = GroupCommitManager.class.getDeclaredMethod("getTableLock", long.class);
        getTableLockMethod.setAccessible(true);
        getTableLockMethod.invoke(groupCommitManager, tableId);
        Assert.assertTrue(getTableLocks().containsKey(tableId));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = 1;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
        // StampedLock objects are never removed — see comment on tableLocks field
        Assert.assertTrue(getTableLocks().containsKey(tableId));
    }

    @Test
    public void testTableLocksNotRemovedAfterBeUnavailable(@Injectable OlapTable table,
                                                           @Injectable Backend backend) throws Exception {
        long tableId = 2100L;
        long backendId = 21L;

        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, new SlidingWindowCounter(10));

        Method getTableLockMethod = GroupCommitManager.class.getDeclaredMethod("getTableLock", long.class);
        getTableLockMethod.setAccessible(true);
        getTableLockMethod.invoke(groupCommitManager, tableId);
        Assert.assertTrue(getTableLocks().containsKey(tableId));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitDataBytes();
                result = Integer.MAX_VALUE;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getBackend(backendId);
                result = backend;
                backend.isAlive();
                result = false;
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBeMap().containsKey(tableId));
        Assert.assertFalse(getTableToPressureMap().containsKey(tableId));
        // StampedLock objects are never removed — see comment on tableLocks field
        Assert.assertTrue(getTableLocks().containsKey(tableId));
    }

    @Test
    public void testReaccessAfterMapsCleaned(@Injectable OlapTable table,
                                             @Injectable Backend backend) throws Exception {
        long tableId = 2200L;
        long backendId = 22L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        counter.add(Long.MAX_VALUE / 2);
        getTableToBeMap().put(tableId, backendId);
        getTableToPressureMap().put(tableId, counter);

        Method getTableLockMethod = GroupCommitManager.class.getDeclaredMethod("getTableLock", long.class);
        getTableLockMethod.setAccessible(true);
        getTableLockMethod.invoke(groupCommitManager, tableId);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitDataBytes();
                result = 1;
                minTimes = 0;
                table.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getBackend(backendId);
                result = backend;
                minTimes = 0;
                backend.isAlive();
                result = true;
                minTimes = 0;
                backend.isDecommissioned();
                result = false;
                minTimes = 0;
                backend.getId();
                result = backendId;
                minTimes = 0;
            }};

        Long result1 = invokeGetCachedBackend(tableId);
        Assert.assertNull(result1);
        // StampedLock is never removed, so it remains in tableLocks
        Assert.assertTrue(getTableLocks().containsKey(tableId));

        List<Backend> backends = Collections.singletonList(backend);
        Long result2 = invokeGetRandomBackend(tableId, backends);
        Assert.assertEquals(Long.valueOf(backendId), result2);
        Assert.assertTrue(getTableToBeMap().containsKey(tableId));
        Assert.assertTrue(getTableToPressureMap().containsKey(tableId));

        Long result3 = invokeGetCachedBackend(tableId);
        Assert.assertEquals(Long.valueOf(backendId), result3);
    }

    @Test
    public void testDifferentTableIdsNoLockContention(@Injectable OlapTable table1,
                                                       @Injectable OlapTable table2,
                                                       @Injectable Backend backend1,
                                                       @Injectable Backend backend2) throws Exception {
        long tableId1 = 1200L;
        long tableId2 = 1201L;
        long backendId1 = 12L;
        long backendId2 = 13L;
        int perTableThreads = 10;

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId1);
                result = table1;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId2);
                result = table2;
                minTimes = 0;
                table1.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                table2.getGroupCommitIntervalMs();
                result = 1000L;
                minTimes = 0;
                backend1.isAlive();
                result = true;
                minTimes = 0;
                backend1.isDecommissioned();
                result = false;
                minTimes = 0;
                backend1.getId();
                result = backendId1;
                minTimes = 0;
                backend2.isAlive();
                result = true;
                minTimes = 0;
                backend2.isDecommissioned();
                result = false;
                minTimes = 0;
                backend2.getId();
                result = backendId2;
                minTimes = 0;
            }};

        CyclicBarrier barrier = new CyclicBarrier(perTableThreads * 2);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < perTableThreads; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    List<Backend> backends = Collections.singletonList(backend1);
                    Long beId = invokeGetRandomBackend(tableId1, backends);
                    if (beId != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // ignore
                }
            }).start();
            new Thread(() -> {
                try {
                    barrier.await();
                    List<Backend> backends = Collections.singletonList(backend2);
                    Long beId = invokeGetRandomBackend(tableId2, backends);
                    if (beId != null) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // ignore
                }
            }).start();
        }

        Thread.sleep(3000);
        Assert.assertEquals(perTableThreads * 2, successCount.get());
        Assert.assertEquals(backendId1, (long) getTableToBeMap().get(tableId1));
        Assert.assertEquals(backendId2, (long) getTableToBeMap().get(tableId2));
    }
}
