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
import org.apache.doris.common.Pair;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupCommitManagerTest {

    private GroupCommitManager groupCommitManager;

    @Mocked
    private Env env;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private InternalCatalog internalCatalog;

    @Injectable
    private OlapTable table;

    @Injectable
    private OlapTable table1;

    @Injectable
    private OlapTable table2;

    @Injectable
    private Backend backend;

    @Injectable
    private Backend backend1;

    @Injectable
    private Backend backend2;

    @Before
    public void setUp() {
        groupCommitManager = new GroupCommitManager();
    }

    // ==================== Reflection helpers ====================

    @SuppressWarnings("unchecked")
    private Map<Long, Pair<Long, SlidingWindowCounter>> getTableToBePairMap() throws Exception {
        Field field = GroupCommitManager.class.getDeclaredField("tableToBePairMap");
        field.setAccessible(true);
        return (Map<Long, Pair<Long, SlidingWindowCounter>>) field.get(groupCommitManager);
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

    // ==================== Unit tests for block/unblock/isBlock ====================

    @Test
    public void testBlockAndIsBlock() {
        long tableId = 100L;
        Assert.assertFalse(groupCommitManager.isBlock(tableId));

        groupCommitManager.blockTable(tableId);
        Assert.assertTrue(groupCommitManager.isBlock(tableId));
    }

    @Test
    public void testUnblockTable() {
        long tableId = 200L;
        groupCommitManager.blockTable(tableId);
        Assert.assertTrue(groupCommitManager.isBlock(tableId));

        groupCommitManager.unblockTable(tableId);
        Assert.assertFalse(groupCommitManager.isBlock(tableId));
    }

    @Test
    public void testBlockMultipleTables() {
        long tableId1 = 301L;
        long tableId2 = 302L;
        long tableId3 = 303L;

        groupCommitManager.blockTable(tableId1);
        groupCommitManager.blockTable(tableId2);

        Assert.assertTrue(groupCommitManager.isBlock(tableId1));
        Assert.assertTrue(groupCommitManager.isBlock(tableId2));
        Assert.assertFalse(groupCommitManager.isBlock(tableId3));
    }

    @Test
    public void testUnblockNonBlockedTable() {
        long tableId = 400L;
        // unblock a table that was never blocked should not throw
        groupCommitManager.unblockTable(tableId);
        Assert.assertFalse(groupCommitManager.isBlock(tableId));
    }

    @Test
    public void testBlockIdempotent() {
        long tableId = 500L;
        groupCommitManager.blockTable(tableId);
        groupCommitManager.blockTable(tableId);
        Assert.assertTrue(groupCommitManager.isBlock(tableId));

        groupCommitManager.unblockTable(tableId);
        Assert.assertFalse(groupCommitManager.isBlock(tableId));
    }

    // ==================== Unit tests for getCachedBackend ====================

    @Test
    public void testGetCachedBackendNoEntry() throws Exception {
        long tableId = 1001L;
        // tableToBePairMap is empty, should return null
        Long result = invokeGetCachedBackend(tableId);
        Assert.assertNull(result);
    }

    @Test
    public void testGetCachedBackendTableNotFound() throws Exception {
        long tableId = 1002L;
        long backendId = 1L;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = null;
            }};

        // When table is null, getCachedBackend will NPE on table.getGroupCommitDataBytes()
        // This is expected behavior - the table should always exist in production
        try {
            invokeGetCachedBackend(tableId);
            Assert.fail("Expected NullPointerException due to null table");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testGetCachedBackendNormal() throws Exception {
        long tableId = 1003L;
        long backendId = 2L;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
            }};

        Long result = invokeGetCachedBackend(tableId);
        Assert.assertEquals(Long.valueOf(backendId), result);
    }

    @Test
    public void testGetCachedBackendPressureExceededRemovesEntry() throws Exception {
        long tableId = 1004L;
        long backendId = 3L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        counter.add(Long.MAX_VALUE / 2);
        getTableToBePairMap().put(tableId, Pair.of(backendId, counter));

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
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeUnavailableRemovesEntry() throws Exception {
        long tableId = 1005L;
        long backendId = 4L;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeDecommissionedRemovesEntry() throws Exception {
        long tableId = 1006L;
        long backendId = 5L;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testGetCachedBackendBeNullRemovesEntry() throws Exception {
        long tableId = 1007L;
        long backendId = 6L;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    // ==================== Unit tests for getRandomBackend ====================

    @Test
    public void testGetRandomBackendNoAliveBackend() throws Exception {
        long tableId = 2001L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitIntervalMs();
                result = 1000;
                backend.isAlive();
                result = false;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testGetRandomBackendAllBackendsDecommissioned() throws Exception {
        long tableId = 2002L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitIntervalMs();
                result = 1000;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = true;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertNull(result);
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testGetRandomBackendPutsPairAtomically() throws Exception {
        long tableId = 2003L;
        long backendId = 20L;
        List<Backend> backends = Collections.singletonList(backend);

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                table.getGroupCommitIntervalMs();
                result = 1000;
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = false;
                backend.getId();
                result = backendId;
            }};

        Long result = invokeGetRandomBackend(tableId, backends);
        Assert.assertEquals(Long.valueOf(backendId), result);

        // Verify the Pair is stored atomically - both backendId and counter exist together
        Pair<Long, SlidingWindowCounter> pair = getTableToBePairMap().get(tableId);
        Assert.assertNotNull(pair);
        Assert.assertEquals(Long.valueOf(backendId), pair.first);
        Assert.assertNotNull(pair.second);
    }

    // ==================== Unit tests for updateLoadDataInternal ====================

    @Test
    public void testUpdateLoadDataInternalNoEntry() throws Exception {
        long tableId = 3001L;
        // No entry in tableToBePairMap, updateLoadDataInternal should just log and return
        invokeUpdateLoadDataInternal(tableId, 100L);
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));
    }

    @Test
    public void testUpdateLoadDataInternalNormal() throws Exception {
        long tableId = 3002L;
        long backendId = 30L;

        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        getTableToBePairMap().put(tableId, Pair.of(backendId, counter));

        invokeUpdateLoadDataInternal(tableId, 100L);

        Pair<Long, SlidingWindowCounter> pair = getTableToBePairMap().get(tableId);
        Assert.assertNotNull(pair);
        Assert.assertEquals(Long.valueOf(backendId), pair.first);
        // Counter should have been updated
        Assert.assertNotNull(pair.second);
    }

    @Test
    public void testUpdateLoadDataInternalInvalidTableId() throws Exception {
        // tableId == -1 should be handled gracefully (just logs a warning)
        invokeUpdateLoadDataInternal(-1, 100L);
        Assert.assertFalse(getTableToBePairMap().containsKey(-1L));
    }

    // ==================== Unit tests for selectBackendForLocalGroupCommitInternal ====================

    @Test
    public void testSelectBackendNoAliveBackends() throws Exception {
        long tableId = 4001L;

        new Expectations() {{
                env.getCurrentSystemInfo();
                result = systemInfoService;
                systemInfoService.getAllBackends();
                result = Collections.emptyList();
            }};

        try {
            invokeSelectBackendForLocalGroupCommitInternal(tableId);
            Assert.fail("Expected LoadException");
        } catch (InvocationTargetException e) {
            Assert.assertTrue(e.getCause() instanceof LoadException);
            Assert.assertTrue(e.getCause().getMessage().contains("No alive backend"));
        }
    }

    @Test
    public void testSelectBackendUsesCachedBackend() throws Exception {
        long tableId = 4002L;
        long backendId = 40L;

        // Pre-populate the cache with a valid entry
        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
            }};

        long result = invokeSelectBackendForLocalGroupCommitInternal(tableId);
        Assert.assertEquals(backendId, result);
    }

    @Test
    public void testSelectBackendSelectsRandomWhenNoCache() throws Exception {
        long tableId = 4003L;
        long backendId = 41L;

        new Expectations() {{
                env.getInternalCatalog();
                result = internalCatalog;
                minTimes = 0;
                internalCatalog.getTableByTableId(tableId);
                result = table;
                minTimes = 0;
                table.getGroupCommitIntervalMs();
                result = 1000;
                env.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;
                systemInfoService.getAllBackends();
                result = Collections.singletonList(backend);
                backend.isAlive();
                result = true;
                backend.isDecommissioned();
                result = false;
                backend.getId();
                result = backendId;
            }};

        long result = invokeSelectBackendForLocalGroupCommitInternal(tableId);
        Assert.assertEquals(backendId, result);
    }

    // ==================== Concurrency tests ====================

    @Test
    public void testConcurrentGetCachedBackendNoNpe() throws Exception {
        long tableId = 5001L;
        long backendId = 50L;
        int threadCount = 20;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
            }};

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger npeCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeGetCachedBackend(tableId);
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
        long tableId = 5002L;
        long backendId = 51L;
        int threadCount = 20;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger npeCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeUpdateLoadDataInternal(tableId, 100L);
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
    public void testConcurrentGetRandomThenGetCached() throws Exception {
        long tableId = 5003L;
        long backendId = 52L;
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
                result = 1000;
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
                        invokeGetCachedBackend(tableId);
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
        // After concurrent getRandom + getCached, the Pair entry should be consistent
        Pair<Long, SlidingWindowCounter> pair = getTableToBePairMap().get(tableId);
        Assert.assertNotNull(pair);
        Assert.assertEquals(Long.valueOf(backendId), pair.first);
        Assert.assertNotNull(pair.second);
    }

    @Test
    public void testSelectBackendEndToEndUnderConcurrency() throws Exception {
        long tableId = 5004L;
        long backendId = 53L;
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
                result = 1000;
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
    public void testConcurrentBlockUnblockNoException() {
        long tableId = 5005L;
        int threadCount = 20;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            new Thread(() -> {
                try {
                    barrier.await();
                    if (idx % 2 == 0) {
                        groupCommitManager.blockTable(tableId);
                    } else {
                        groupCommitManager.unblockTable(tableId);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }).start();
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignore
        }
        Assert.assertEquals(0, errorCount.get());
    }

    // ==================== Reaccess after entry removed ====================

    @Test
    public void testReaccessAfterPairRemoved() throws Exception {
        long tableId = 6001L;
        long backendId = 60L;

        // Pre-populate with a pressure-exceeded entry
        SlidingWindowCounter counter = new SlidingWindowCounter(10);
        counter.add(Long.MAX_VALUE / 2);
        getTableToBePairMap().put(tableId, Pair.of(backendId, counter));

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
                result = 1000;
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

        // First call: pressure exceeded, entry removed
        Long result1 = invokeGetCachedBackend(tableId);
        Assert.assertNull(result1);
        Assert.assertFalse(getTableToBePairMap().containsKey(tableId));

        // Second call: getRandomBackend creates a new entry
        List<Backend> backends = Collections.singletonList(backend);
        Long result2 = invokeGetRandomBackend(tableId, backends);
        Assert.assertEquals(Long.valueOf(backendId), result2);
        Assert.assertTrue(getTableToBePairMap().containsKey(tableId));

        // Third call: getCachedBackend finds the new entry
        Long result3 = invokeGetCachedBackend(tableId);
        Assert.assertEquals(Long.valueOf(backendId), result3);
    }

    // ==================== Different tableIds no contention ====================

    @Test
    public void testDifferentTableIdsNoContention() throws Exception {
        long tableId1 = 7001L;
        long tableId2 = 7002L;
        long backendId1 = 70L;
        long backendId2 = 71L;
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
                result = 1000;
                minTimes = 0;
                table2.getGroupCommitIntervalMs();
                result = 1000;
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

        Pair<Long, SlidingWindowCounter> pair1 = getTableToBePairMap().get(tableId1);
        Pair<Long, SlidingWindowCounter> pair2 = getTableToBePairMap().get(tableId2);
        Assert.assertNotNull(pair1);
        Assert.assertNotNull(pair2);
        Assert.assertEquals(Long.valueOf(backendId1), pair1.first);
        Assert.assertEquals(Long.valueOf(backendId2), pair2.first);
    }

    // ==================== Pair consistency under concurrent read/write ====================

    @Test
    public void testPairConsistencyUnderConcurrentReadAndRemove() throws Exception {
        long tableId = 8001L;
        long backendId = 80L;
        int readerCount = 20;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

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
            }};

        CyclicBarrier barrier = new CyclicBarrier(readerCount + 1);
        AtomicInteger npeCount = new AtomicInteger(0);

        Thread remover = new Thread(() -> {
            try {
                barrier.await();
                getTableToBePairMap().remove(tableId);
            } catch (Exception e) {
                // ignore
            }
        });

        for (int i = 0; i < readerCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    // getCachedBackend reads the Pair atomically from ConcurrentHashMap
                    invokeGetCachedBackend(tableId);
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
    public void testConcurrentRemoveFromPairMapWhileUpdateLoad() throws Exception {
        long tableId = 8002L;
        long backendId = 81L;
        int updaterCount = 20;

        getTableToBePairMap().put(tableId, Pair.of(backendId, new SlidingWindowCounter(10)));

        CyclicBarrier barrier = new CyclicBarrier(updaterCount + 1);
        AtomicInteger npeCount = new AtomicInteger(0);

        Thread remover = new Thread(() -> {
            try {
                barrier.await();
                getTableToBePairMap().remove(tableId);
            } catch (Exception e) {
                // ignore
            }
        });

        for (int i = 0; i < updaterCount; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeUpdateLoadDataInternal(tableId, 100L);
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
}
