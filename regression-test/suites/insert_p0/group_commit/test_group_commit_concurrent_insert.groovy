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

import java.util.UUID
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

suite("test_group_commit_concurrent_insert", "p0") {

    def dbName = "regression_test_insert_p0"
    def baseTableName = "test_group_commit_concurrent_insert"

    sql """ CREATE DATABASE IF NOT EXISTS ${dbName}; """

    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, dbName)
    url += "&rewriteBatchedStatements=true&cachePrepStmts=true"
    url += "&sessionVariables=group_commit=async_mode&sessionVariables=enable_nereids_planner=false"
    logger.info("connect url: " + url)

    def getRowCount = { fullTable, expectedRowCount ->
        def retry = 0
        while (retry < 60) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${fullTable}"
            logger.info("table=${fullTable}, rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                return true
            }
            retry++
        }
        return false
    }

    sql """ SET PROPERTY 'max_user_connections' = '10000'; """

    List<String> allTableNames = new ArrayList<>()

    try {
        // =====================================================================
        // Test 1: Multi-round cold start — each round uses fresh tables
        // Exercises: concurrent getRandomBackend (write-write race on
        //            tableToBeMap + tableToPressureMap)
        // =====================================================================
        def tableCount = 5
        def clientPerTable = 100
        def insertsPerClient = 10
        def expectedRowsPerTable = clientPerTable * insertsPerClient
        def totalClients = tableCount * clientPerTable
        def roundCount = 5

        for (int round = 0; round < roundCount; round++) {
            logger.info("===== Test1 Round ${round + 1}/${roundCount} =====")

            List<String> roundTableNames = new ArrayList<>()
            for (int t = 0; t < tableCount; t++) {
                def tn = baseTableName + "_cold_r${round}_t${t}"
                def fullTable = dbName + "." + tn
                roundTableNames.add(fullTable)
                allTableNames.add(fullTable)
                sql """ drop table if exists ${fullTable}; """
                sql """
                CREATE TABLE ${fullTable} (
                    `id` varchar(50) NOT NULL,
                    `name` varchar(50) NULL,
                    `score` int(11) NULL default "-1"
                ) ENGINE=OLAP
                DUPLICATE KEY(`id`, `name`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 4
                PROPERTIES (
                    "replication_num" = "1"
                );
                """
            }

            CyclicBarrier barrier = new CyclicBarrier(totalClients)
            AtomicInteger errorCount = new AtomicInteger(0)
            AtomicInteger successCount = new AtomicInteger(0)
            List<Thread> threads = new ArrayList<>()

            for (int t = 0; t < tableCount; t++) {
                def fullTable = roundTableNames.get(t)
                for (int c = 0; c < clientPerTable; c++) {
                    int tableIdx = t
                    int clientId = c
                    Thread th = new Thread({
                        try {
                            connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
                                def ps = prepareStatement """ INSERT INTO ${fullTable}(id, name, score) VALUES(?, ?, ?) """
                                barrier.await(60, TimeUnit.SECONDS)

                                for (int j = 0; j < insertsPerClient; j++) {
                                    def id = UUID.randomUUID().toString()
                                    def score = clientId * insertsPerClient + j
                                    ps.setString(1, id)
                                    ps.setString(2, "name_${id}")
                                    ps.setInt(3, score)
                                    ps.addBatch()
                                }
                                ps.executeBatch()
                                successCount.incrementAndGet()
                            }
                        } catch (Exception e) {
                            logger.warn("Test1 round=${round} table=${tableIdx} client=${clientId} got exception: " + e.getMessage())
                            errorCount.incrementAndGet()
                        }
                    })
                    threads.add(th)
                    th.start()
                }
            }

            for (Thread th : threads) {
                th.join(180000)
            }

            logger.info("Test1 Round ${round}: successCount=" + successCount.get() + ", errorCount=" + errorCount.get())
            assertEquals(0, errorCount.get())
            assertEquals(totalClients, successCount.get())

            for (int t = 0; t < tableCount; t++) {
                def fullTable = roundTableNames.get(t)
                assertTrue(getRowCount(fullTable, expectedRowsPerTable) as boolean,
                           "table ${fullTable} should have ${expectedRowsPerTable} rows")
                def rc = sql "select count(*) from ${fullTable}"
                assertEquals(expectedRowsPerTable, rc[0][0])
            }
        }

        // =====================================================================
        // Test 2: Sustained insert on same tables — exercises getCachedBackend
        // (read race) and getRandomBackend (write race) interleaving, plus
        // updateLoadData + getCachedBackend interleaving.
        //
        // With group_commit_data_bytes set small, pressure will exceed
        // threshold quickly, causing getCachedBackend to return null and
        // re-enter getRandomBackend. This creates a mix of:
        //   - getCachedBackend reads (cached hit path)
        //   - getRandomBackend writes (cache miss / pressure exceeded path)
        //   - updateLoadData writes (concurrent with above)
        // =====================================================================
        def sustainedTableCount = 5
        def sustainedClientPerTable = 100
        def sustainedInsertsPerClient = 20
        def sustainedExpectedRows = sustainedClientPerTable * sustainedInsertsPerClient
        def sustainedTotalClients = sustainedTableCount * sustainedClientPerTable

        List<String> sustainedTableNames = new ArrayList<>()
        for (int t = 0; t < sustainedTableCount; t++) {
            def tn = baseTableName + "_sustained_t${t}"
            def fullTable = dbName + "." + tn
            sustainedTableNames.add(fullTable)
            allTableNames.add(fullTable)
            sql """ drop table if exists ${fullTable}; """
            sql """
            CREATE TABLE ${fullTable} (
                `id` varchar(50) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_data_bytes" = "100"
            );
            """
        }

        CyclicBarrier barrier2 = new CyclicBarrier(sustainedTotalClients)
        AtomicInteger errorCount2 = new AtomicInteger(0)
        AtomicInteger successCount2 = new AtomicInteger(0)
        List<Thread> threads2 = new ArrayList<>()

        for (int t = 0; t < sustainedTableCount; t++) {
            def fullTable = sustainedTableNames.get(t)
            for (int c = 0; c < sustainedClientPerTable; c++) {
                int tableIdx = t
                int clientId = c
                Thread th = new Thread({
                    try {
                        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
                            def ps = prepareStatement """ INSERT INTO ${fullTable}(id, name, score) VALUES(?, ?, ?) """
                            barrier2.await(60, TimeUnit.SECONDS)

                            for (int j = 0; j < sustainedInsertsPerClient; j++) {
                                def id = UUID.randomUUID().toString()
                                def score = clientId * sustainedInsertsPerClient + j
                                ps.setString(1, id)
                                ps.setString(2, "name_${id}")
                                ps.setInt(3, score)
                                ps.addBatch()
                            }
                            ps.executeBatch()
                            successCount2.incrementAndGet()
                        }
                    } catch (Exception e) {
                        logger.warn("Test2 table=${tableIdx} client=${clientId} got exception: " + e.getMessage())
                        errorCount2.incrementAndGet()
                    }
                })
                threads2.add(th)
                th.start()
            }
        }

        for (Thread th : threads2) {
            th.join(180000)
        }

        logger.info("Test2: successCount=" + successCount2.get() + ", errorCount=" + errorCount2.get())
        assertEquals(0, errorCount2.get())
        assertEquals(sustainedTotalClients, successCount2.get())

        for (int t = 0; t < sustainedTableCount; t++) {
            def fullTable = sustainedTableNames.get(t)
            assertTrue(getRowCount(fullTable, sustainedExpectedRows) as boolean,
                       "table ${fullTable} should have ${sustainedExpectedRows} rows")
            def rc = sql "select count(*) from ${fullTable}"
            assertEquals(sustainedExpectedRows, rc[0][0])
        }

    } finally {
        for (String fullTable : allTableNames) {
            try {
                sql """ drop table if exists ${fullTable}; """
            } catch (Exception e) {
                logger.warn("Cleanup failed for ${fullTable}: " + e.getMessage())
            }
        }
    }
}
