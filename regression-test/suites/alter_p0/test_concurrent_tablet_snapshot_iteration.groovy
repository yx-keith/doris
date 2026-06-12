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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

suite("test_concurrent_tablet_snapshot_iteration", "nonConcurrent") {
    def table = "test_concurrent_tablet_snapshot_iteration"
    def observerError = new AtomicReference<Throwable>()
    def stopObserver = new AtomicBoolean(false)
    def observerIterations = new AtomicLong(0)
    def observerCount = 3

    sql """DROP TABLE IF EXISTS ${table} FORCE"""
    sql """
        CREATE TABLE ${table} (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v1 BIGINT
        )
        DUPLICATE KEY(k1, k2)
        PARTITION BY RANGE(k1) (
            PARTITION p1 VALUES LESS THAN ("64"),
            PARTITION p2 VALUES LESS THAN ("128"),
            PARTITION p3 VALUES LESS THAN ("192")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 16
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "false"
        )
    """

    def rows = (1..120).collect { "(${it}, ${it % 17}, ${it * 10})" }.join(",")
    sql """INSERT INTO ${table} VALUES ${rows}"""
    sql """SYNC"""

    def observers = (1..observerCount).collect {
        Thread.start {
            try {
                while (!stopObserver.get()) {
                    sql """SHOW DATA FROM ${table}"""
                    sql """SHOW TABLETS FROM ${table}"""
                    sql """SELECT COUNT(*) FROM ${table}"""
                    observerIterations.incrementAndGet()
                }
            } catch (Throwable t) {
                observerError.compareAndSet(null, t)
                stopObserver.set(true)
            }
        }
    }

    def mainError = null
    try {
        createMV """CREATE MATERIALIZED VIEW mv_${table}_sum AS
                    SELECT k1, SUM(v1) FROM ${table} GROUP BY k1"""

        sql """ALTER TABLE ${table} ADD COLUMN v2 BIGINT DEFAULT '0'"""
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE TableName='${table}' ORDER BY createtime DESC LIMIT 1"""
            time 600
        }

        createMV """CREATE MATERIALIZED VIEW mv_${table}_k2 AS
                    SELECT k2, SUM(v1) FROM ${table} GROUP BY k2"""

        sql """INSERT INTO ${table}(k1, k2, v1, v2) VALUES (121, 2, 1210, 12100), (122, 3, 1220, 12200)"""
        sql """SYNC"""
        def result = sql """SELECT COUNT(*), SUM(v1), SUM(v2) FROM ${table}"""
        assertEquals("122", result[0][0].toString())
        assertEquals("75030", result[0][1].toString())
        assertEquals("24300", result[0][2].toString())
    } catch (Throwable t) {
        mainError = t
    } finally {
        stopObserver.set(true)
        observers.each { it.join(30000) }
    }

    def failure = observerError.get()
    def stillAlive = observers.findAll { it.isAlive() }.size()
    def iterations = observerIterations.get()
    sql """DROP TABLE IF EXISTS ${table} FORCE"""
    if (mainError != null) {
        throw mainError
    }
    if (stillAlive > 0) {
        throw new RuntimeException("${stillAlive} observer thread(s) did not stop after concurrent tablet iteration test")
    }
    if (failure != null) {
        throw failure
    }
    assertTrue(iterations > 10,
            "observer threads completed too few iterations (${iterations}) to exercise the race window")
}
