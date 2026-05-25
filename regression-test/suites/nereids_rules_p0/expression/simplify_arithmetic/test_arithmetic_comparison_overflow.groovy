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

// Regression test for https://github.com/apache/doris/issues/61761
// SimplifyArithmeticComparisonRule used to rearrange
//   date_sub(i, K) <= MAX_DATE  ==>  i <= date_add(MAX_DATE, K)
// without checking that the new constant overflows the date domain,
// which caused a runtime "out of range" error.
//
// Key insight: we insert boundary values (9999-12-31, 0000-01-01) so that
// the result DIFFERS depending on whether the rewrite happened or not.
// If the rule incorrectly rewrites and produces `col <= NULL`, the result
// becomes NULL instead of the expected true/false.
suite("test_arithmetic_comparison_overflow") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists test_rewrite_arithmetic_61761"

    sql """
        create table test_rewrite_arithmetic_61761 (
            i datetime NOT NULL,
            d date NOT NULL
        )
        DISTRIBUTED BY HASH(i) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Insert boundary values so that overflow vs correct result is distinguishable.
    // Row 1: d='9999-12-31', i='9999-12-31 23:59:59' (upper bound)
    // Row 2: d='0000-01-01', i='0000-01-01 00:00:00' (lower bound)
    // Row 3: d='2026-01-20', i='2026-01-20 10:00:00' (normal value for sanity)
    sql """
        insert into test_rewrite_arithmetic_61761 values
            ('9999-12-31 23:59:59', '9999-12-31'),
            ('0000-01-01 00:00:00', '0000-01-01'),
            ('2026-01-20 10:00:00', '2026-01-20');
    """

    // ==================== Upper-bound overflow (<=) ====================
    // For d='9999-12-31': date_sub('9999-12-31', 1 day) = '9999-12-30' <= '9999-12-31' => true
    // If buggy rewrite: d <= days_add('9999-12-31', 1) = NULL => NULL (wrong!)

    // days upper overflow
    assertTrue(sql("select date_sub(d, interval 1 day) <= '9999-12-31' from test_rewrite_arithmetic_61761 where d = '9999-12-31'")[0][0])

    // seconds upper overflow
    assertTrue(sql("select date_sub(i, interval 1 second) <= '9999-12-31 23:59:59' from test_rewrite_arithmetic_61761 where i = '9999-12-31 23:59:59'")[0][0])

    // weeks upper overflow: date_sub('9999-12-31', 1 week) = '9999-12-24' <= '9999-12-25' => true
    assertTrue(sql("select date_sub(d, interval 1 week) <= '9999-12-25' from test_rewrite_arithmetic_61761 where d = '9999-12-31'")[0][0])

    // hours upper overflow
    assertTrue(sql("select date_sub(i, interval 1 hour) <= '9999-12-31 23:00:00' from test_rewrite_arithmetic_61761 where i = '9999-12-31 23:59:59'")[0][0])

    // minutes upper overflow
    assertTrue(sql("select date_sub(i, interval 1 minute) <= '9999-12-31 23:59:00' from test_rewrite_arithmetic_61761 where i = '9999-12-31 23:59:59'")[0][0])

    // ==================== Lower-bound underflow (>=) ====================
    // For d='0000-01-01': date_add('0000-01-01', 1 day) = '0000-01-02' >= '0000-01-01' => true
    // If buggy rewrite: d >= days_sub('0000-01-01', 1) = NULL => NULL (wrong!)

    // days lower underflow
    assertTrue(sql("select date_add(d, interval 1 day) >= '0000-01-01' from test_rewrite_arithmetic_61761 where d = '0000-01-01'")[0][0])

    // seconds lower underflow
    assertTrue(sql("select date_add(i, interval 1 second) >= '0000-01-01 00:00:00' from test_rewrite_arithmetic_61761 where i = '0000-01-01 00:00:00'")[0][0])

    // weeks lower underflow
    assertTrue(sql("select date_add(d, interval 1 week) >= '0000-01-01' from test_rewrite_arithmetic_61761 where d = '0000-01-01'")[0][0])

    // hours lower underflow
    assertTrue(sql("select date_add(i, interval 1 hour) >= '0000-01-01 00:00:00' from test_rewrite_arithmetic_61761 where i = '0000-01-01 00:00:00'")[0][0])

    // minutes lower underflow
    assertTrue(sql("select date_add(i, interval 1 minute) >= '0000-01-01 00:00:00' from test_rewrite_arithmetic_61761 where i = '0000-01-01 00:00:00'")[0][0])

    // ==================== Other comparison operators with overflow ====================

    // < operator: date_sub('9999-12-31', 1 day) = '9999-12-30' < '9999-12-31' => true
    // If buggy rewrite: d < NULL => NULL (wrong!)
    assertTrue(sql("select date_sub(d, interval 1 day) < '9999-12-31' from test_rewrite_arithmetic_61761 where d = '9999-12-31'")[0][0])

    // > operator: date_sub('0000-01-01', 1 day) = '0000-00-01' is invalid,
    // but for d='0000-01-01': date_sub('0000-01-01', 1 day) underflows.
    // Test with d='2026-01-20': date_sub('2026-01-20', 1 day) = '2026-01-19' > '9999-12-31' => false
    // If buggy rewrite: d > days_add('9999-12-31', 1) = NULL => NULL (wrong!)
    assertFalse(sql("select date_sub(d, interval 1 day) > '9999-12-31' from test_rewrite_arithmetic_61761 where d = '2026-01-20'")[0][0])

    // ==================== Sanity: safe rewrites still work ====================

    // days: date_sub('2026-01-20', 1 day) = '2026-01-19' <= '2026-01-21' => true (safe rewrite)
    assertTrue(sql("select date_sub(d, interval 1 day) <= '2026-01-21' from test_rewrite_arithmetic_61761 where d = '2026-01-20'")[0][0])

    // seconds: date_sub('2026-01-20 10:00:00', 1 second) = '2026-01-20 09:59:59' <= '2026-01-20 10:00:01' => true
    assertTrue(sql("select date_sub(i, interval 1 second) <= '2026-01-20 10:00:01' from test_rewrite_arithmetic_61761 where i = '2026-01-20 10:00:00'")[0][0])

    // ==================== Right operand is not a Literal ====================

    // When right is a column (not a literal), FoldConstantRule cannot fold the
    // rearranged expression, so overflow check is skipped and rewrite proceeds normally.
    // date_sub('2026-01-20', 1 day) = '2026-01-19' <= '2026-01-20' => true
    assertTrue(sql("select date_sub(d, interval 1 day) <= d from test_rewrite_arithmetic_61761 where d = '2026-01-20'")[0][0])

    // ==================== Numeric overflow does not block rewrite ====================

    // Numeric arithmetic is a mathematical identity; FE folding failure should not
    // prevent the rewrite because the expression is still valid at runtime.
    assertTrue(sql("select cast(d as bigint) + 9223372036854775807 > 1 from test_rewrite_arithmetic_61761 where d = '2026-01-20'")[0][0])

    sql "drop table if exists test_rewrite_arithmetic_61761"
}
