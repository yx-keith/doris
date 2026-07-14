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

suite("test_join_decimal_string_cast") {
    def tableDecimal = "test_join_decimal_string_cast_decimal"
    def tableString = "test_join_decimal_string_cast_string"
    def tableBigInt = "test_join_decimal_string_cast_bigint"
    def tableInt = "test_join_decimal_string_cast_int"

    sql """ drop table if exists ${tableDecimal} """
    sql """ drop table if exists ${tableString} """
    sql """ drop table if exists ${tableBigInt} """
    sql """ drop table if exists ${tableInt} """

    // Test 1: Decimal(38, 0) vs Varchar - precision boundary test
    sql """
        create table ${tableDecimal} (
            k decimal(38, 0) not null,
            v int
        )
        engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    sql """
        create table ${tableString} (
            k varchar(64) not null,
            v int
        )
        engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    // Test with numbers exceeding double precision (2^53)
    sql """
        insert into ${tableDecimal} values
            (9007199254740992, 1),
            (9007199254740993, 2)
    """

    sql """
        insert into ${tableString} values
            ('9007199254740992', 10),
            ('9007199254740993', 20)
    """

    sql "set enable_nereids_planner=false"
    def legacyResult = sql """
        select cast(d.k as string), s.k, d.v, s.v
        from ${tableDecimal} d
        join ${tableString} s on d.k = s.k
        order by d.v, s.v
    """
    assertEquals(2, legacyResult.size())
    assertEquals("9007199254740992", legacyResult[0][0].toString())
    assertEquals("9007199254740992", legacyResult[0][1].toString())
    assertEquals("9007199254740993", legacyResult[1][0].toString())
    assertEquals("9007199254740993", legacyResult[1][1].toString())

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    def nereidsResult = sql """
        select cast(d.k as string), s.k, d.v, s.v
        from ${tableDecimal} d
        join ${tableString} s on d.k = s.k
        order by d.v, s.v
    """
    assertEquals(2, nereidsResult.size())
    assertEquals("9007199254740992", nereidsResult[0][0].toString())
    assertEquals("9007199254740992", nereidsResult[0][1].toString())
    assertEquals("9007199254740993", nereidsResult[1][0].toString())
    assertEquals("9007199254740993", nereidsResult[1][1].toString())

    // Test 2: BigInt vs String - precision boundary test
    sql """
        create table ${tableBigInt} (
            k bigint not null,
            v int
        )
        engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    sql """
        insert into ${tableBigInt} values
            (9007199254740992, 1),
            (9007199254740993, 2)
    """

    sql "set enable_nereids_planner=false"
    def legacyBigIntResult = sql """
        select cast(b.k as string), s.k, b.v, s.v
        from ${tableBigInt} b
        join ${tableString} s on b.k = s.k
        order by b.v, s.v
    """
    assertEquals(2, legacyBigIntResult.size())
    assertEquals("9007199254740992", legacyBigIntResult[0][0].toString())
    assertEquals("9007199254740992", legacyBigIntResult[0][1].toString())
    assertEquals("9007199254740993", legacyBigIntResult[1][0].toString())
    assertEquals("9007199254740993", legacyBigIntResult[1][1].toString())

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    def nereidsBigIntResult = sql """
        select cast(b.k as string), s.k, b.v, s.v
        from ${tableBigInt} b
        join ${tableString} s on b.k = s.k
        order by b.v, s.v
    """
    assertEquals(2, nereidsBigIntResult.size())
    assertEquals("9007199254740992", nereidsBigIntResult[0][0].toString())
    assertEquals("9007199254740992", nereidsBigIntResult[0][1].toString())
    assertEquals("9007199254740993", nereidsBigIntResult[1][0].toString())
    assertEquals("9007199254740993", nereidsBigIntResult[1][1].toString())

    // Test 3: Int vs String
    sql """
        create table ${tableInt} (
            k int not null,
            v int
        )
        engine=olap
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        )
    """

    sql """
        insert into ${tableInt} values
            (123456789, 1),
            (987654321, 2)
    """

    sql """ truncate table ${tableString} """
    sql """
        insert into ${tableString} values
            ('123456789', 10),
            ('987654321', 20)
    """

    sql "set enable_nereids_planner=false"
    def legacyIntResult = sql """
        select i.k, s.k, i.v, s.v
        from ${tableInt} i
        join ${tableString} s on i.k = s.k
        order by i.v, s.v
    """
    assertEquals(2, legacyIntResult.size())
    assertEquals(123456789, legacyIntResult[0][0])
    assertEquals("123456789", legacyIntResult[0][1].toString())
    assertEquals(987654321, legacyIntResult[1][0])
    assertEquals("987654321", legacyIntResult[1][1].toString())

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    def nereidsIntResult = sql """
        select i.k, s.k, i.v, s.v
        from ${tableInt} i
        join ${tableString} s on i.k = s.k
        order by i.v, s.v
    """
    assertEquals(2, nereidsIntResult.size())
    assertEquals(123456789, nereidsIntResult[0][0])
    assertEquals("123456789", nereidsIntResult[0][1].toString())
    assertEquals(987654321, nereidsIntResult[1][0])
    assertEquals("987654321", nereidsIntResult[1][1].toString())

    // Test 4: Edge cases - invalid strings, empty strings
    sql """ truncate table ${tableDecimal} """
    sql """ truncate table ${tableString} """

    sql """
        insert into ${tableDecimal} values
            (100, 1),
            (200, 2),
            (300, 3)
    """

    sql """
        insert into ${tableString} values
            ('100', 10),
            ('200', 20),
            ('invalid', 30),
            ('', 40),
            ('300.5', 50)
    """

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    def edgeCaseResult = sql """
        select cast(d.k as string), s.k, d.v, s.v
        from ${tableDecimal} d
        join ${tableString} s on d.k = s.k
        order by d.v, s.v
    """
    // Only exact matches should join
    assertEquals(2, edgeCaseResult.size())
    assertEquals("100", edgeCaseResult[0][0].toString())
    assertEquals("100", edgeCaseResult[0][1].toString())
    assertEquals("200", edgeCaseResult[1][0].toString())
    assertEquals("200", edgeCaseResult[1][1].toString())

    sql """ drop table if exists ${tableDecimal} """
    sql """ drop table if exists ${tableString} """
    sql """ drop table if exists ${tableBigInt} """
    sql """ drop table if exists ${tableInt} """
}
