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

    sql """ drop table if exists ${tableDecimal} """
    sql """ drop table if exists ${tableString} """

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

    sql """ drop table if exists ${tableDecimal} """
    sql """ drop table if exists ${tableString} """
}
