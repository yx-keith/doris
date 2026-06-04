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

suite("test_varchar_bigint_join_precision") {
    sql """ DROP TABLE IF EXISTS `test_varchar_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_varchar_bigint_join_t2` """
    sql """ DROP TABLE IF EXISTS `test_char_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_char_bigint_join_t2` """
    sql """ DROP TABLE IF EXISTS `test_string_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_string_bigint_join_t2` """

    // Test 1: varchar(bigint) = bigint
    sql """
        CREATE TABLE `test_varchar_bigint_join_t1` (
            `id` bigint NULL,
            `org_id` varchar(765) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        CREATE TABLE `test_varchar_bigint_join_t2` (
            `id` bigint NULL,
            `id_path` varchar(3072) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        INSERT INTO test_varchar_bigint_join_t1 VALUES
            (1, '1945392950687043594')
    """

    sql """
        INSERT INTO test_varchar_bigint_join_t2 VALUES
            (1945392950687043591, '/a/b/c/1'),
            (1945392950687043592, '/a/b/c/2'),
            (1945392950687043594, '/a/b/c/4'),
            (1945392950687043597, '/a/b/c/7')
    """

    qt_join """ select t1.id, t2.id, t2.id_path
                from test_varchar_bigint_join_t1 t1
                left join test_varchar_bigint_join_t2 t2 on t1.org_id = t2.id
                order by t1.id """

    qt_where """ select * from test_varchar_bigint_join_t2
                 where id = '1945392950687043594' """

    qt_in """ select * from test_varchar_bigint_join_t2
              where id in ('1945392950687043594', '1945392950687043597') order by id """

    sql """ DROP TABLE IF EXISTS `test_varchar_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_varchar_bigint_join_t2` """

    // Test 2: char(bigint) = bigint
    sql """
        CREATE TABLE `test_char_bigint_join_t1` (
            `id` bigint NULL,
            `org_id` char(50) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        CREATE TABLE `test_char_bigint_join_t2` (
            `id` bigint NULL,
            `id_path` char(100) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        INSERT INTO test_char_bigint_join_t1 VALUES
            (1, '1945392950687043594')
    """

    sql """
        INSERT INTO test_char_bigint_join_t2 VALUES
            (1945392950687043591, '/a/b/c/1'),
            (1945392950687043592, '/a/b/c/2'),
            (1945392950687043594, '/a/b/c/4'),
            (1945392950687043597, '/a/b/c/7')
    """

    qt_join """ select t1.id, t2.id, t2.id_path
                from test_char_bigint_join_t1 t1
                left join test_char_bigint_join_t2 t2 on t1.org_id = t2.id
                order by t1.id """

    qt_where """ select * from test_char_bigint_join_t2
                 where id = '1945392950687043594' """

    qt_in """ select * from test_char_bigint_join_t2
              where id in ('1945392950687043594', '1945392950687043597') order by id """

    sql """ DROP TABLE IF EXISTS `test_char_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_char_bigint_join_t2` """

    // Test 3: string(bigint) = bigint
    sql """
        CREATE TABLE `test_string_bigint_join_t1` (
            `id` bigint NULL,
            `org_id` string NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        CREATE TABLE `test_string_bigint_join_t2` (
            `id` bigint NULL,
            `id_path` string NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default:1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true"
        )
    """

    sql """
        INSERT INTO test_string_bigint_join_t1 VALUES
            (1, '1945392950687043594')
    """

    sql """
        INSERT INTO test_string_bigint_join_t2 VALUES
            (1945392950687043591, '/a/b/c/1'),
            (1945392950687043592, '/a/b/c/2'),
            (1945392950687043594, '/a/b/c/4'),
            (1945392950687043597, '/a/b/c/7')
    """

    qt_join """ select t1.id, t2.id, t2.id_path
                from test_string_bigint_join_t1 t1
                left join test_string_bigint_join_t2 t2 on t1.org_id = t2.id
                order by t1.id """

    qt_where """ select * from test_string_bigint_join_t2
                 where id = '1945392950687043594' """

    qt_in """ select * from test_string_bigint_join_t2
              where id in ('1945392950687043594', '1945392950687043597') order by id """

    sql """ DROP TABLE IF EXISTS `test_string_bigint_join_t1` """
    sql """ DROP TABLE IF EXISTS `test_string_bigint_join_t2` """
}
