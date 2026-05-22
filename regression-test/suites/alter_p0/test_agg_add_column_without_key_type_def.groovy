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

suite('test_agg_add_column_without_key_type_def') {
    def dbName='test_agg_add_column_without_key_type_def_db'
    def tableName = 'test_agg_add_column_without_key_type_def_tbl'
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql """create database ${dbName}"""
    sql """
        create table ${dbName}.${tableName}(
            k1 int(11) null,
            v1 int(11) sum null
        ) engine=olap
        aggregate key(k1)
        distributed by hash(k1) buckets 1
        properties("replication_allocation" = "tag.location.default: 1");
    """

    // Negative case: add double/float column without aggregate type should be rejected
    // because they will be treated as key column, but float/double cannot be key
    def columnTypes = ["double", "float"]
    def startColIndex = 2
    for (final def columnType in columnTypes) {
        def columnName = "k${startColIndex}"
        startColIndex++
        test {
            sql """alter table ${dbName}.${tableName} add column (${columnName} ${columnType} comment 'add column ${columnName}');"""
            exception "Float or double can not used as a key, use decimal instead."
        }
    }

    // Positive case: add double/float column with aggregate type should succeed
    startColIndex = 2
    for (final def columnType in columnTypes) {
        def columnName = "v${startColIndex}"
        startColIndex++
        sql """alter table ${dbName}.${tableName} add column (${columnName} ${columnType} sum comment 'add column ${columnName}');"""
    }

    // Light schema change completes immediately, verify columns are added
    def showResult = sql """SHOW CREATE TABLE ${dbName}.${tableName}"""
    assertTrue(showResult[0][1].contains("v2"))
    assertTrue(showResult[0][1].contains("v3"))

    sql """insert into ${dbName}.${tableName} values(1, 1, 1.0, 1.0);"""
    def result = sql """ select * from ${dbName}.${tableName} order by k1 """
    assertTrue(result.size() == 1)
    assertTrue(result[0][0] == 1)
    assertTrue(result[0][1] == 1)
    assertTrue(result[0][2] == 1.0)
    assertTrue(result[0][3] == 1.0)

    // Insert again with same key, verify sum aggregation works correctly
    sql """insert into ${dbName}.${tableName} values(1, 2, 2.0, 2.0);"""
    result = sql """ select * from ${dbName}.${tableName} order by k1 """
    assertTrue(result.size() == 1)
    assertTrue(result[0][0] == 1)
    assertTrue(result[0][1] == 3)
    assertTrue(result[0][2] == 3.0)
    assertTrue(result[0][3] == 3.0)

    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
}
