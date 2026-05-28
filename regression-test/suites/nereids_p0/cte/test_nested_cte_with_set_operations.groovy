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

suite("test_nested_cte_with_set_operations") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_sales;"
    sql """
        CREATE TABLE `test_sales`(
            `id` int,
            `item` string,  
            `amount` double, 
            `year` int, 
            `month` int) properties (replication_num=1);
    """

    sql """
        INSERT INTO `test_sales` 
        (`id`, `item`, `amount`, `year`, `month`) VALUES
        (1,  'A', 100.0, 2023, 1),
        (2,  'B', 200.0, 2023, 2),
        (3,  'C', 200.0, 2024, 3),
        (4,  'D', 300.0, 2024, 4),
        (5,  'E', 400.0, 2024, 5),
        (6,  'F', 400.0, 2025, 6),
        (7,  'G', 500.0, 2025, 7);
    """

    sql """
           WITH
           tbl1 as (select amount from test_sales where year = 2023 EXCEPT select amount from test_sales where year = 2024),
           tbl2 as (select amount from test_sales where year = 2024 EXCEPT select amount from test_sales where year = 2025),
           total as (select (select count(1) from tbl1), (select count(1) from tbl2))
           select * from tbl1 UNION ALL select * from tbl2;
    """
}
