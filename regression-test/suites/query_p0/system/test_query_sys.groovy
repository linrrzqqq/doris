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

suite("test_query_sys", "query,p0") {
    sql "use test_query_db;"

    def tableName = "test"
    sql "SELECT DATABASE();"
    sql "SELECT \"welecome to my blog!\";"
    sql "describe ${tableName};"
    sql "select version();"
    sql "select rand();"
    sql "select rand(20);"
    sql "select random();"
    sql "select random(20);"
    sql "select rand(1, 10);"
    sql "select random(-5, -3);"
    test{
        sql "select rand(10,1);"
        exception "random's lower bound should less than upper bound"
    }
    sql "SELECT CONNECTION_ID();"
    sql "SELECT CURRENT_USER();"
    sql "SELECT SESSION_USER();"
    sql "SELECT CURRENT_CATALOG();"
    // sql "select now();"
    sql "select localtime();"
    sql "select localtimestamp();"
    sql "select pi();"
    sql "select e();"
    sql "select sleep(2);"
    sql "select sleep(null);"
    sql "select last_query_id();"
    sql "select LAST_QUERY_ID();"

    // INFORMATION_SCHEMA
    sql "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema=\"test_query_db\" and TABLE_TYPE = \"BASE TABLE\" order by table_name"
    sql "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \"${tableName}\" AND table_schema =\"test_query_db\" AND column_name LIKE \"k%\""

    test {
        sql "select * from http_stream('format'='csv');"
        exception "No Alive backends"
    }

    // `workload_group_resource_usage` will be refresh 30s after BE startup so sleep 30s to get a stable result
    sleep(30000)
    sql """set parallel_pipeline_task_num=8"""
    def rows1 = sql """ select count(*) from information_schema.workload_group_resource_usage; """
    sql """set parallel_pipeline_task_num=1"""
    def rows2 = sql """ select count(*) from information_schema.workload_group_resource_usage; """
    assertEquals(rows1, rows2)

    sql "set debug_skip_fold_constant=true;"
    sql "select version();"
}
