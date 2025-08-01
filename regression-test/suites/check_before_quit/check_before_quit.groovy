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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("check_before_quit", "nonConcurrent,p0") {
    List<List<Object>> be_result = sql "show backends"
    List<Object> beRow = be_result.get(0)
    String beHost = beRow.get(1).toString().trim()
    String bePort =  beRow.get(4).toString().trim()
    String beBrpcPort = beRow.get(5).toString().trim()
    logger.info("get be host and port:" + beHost + " " + bePort + ", BrpcPort:" + beBrpcPort)

    //NOTE: this suite is used to check whether workload group's query queue works correctly when all query finished
    long beginTime = System.currentTimeMillis();
    long timeoutMs = 300 * 1000 // 300s
    boolean clear = false

    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        List<List<Object>> result = sql "show workload groups;"
        logger.info("result, ${result}")

        clear = true
        for (int i = 0; i < result.size(); i++) {
            List<Object> row = result.get(i)
            int col_size = row.size()
            int running_query_num = Integer.valueOf(row.get(col_size - 2).toString())
            int waiting_query_num = Integer.valueOf(row.get(col_size - 1).toString())
            if (running_query_num != 0 || waiting_query_num != 0) {
                clear = false
                break
            }
        }

        if (clear) {
            break
        }

        Thread.sleep(500)
    }

    if (clear == false) {
        List<List<Object>> result = sql "select QUERY_ID,QUERY_START_TIME,QUERY_TIME_MS,WORKLOAD_GROUP_ID,SQL,QUERY_STATUS from information_schema.active_queries"
        logger.info("active queries: ${result}")
        if (result.size() == 1) {
            clear = true
        }

        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("curl http://" + beHost + ":" + bePort + "/api/running_pipeline_tasks/10")
        String command = strBuilder.toString()
        def process = command.toString().execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Request be running task: code=" + code + ", err=" + err)
        logger.info("print be task:${out}")
    }

    assertTrue(clear)

    // check spill temporary files is cleared
    // Function to parse the metrics
    def getPrometheusMetric = { String data, String name -> {
        def metricValues = []
        data.eachLine { line ->
            line = line.trim()

            // Skip comment lines
            if (line.startsWith('#')) return

            // Regular expression to match metric lines
            def matcher = (line =~ /^(\w+)(\{[^}]+\})?\s+(.+)$/)

            if (matcher) {
                def metricName = matcher[0][1]
                def labels = matcher[0][2]
                def value = matcher[0][3]
                if (metricName == name) {
                    metricValues << value
                }
            }
        }
        return metricValues
    } }

    def getVar = { String data, String name -> {
        def varValues = []
        data.eachLine { line ->
            line = line.trim()

            // Skip comment lines
            if (line.startsWith('#')) return

            // Regular expression to match metric lines
            def matcher = (line =~ /^(\w+)\s:\s(.+)$/)

            if (matcher) {
                def varName = matcher[0][1]
                def varValue = matcher[0][2]
                if (varName == name) {
                    varValues << varValue
                }
            }
        }
        return varValues
    } }

    beginTime = System.currentTimeMillis();
    timeoutMs = 30 * 1000 // 30s
    clear = false

    def command_metrics = "curl http://${beHost}:${bePort}/metrics"
    def command_vars = "curl http://${beHost}:${beBrpcPort}/vars"
    def command_load_channels = "curl http://${beHost}:${bePort}/api/load_channels"
    def command_load_streams = "curl http://${beHost}:${bePort}/api/load_streams"
    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        clear = true
        logger.info("executing command: ${command_metrics}")
        def process_metrics = command_metrics.execute()
        def outputStream_metrics = new StringBuffer()
        def errorStream_metrics = new StringBuffer()
        process_metrics.consumeProcessOutput(outputStream_metrics, errorStream_metrics)
        def code_metrics = process_metrics.waitFor()
        def metrics = outputStream_metrics.toString()
        logger.info("Request BE metrics: code=" + code_metrics + ", err=" + errorStream_metrics.toString())

        logger.info("executing command: ${command_vars}")
        def process_vars = command_vars.execute()
        def outputStream_vars = new StringBuffer()
        def errorStream_vars = new StringBuffer()
        process_vars.consumeProcessOutput(outputStream_vars, errorStream_vars)
        def code_vars = process_vars.waitFor()
        def vars = outputStream_vars.toString()
        logger.info("Request BE vars: code=" + code_vars + ", err=" + errorStream_vars.toString())

        def hasSpillData = getPrometheusMetric(metrics, "doris_be_spill_disk_has_spill_data")
        logger.info("has spill temporary files :${hasSpillData}")
        for (int i = 0; i < hasSpillData.size(); i++) {
            if (0 != Integer.valueOf(hasSpillData.get(i))) {
                clear = false;
                break;
            }
        }

        hasSpillData = getPrometheusMetric(metrics, "doris_be_spill_disk_has_spill_gc_data")
        logger.info("has spill temporary files :${hasSpillData}")
        for (int i = 0; i < hasSpillData.size(); i++) {
            if (0 != Integer.valueOf(hasSpillData.get(i))) {
                clear = false;
                break;
            }
        }

        def spill_data_sizes = getPrometheusMetric(metrics, "doris_be_spill_disk_data_size")
        logger.info("spill data sizes :${spill_data_sizes}")
        for (int i = 0; i < spill_data_sizes.size(); i++) {
            if (0 != Integer.valueOf(spill_data_sizes.get(i))) {
                clear = false;
                break;
            }
        }

        def doris_be_load_channel_count = getPrometheusMetric(metrics, "doris_be_load_channel_count")
        logger.info("doris_be_load_channel_count :${doris_be_load_channel_count}")
        for (int i = 0; i < doris_be_load_channel_count.size(); i++) {
            if (0 != Integer.valueOf(doris_be_load_channel_count.get(i))) {
                clear = false;
                break;
            }
        }

        def load_stream_count = getVar(vars, "load_stream_count")
        logger.info("load_stream_count :${load_stream_count}")
        for (int i = 0; i < load_stream_count.size(); i++) {
            if (0 != Integer.valueOf(load_stream_count.get(i))) {
                clear = false;
                break;
            }
        }

        def load_stream_writer_count = getVar(vars, "load_stream_writer_count")
        logger.info("load_stream_writer_count :${load_stream_writer_count}")
        for (int i = 0; i < load_stream_writer_count.size(); i++) {
            if (0 != Integer.valueOf(load_stream_writer_count.get(i))) {
                clear = false;
                break;
            }
        }

        def load_stream_file_writer_count = getVar(vars, "load_stream_file_writer_count")
        logger.info("load_stream_file_writer_count :${load_stream_file_writer_count}")
        for (int i = 0; i < load_stream_file_writer_count.size(); i++) {
            if (0 != Integer.valueOf(load_stream_file_writer_count.get(i))) {
                clear = false;
                break;
            }
        }

        if (clear) {
            break
        }

        logger.info("executing command: ${command_load_channels}")
        def process_load_channels = command_load_channels.execute()
        def outputStream_load_channels = new StringBuffer()
        def errorStream_load_channels = new StringBuffer()
        process_load_channels.consumeProcessOutput(outputStream_load_channels, errorStream_load_channels)
        def code_load_channels = process_load_channels.waitFor()
        def load_channels = outputStream_load_channels.toString()
        logger.info("Request BE load_channels: code=" + code_load_channels + ", err=" + errorStream_load_channels.toString())
        logger.info("load_channels: " + load_channels);

        logger.info("executing command: ${command_load_streams}")
        def process_load_streams = command_load_streams.execute()
        def outputStream_load_streams = new StringBuffer()
        def errorStream_load_streams = new StringBuffer()
        process_load_streams.consumeProcessOutput(outputStream_load_streams, errorStream_load_streams)
        def code_load_streams = process_load_streams.waitFor()
        def load_streams = outputStream_load_streams.toString()
        logger.info("Request BE load_streams: code=" + code_load_streams + ", err=" + errorStream_load_streams.toString())
        logger.info("load_streams: " + load_streams);

        Thread.sleep(2000)
    }

    // check create table sql
    List<List<Object>> allDataBases = sql "show databases;"
    logger.info("show all databases: ${allDataBases}")

    def num = allDataBases.size()
    def failureList = []

    sql "set enable_decimal256 = true;"
    sql """
        ADMIN SET ALL FRONTENDS CONFIG ('enable_inverted_index_v1_for_variant' = 'true');
    """

    for (int i = 0; i < num; i++) {
        def db = allDataBases[i][0]
        if (db == "__internal_schema" || db == "information_schema" || db == "mysql") {
            continue
        }
        List<List<Object>> allTables = sql "show tables from ${db}"
        logger.info("show all tables: ${allTables}")
        for (int j = 0; j < allTables.size(); j++) {
            def tbl = allTables[j][0]
            def createTableSql = ""
            try {
                // cloud mode not allowed to set light_schema_change = true
                if (!isCloudMode()) {
                    createTableSql = sql "show create table ${db}.${tbl}"
                    if (createTableSql[0][1].contains("CREATE TABLE")) {
                        try {
                            sql " ALTER TABLE ${db}.${tbl} SET (\"light_schema_change\" = \"true\") "
                        } catch (Exception alterEx) {
                            logger.warn("Failed to alter table ${db}.${tbl} to set light_schema_change: ${alterEx.getMessage()}")
                            failureList << [
                                operation: "ALTER TABLE",
                                target: "${tbl}", 
                                error: alterEx.getMessage()
                            ]
                        }
                    }
                    createTableSql = sql "show create table ${db}.${tbl}"
                }
            } catch (Exception e) {
                if (e.getMessage().contains("not support async materialized view")) {
                    try {
                        createTableSql = sql "show create materialized view ${db}.${tbl}"
                    } catch (Exception e2) {
                        if (e2.getMessage().contains("table not found")) {
                            continue
                        } else {
                            logger.info(e2.getMessage())
                            failureList << [
                                operation: "SHOW CREATE MATERIALIZED VIEW", 
                                target: "${db}.${tbl}", 
                                error: e2.getMessage()
                            ]
                            continue
                        }
                    }
                    logger.info("create materialized view sql: ${createTableSql}")
                } else {
                    logger.warn("Failed to show create table ${db}.${tbl}: ${e.getMessage()}")
                    failureList << [
                        operation: "SHOW CREATE TABLE",
                        target: "${db}.${tbl}", 
                        error: e.getMessage()
                    ]
                    continue
                }
            }

            try {
                if (createTableSql[0][1].contains("CREATE VIEW")) {
                    sql "drop view if exists ${tbl}"
                } else if (createTableSql[0][1].contains("CREATE MATERIALIZED VIEW")) {
                    sql "drop materialized view if exists ${tbl}"
                } else {
                    sql "drop table if exists ${tbl}"
                    // only re create table, because the table which view depends may be dropped,
                    // so recreate view may fail
                    try {
                        if (createTableSql[0][1].contains("agg_state<")) {
                            logger.info("Skipping table ${tbl} because it contains agg_state column type")
                            continue
                        }

                        sql(createTableSql[0][1])
                        def createTableSqlResult = sql "show create table ${tbl}"

                        def eraseReplicationAllocationProperty = { String sql ->
                            def result = sql.replaceAll(/(,\s*)?"replication_allocation"\s*=\s*"tag\.location\.default:\s*[^"]*"(,\s*|\s*\))/, '$2')
                            result = result.replaceAll(/PROPERTIES\s*\(\s*,/, 'PROPERTIES (')
                            result = result.replaceAll(/,(\s*,)+/, ',')
                            return result.trim()
                        }
                        createTableSql = eraseReplicationAllocationProperty(createTableSql[0][1])
                        createTableSqlResult = eraseReplicationAllocationProperty(createTableSqlResult[0][1])
                        
                        logger.info("target: ${createTableSqlResult}, origin: ${createTableSql}")

                        if (createTableSqlResult.trim() != createTableSql.trim()) {
                            failureList << [
                                operation: "CREATE TABLE", 
                                target: "${tbl}", 
                                expected: createTableSql.trim(),
                                actual: createTableSqlResult.trim()
                            ]
                        }
                    } catch (Exception recreateEx) {
                        logger.warn("Failed to recreate table ${tbl}: ${recreateEx.getMessage()}")
                        failureList << [
                            operation: "RECREATE TABLE", 
                            target: "${tbl}", 
                            sql: createTableSql,
                            error: recreateEx.getMessage()
                        ]
                    }
                }
            } catch (Exception dropEx) {
                logger.warn("Failed during drop operation for ${tbl}: ${dropEx.getMessage()}")
                failureList << [
                    operation: "DROP", 
                    target: "${tbl}", 
                    error: dropEx.getMessage()
                ]
            }
        }
    }

    if (failureList.size() > 0) {
        logger.error("Total failures: ${failureList.size()}")
        failureList.eachWithIndex { failure, idx ->
            logger.error("Failure #${idx + 1}:")
            failure.each { key, value ->
                if (key == "expected" || key == "actual" || key == "sql") {
                    logger.error("  ${key}:\n${value}")
                } else {
                    logger.error("  ${key}: ${value}")
                }
            }
        }
        logger.error("Found ${failureList.size()} failures during database recreation checks")
    } else {
        clear = true
    }

    assertTrue(clear)
}
