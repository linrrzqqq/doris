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

import org.junit.Assert

suite("test_javaudf_deterministic") {
    def tableName = "test_javaudf_deterministic_seed"
    def udfMvName = "java_udf_deterministic_false_mtmv"
    def udafMvName = "java_udaf_deterministic_false_mtmv"
    def udfDetMvName = "java_udf_deterministic_true_mtmv"
    def udafDetMvName = "java_udaf_deterministic_true_mtmv"
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    try {
        sql """ SET enable_nereids_planner = true; """
        sql """ SET enable_fallback_to_original_planner = false; """

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS ${udfMvName}; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS ${udafMvName}; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS ${udfDetMvName}; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS ${udafDetMvName}; """
        sql """ DROP FUNCTION IF EXISTS java_udf_deterministic_test(INT); """
        sql """ DROP FUNCTION IF EXISTS java_udaf_deterministic_test(INT); """
        sql """ DROP FUNCTION IF EXISTS java_udf_deterministic_true_test(INT); """
        sql """ DROP FUNCTION IF EXISTS java_udaf_deterministic_true_test(INT); """

        sql """
        CREATE TABLE ${tableName} (
            id INT,
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES (1, 10), (2, 20), (3, 30); """
        sql """ sync; """

        File jarFile = new File(jarPath)
        if (!jarFile.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """
        CREATE FUNCTION java_udf_deterministic_test(INT) RETURNS INT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF"
        );
        """
        def udfShowDefault = sql """ SHOW CREATE FUNCTION java_udf_deterministic_test(INT); """
        assertTrue(udfShowDefault.size() == 1)
        assertTrue(udfShowDefault[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE AGGREGATE FUNCTION java_udaf_deterministic_test(INT) RETURNS BIGINT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumInt",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        );
        """
        def udafShowDefault = sql """ SHOW CREATE FUNCTION java_udaf_deterministic_test(INT); """
        assertTrue(udafShowDefault.size() == 1)
        assertTrue(udafShowDefault[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE FUNCTION java_udf_deterministic_true_test(INT) RETURNS INT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF",
            "deterministic"="true"
        );
        """
        def udfShowDet = sql """ SHOW CREATE FUNCTION java_udf_deterministic_true_test(INT); """
        assertTrue(udfShowDet.size() == 1)
        assertTrue(udfShowDet[0][1].contains("\"DETERMINISTIC\"=\"true\""))

        sql """
        CREATE AGGREGATE FUNCTION java_udaf_deterministic_true_test(INT) RETURNS BIGINT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumInt",
            "always_nullable"="false",
            "type"="JAVA_UDF",
            "deterministic"="true"
        );
        """
        def udafShowDet = sql """ SHOW CREATE FUNCTION java_udaf_deterministic_true_test(INT); """
        assertTrue(udafShowDet.size() == 1)
        assertTrue(udafShowDet[0][1].contains("\"DETERMINISTIC\"=\"true\""))

        try {
            sql """
            CREATE MATERIALIZED VIEW ${udfMvName}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS
            SELECT id, java_udf_deterministic_test(v) AS result
            FROM ${tableName};
            """
            Assert.fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"))
        }

        try {
            sql """
            CREATE MATERIALIZED VIEW ${udafMvName}
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS
            SELECT id, java_udaf_deterministic_test(v) AS result
            FROM ${tableName}
            GROUP BY id;
            """
            Assert.fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"))
        }

        sql """
        CREATE MATERIALIZED VIEW ${udfDetMvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT id, java_udf_deterministic_true_test(v) AS result
        FROM ${tableName};
        """
        def udfDetShow = sql """ SHOW CREATE MATERIALIZED VIEW ${udfDetMvName}; """
        assertTrue(udfDetShow.size() == 1)

        sql """
        CREATE MATERIALIZED VIEW ${udafDetMvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT id, java_udaf_deterministic_true_test(v) AS result
        FROM ${tableName}
        GROUP BY id;
        """
        def udafDetShow = sql """ SHOW CREATE MATERIALIZED VIEW ${udafDetMvName}; """
        assertTrue(udafDetShow.size() == 1)
    } finally {
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS ${udfMvName}; """)
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS ${udafMvName}; """)
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS ${udfDetMvName}; """)
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS ${udafDetMvName}; """)
        try_sql(""" DROP FUNCTION IF EXISTS java_udf_deterministic_test(INT); """)
        try_sql(""" DROP FUNCTION IF EXISTS java_udaf_deterministic_test(INT); """)
        try_sql(""" DROP FUNCTION IF EXISTS java_udf_deterministic_true_test(INT); """)
        try_sql(""" DROP FUNCTION IF EXISTS java_udaf_deterministic_true_test(INT); """)
        try_sql(""" DROP TABLE IF EXISTS ${tableName}; """)
    }
}
