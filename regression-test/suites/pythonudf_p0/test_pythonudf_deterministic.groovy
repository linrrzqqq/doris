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

import org.junit.Assert;

suite("test_pythonudf_deterministic") {
    def runtime_version = getPythonUdfRuntimeVersion()

    try {
        sql """ DROP TABLE IF EXISTS cte_uuid_seed; """
        sql """ DROP TABLE IF EXISTS mtmv_uuid_seed; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS py_uuid_token_mtmv; """
        sql """ DROP FUNCTION IF EXISTS py_uuid_token_false(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_token_det(INT); """
        sql """
        CREATE TABLE cte_uuid_seed (id INT) ENGINE=OLAP DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        sql """ INSERT INTO cte_uuid_seed VALUES (1), (2), (3); """
        sql """ sync; """

        sql """ SET enable_nereids_planner = true; """
        sql """ SET enable_fallback_to_original_planner = false; """

        sql """ DROP FUNCTION IF EXISTS py_uuid_token(INT); """
        sql """
        CREATE FUNCTION py_uuid_token(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_uuid_token_impl",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import uuid

def py_uuid_token_impl(x):
    return f"{x}-{uuid.uuid4()}"
\$\$;
        """
        def showDefault = sql """ SHOW CREATE FUNCTION py_uuid_token(INT); """
        assertTrue(showDefault.size() == 1)
        assertTrue(showDefault[0][1].contains("DETERMINISTIC"))
        assertTrue(showDefault[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE FUNCTION py_uuid_token_false(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_uuid_token_impl",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}",
            "deterministic" = "false"
        )
        AS \$\$
import uuid

def py_uuid_token_impl(x):
    return f"{x}-{uuid.uuid4()}"
\$\$;
        """
        def showExplicitFalse = sql """ SHOW CREATE FUNCTION py_uuid_token_false(INT); """
        assertTrue(showExplicitFalse.size() == 1)
        assertTrue(showExplicitFalse[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE FUNCTION py_uuid_token_det(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_uuid_token_det_impl",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}",
            "deterministic" = "true"
        )
        AS \$\$
import uuid

def py_uuid_token_det_impl(x):
    return f"{x}-{uuid.uuid4()}"
\$\$;
        """
        def showDet = sql """ SHOW CREATE FUNCTION py_uuid_token_det(INT); """
        assertTrue(showDet.size() == 1)
        assertTrue(showDet[0][1].contains("\"DETERMINISTIC\"=\"true\""))

        sql """ SET enable_cte_materialize = true; """
        sql """ SET inline_cte_referenced_threshold = 1; """
        qt_materialized """
        WITH cte AS (
            SELECT id, py_uuid_token(id) AS token
            FROM cte_uuid_seed
        )
        SELECT id, COUNT(DISTINCT token) AS distinct_tokens
        FROM (
            SELECT id, token FROM cte
            UNION ALL
            SELECT id, token FROM cte
        ) u
        GROUP BY id
        ORDER BY id;
        """

        sql """ SET enable_cte_materialize = true; """
        sql """ SET inline_cte_referenced_threshold = 10; """
        qt_inlined """
        WITH cte AS (
            SELECT id, py_uuid_token(id) AS token
            FROM cte_uuid_seed
        )
        SELECT id, COUNT(DISTINCT token) AS distinct_tokens
        FROM (
            SELECT id, token FROM cte
            UNION ALL
            SELECT id, token FROM cte
        ) u
        GROUP BY id
        ORDER BY id;
        """

        sql """ SET enable_cte_materialize = true; """
        sql """ SET inline_cte_referenced_threshold = 1; """
        qt_materialized_det """
        WITH cte AS (
            SELECT id, py_uuid_token_det(id) AS token
            FROM cte_uuid_seed
        )
        SELECT id, COUNT(DISTINCT token) AS distinct_tokens
        FROM (
            SELECT id, token FROM cte
            UNION ALL
            SELECT id, token FROM cte
        ) u
        GROUP BY id
        ORDER BY id;
        """

        sql """ SET enable_cte_materialize = true; """
        sql """ SET inline_cte_referenced_threshold = 10; """
        qt_inlined_det """
        WITH cte AS (
            SELECT id, py_uuid_token_det(id) AS token
            FROM cte_uuid_seed
        )
        SELECT id, COUNT(DISTINCT token) AS distinct_tokens
        FROM (
            SELECT id, token FROM cte
            UNION ALL
            SELECT id, token FROM cte
        ) u
        GROUP BY id
        ORDER BY id;
        """

        sql """
        CREATE TABLE mtmv_uuid_seed (id INT) ENGINE=OLAP DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        sql """ INSERT INTO mtmv_uuid_seed VALUES (1), (2), (3); """
        sql """ sync; """

        try {
            sql """
            CREATE MATERIALIZED VIEW py_uuid_token_mtmv
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS
            SELECT id, py_uuid_token(id) AS token
            FROM mtmv_uuid_seed;
            """
            Assert.fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"))
        }
    } finally {
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS py_uuid_token_mtmv; """)
        sql """ DROP FUNCTION IF EXISTS py_uuid_token(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_token_false(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_token_det(INT); """
        sql """ DROP TABLE IF EXISTS cte_uuid_seed; """
        sql """ DROP TABLE IF EXISTS mtmv_uuid_seed; """
    }
}
