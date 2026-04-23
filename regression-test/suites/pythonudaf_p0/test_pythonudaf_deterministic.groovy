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

suite("test_pythonudaf_deterministic") {
    def runtime_version = getPythonUdfRuntimeVersion()

    try {
        sql """ DROP TABLE IF EXISTS cte_uuid_seed; """
        sql """ DROP TABLE IF EXISTS mtmv_uuid_seed; """
        sql """ DROP MATERIALIZED VIEW IF EXISTS py_uuid_agg_mtmv; """
        sql """ DROP FUNCTION IF EXISTS py_uuid_agg_false(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_agg_det(INT); """
        sql """
        CREATE TABLE cte_uuid_seed (id INT) ENGINE=OLAP DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        sql """ INSERT INTO cte_uuid_seed VALUES (1), (2), (3); """
        sql """ sync; """

        sql """ SET enable_nereids_planner = true; """
        sql """ SET enable_fallback_to_original_planner = false; """

        sql """ DROP FUNCTION IF EXISTS py_uuid_agg(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_uuid_agg(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "PyUuidAgg",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import uuid

class PyUuidAgg:
    def __init__(self):
        self.last = None

    def accumulate(self, value):
        if value is not None:
            self.last = value

    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state

    def finish(self):
        return f"{self.last}-{uuid.uuid4()}"

    @property
    def aggregate_state(self):
        return self.last
\$\$;
        """
        def showDefault = sql """ SHOW CREATE FUNCTION py_uuid_agg(INT); """
        assertTrue(showDefault.size() == 1)
        assertTrue(showDefault[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE AGGREGATE FUNCTION py_uuid_agg_false(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "PyUuidAggFalse",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}",
            "deterministic" = "false"
        )
        AS \$\$
import uuid

class PyUuidAggFalse:
    def __init__(self):
        self.last = None

    def accumulate(self, value):
        if value is not None:
            self.last = value

    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state

    def finish(self):
        return f"{self.last}-{uuid.uuid4()}"

    @property
    def aggregate_state(self):
        return self.last
\$\$;
        """
        def showExplicitFalse = sql """ SHOW CREATE FUNCTION py_uuid_agg_false(INT); """
        assertTrue(showExplicitFalse.size() == 1)
        assertTrue(showExplicitFalse[0][1].contains("\"DETERMINISTIC\"=\"false\""))

        sql """
        CREATE AGGREGATE FUNCTION py_uuid_agg_det(INT)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "PyUuidAggDet",
            "always_nullable" = "false",
            "runtime_version" = "${runtime_version}",
            "deterministic" = "true"
        )
        AS \$\$
import uuid

class PyUuidAggDet:
    def __init__(self):
        self.last = None

    def accumulate(self, value):
        if value is not None:
            self.last = value

    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state

    def finish(self):
        return f"{self.last}-{uuid.uuid4()}"

    @property
    def aggregate_state(self):
        return self.last
\$\$;
        """
        def showDet = sql """ SHOW CREATE FUNCTION py_uuid_agg_det(INT); """
        assertTrue(showDet.size() == 1)
        assertTrue(showDet[0][1].contains("\"DETERMINISTIC\"=\"true\""))

        sql """ SET enable_cte_materialize = true; """
        sql """ SET inline_cte_referenced_threshold = 1; """
        qt_materialized """
        WITH cte AS (
            SELECT id, py_uuid_agg(id) AS token
            FROM cte_uuid_seed
            GROUP BY id
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
            SELECT id, py_uuid_agg(id) AS token
            FROM cte_uuid_seed
            GROUP BY id
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
            SELECT id, py_uuid_agg_det(id) AS token
            FROM cte_uuid_seed
            GROUP BY id
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
            SELECT id, py_uuid_agg_det(id) AS token
            FROM cte_uuid_seed
            GROUP BY id
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
        CREATE TABLE mtmv_uuid_seed (id INT, v INT) ENGINE=OLAP DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        sql """ INSERT INTO mtmv_uuid_seed VALUES (1, 10), (1, 11), (2, 20), (2, 21), (3, 30), (3, 31); """
        sql """ sync; """

        try {
            sql """
            CREATE MATERIALIZED VIEW py_uuid_agg_mtmv
            BUILD DEFERRED REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
            AS
            SELECT id, py_uuid_agg(v) AS token
            FROM mtmv_uuid_seed
            GROUP BY id;
            """
            Assert.fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"))
        }
    } finally {
        try_sql(""" DROP MATERIALIZED VIEW IF EXISTS py_uuid_agg_mtmv; """)
        sql """ DROP FUNCTION IF EXISTS py_uuid_agg(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_agg_false(INT); """
        sql """ DROP FUNCTION IF EXISTS py_uuid_agg_det(INT); """
        sql """ DROP TABLE IF EXISTS cte_uuid_seed; """
        sql """ DROP TABLE IF EXISTS mtmv_uuid_seed; """
    }
}
