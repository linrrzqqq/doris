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

suite("test_nullable_join_key_alias") {
    sql "set disable_join_reorder = true"

    order_qt_nullif_inner """
        SELECT l.number, r.number
        FROM numbers('number' = '2') l
        INNER JOIN [broadcast] numbers('number' = '2') r
          ON nullif(l.number, 1) <=> nullif(r.number, 1)
        ORDER BY 1, 2
    """

    order_qt_if_inner """
        SELECT l.number, r.number
        FROM numbers('number' = '2') l
        INNER JOIN [broadcast] numbers('number' = '2') r
          ON if(l.number = 1, NULL, l.number) <=> if(r.number = 1, NULL, r.number)
        ORDER BY 1, 2
    """

    order_qt_nullif_left_equal """
        SELECT l.number, r.number
        FROM numbers('number' = '2') l
        LEFT JOIN [broadcast] numbers('number' = '2') r
          ON nullif(l.number, 1) = nullif(r.number, 1)
        ORDER BY 1, 2
    """

    order_qt_nullif_left_anti """
        SELECT l.number
        FROM numbers('number' = '2') l
        LEFT ANTI JOIN [broadcast] numbers('number' = '2') r
          ON nullif(l.number, 1) = nullif(r.number, 1)
        ORDER BY 1
    """

    order_qt_multiple_keys """
        SELECT l.number, r.number
        FROM numbers('number' = '4') l
        INNER JOIN [broadcast] numbers('number' = '4') r
          ON nullif(l.number, 1) <=> nullif(r.number, 1)
         AND cast(l.number DIV 2 AS INT) = cast(r.number DIV 2 AS INT)
        ORDER BY 1, 2
    """

    order_qt_multiple_null_safe_keys """
        SELECT l.number, r.number
        FROM numbers('number' = '4') l
        INNER JOIN [broadcast] numbers('number' = '4') r
          ON nullif(l.number, 1) <=> nullif(r.number, 1)
         AND nullif(cast(l.number DIV 2 AS INT), 1)
             <=> nullif(cast(r.number DIV 2 AS INT), 1)
        ORDER BY 1, 2
    """

    order_qt_null_chain """
        SELECT l.number, r.number
        FROM numbers('number' = '8') l
        INNER JOIN [broadcast] numbers('number' = '8') r
          ON if(l.number % 2 = 1, NULL, l.number)
             <=> if(r.number % 2 = 1, NULL, r.number)
        ORDER BY 1, 2
    """

    order_qt_null_chain_other_conjunct """
        SELECT l.number, r.number
        FROM numbers('number' = '8') l
        INNER JOIN [broadcast] numbers('number' = '8') r
          ON if(l.number % 2 = 1, NULL, l.number)
             <=> if(r.number % 2 = 1, NULL, r.number)
         AND l.number < r.number
        ORDER BY 1, 2
    """

    order_qt_null_aware_other_conjunct """
        SELECT l.number
        FROM numbers('number' = '4') l
        WHERE nullif(l.number, 1) NOT IN (
            SELECT nullif(r.number, 1)
            FROM numbers('number' = '4') r
            WHERE r.number > l.number
        )
        ORDER BY 1
    """
}
