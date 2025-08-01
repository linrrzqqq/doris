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

suite ("testAggQueryOnAggMV1") {
    sql """set enable_nereids_planner=true;"""
    sql "set disable_nereids_rules='CONSTANT_PROPAGATION'"
    sql """ DROP TABLE IF EXISTS emps; """

    sql """
            create table emps (
                time_col date, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into emps values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into emps values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into emps values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into emps values("2020-01-03",3,"c",3,3,3);"""
    sql """insert into emps values("2020-01-03",3,"c",3,3,3);"""
    sql """insert into emps values("2020-01-03",3,"c",3,3,3);"""

sql """alter table emps modify column time_col set stats ('row_count'='9');"""

    createMV("create materialized view emps_mv as select deptno, sum(salary), max(commission) from emps group by deptno;")
    createMV("create materialized view emps_mv_count_key as select deptno, count(deptno) from emps group by deptno;")
    createMV("create materialized view emps_mv_if as select deptno, sum(if(empid = 1, empid, salary)) from emps group by deptno;")

    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""

    sql "analyze table emps with sync;"

    mv_rewrite_all_fail("select * from emps order by empid;", ["emps_mv", "emps_mv_count_key", "emps_mv_if"])
    qt_select_star "select * from emps order by empid;"


    mv_rewrite_success("select sum(salary), deptno from emps group by deptno order by deptno;", "emps_mv")
    
    qt_select_mv "select sum(salary), deptno from emps group by deptno order by deptno;"

    mv_rewrite_success("select sum(salary) as salary from emps;", "emps_mv")
    
    qt_select_mv "select sum(salary) as salary from emps;"

    mv_rewrite_success("select deptno, count(deptno) from emps group by deptno order by deptno;", "emps_mv_count_key")
    
    qt_select_mv "select deptno, count(deptno) from emps group by deptno order by deptno;"

    mv_rewrite_success("select deptno, sum(if(empid = 1, empid, salary)) from emps group by deptno;", "emps_mv_if")
    
    qt_select_mv "select deptno, sum(if(empid = 1, empid, salary)) from emps group by deptno order by deptno;"

    mv_rewrite_success("select deptno, count(deptno) from emps where deptno=1 group by deptno order by deptno;", "emps_mv_count_key")
    
    qt_select_mv "select deptno, count(deptno) from emps where deptno=1 group by deptno order by deptno;"

    mv_rewrite_all_fail("select deptno, sum(salary), max(commission) from emps where salary=1 group by deptno order by deptno;", ["emps_mv", "emps_mv_count_key", "emps_mv_if"])
        
    qt_select_mv "select deptno, sum(salary), max(commission) from emps where salary=1 group by deptno order by deptno;"

}

