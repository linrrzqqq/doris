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
suite("test_gis_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set batch_size = 4096;"

    qt_astext_1 "SELECT ST_AsText(ST_Point(24.7, 56.7));"
    qt_aswkt_1 "SELECT ST_AsWKT(ST_Point(24.7, 56.7));"

    qt_astext_2 "SELECT ST_AsText(ST_Circle(111, 64, 10000));"

    qt_contains_1 "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"
    qt_contains_2 "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(50, 50));"
    qt_contains_3 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POINT(2 10)'));"

    qt_contains_4 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(2 0, 8 0)'));"
    qt_contains_5 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(2 5, 8 5)'));"
    qt_contains_6 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(0 0, 10 0)'));"
    qt_contains_7 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(10 0, 0 0)'));"
    qt_contains_8 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(5 0, 5 5)'));"
    qt_contains_9 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(0 0, 10 10)'));"
    qt_contains_10 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(-0.000001 0, 10 10)'));"

    qt_contains_11 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'));"
    qt_contains_12 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))'));"
    qt_contains_13 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 10.000001, 10.000001 10.000001, 10.000001 0, 0 0))'));"
    qt_contains_14 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))'));"
    qt_contains_15 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((4 4, 7 4, 7 7, 4 7, 4 4))'));"
    qt_contains_16 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))'));"
    qt_contains_17 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((2.999999 2.999999, 8.000001 2.999999, 8.000001 8.000001, 2.999999 8.000001, 2.999999 2.999999), (3 3, 8 3, 8 8, 3 8, 3 3))'));"
    qt_contains_18 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((1 1, 9 1, 9 9, 1 9, 1 1), (3.000001 3.000001, 7.999999 3.000001, 7.999999 7.999999, 3.000001 7.999999, 3.000001 3.000001))'));"
    
    qt_contains_19 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('MULTIPOLYGON(((2 2, 4 2, 4 4, 2 4, 2 2)), ((6 6, 8 6, 8 8, 6 8, 6 6)))'));"
    qt_contains_20 "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('MULTIPOLYGON(((2 2, 2 8, 8 8, 8 2, 2 2)), ((10 10, 10 15, 15 15, 15 10, 10 10)))'));"

    qt_contains_21 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(5 5)'));"
    qt_contains_22 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(17 7)'));"
    qt_contains_23 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(12 7)'));"
    qt_contains_24 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(0 5)'));"
    qt_contains_25 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(10 10)'));"
    qt_contains_26 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POINT(5 5)'));"
    qt_contains_27 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POINT(2 5)'));"
    qt_contains_28 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(0.000001 0.000001)'));"
    qt_contains_29 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(-0.000001 0)'));"

    qt_contains_30 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(2 2, 8 8)'));"
    qt_contains_31 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(16 6, 19 9)'));"
    qt_contains_32 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(5 5, 16 6)'));"
    qt_contains_33 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0 0, 10 10)'));"
    qt_contains_34 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(5 0, 5 10)'));"
    qt_contains_35 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('LINESTRING(3 3, 7 7)'));"
    qt_contains_36 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('LINESTRING(1 1, 9 9)'));"
    qt_contains_37 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0.000001 0.000001, 9.999999 9.999999)'));"
    qt_contains_38 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0.000001 0.000001, 10.000001 10.000001)'));"

    qt_contains_39 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((1 1, 1 9, 9 9, 9 1, 1 1))'));"
    qt_contains_40 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((16 6, 16 9, 19 9, 19 6, 16 6))'));"
    qt_contains_41 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))'));"
    qt_contains_42 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'));"
    qt_contains_43 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((-5 -5, -5 15, 15 15, 15 -5, -5 -5))'));"
    qt_contains_44 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))'));"
    qt_contains_45 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POLYGON((1 1, 1 9, 9 9, 9 1, 1 1))'));"
    qt_contains_46 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((0.000001 0.000001, 0.000001 9.999999, 9.999999 9.999999, 9.999999 0.000001, 0.000001 0.000001))'));"
    qt_contains_47 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((-0.000001 -0.000001, -0.000001 10.000001, 10.000001 10.000001, 10.000001 -0.000001, -0.000001 -0.000001))'));"

    qt_contains_48 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 5, 5 5, 5 1, 1 1)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_contains_49 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 5, 5 5, 5 1, 1 1)), ((12 6, 12 9, 14 9, 14 6, 12 6)))'));"
    qt_contains_50 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'));"
    qt_contains_51 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 9, 9 9, 9 1, 1 1)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_contains_52 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((3 3, 3 7, 7 7, 7 3, 3 3)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_contains_53 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((25 5, 25 10, 30 10, 30 5, 25 5)))'));"
    qt_contains_54 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0.000001 0.000001, 0.000001 9.999999, 9.999999 9.999999, 9.999999 0.000001, 0.000001 0.000001)), ((15.000001 5.000001, 15.000001 9.999999, 19.999999 9.999999, 19.999999 5.000001, 15.000001 5.000001)))'));"
    qt_contains_55 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((-0.000001 -0.000001, -0.000001 10.000001, 10.000001 10.000001, 10.000001 -0.000001, -0.000001 -0.000001)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'));"
    qt_contains_56 "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 9, 9 9, 9 1, 1 1), (4 4, 4 6, 6 6, 6 4, 4 4)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"

    qt_intersects_1 "SELECT ST_Intersects(ST_Point(0, 0), ST_Point(0, 0));"
    qt_intersects_2 "SELECT ST_Intersects(ST_Point(0, 0), ST_Point(5, 5));"

    qt_intersects_3 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));"
    qt_intersects_4 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(1.99, 0));"
    qt_intersects_5 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2.01, 0));"
    qt_intersects_6 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_intersects_7 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_intersects_8 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_intersects_9 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2.1 0, 3 0)\"));"
    qt_intersects_10 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1.99 0, 3 0)\"));"
    qt_intersects_11 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (-2 0.01, 3 0.01)\"));"
    qt_intersects_12 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_intersects_13 "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_intersects_14 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_intersects_15 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_intersects_16 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_intersects_17 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_intersects_18 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_intersects_19 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"
    qt_intersects_20 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (0 0.01, 10 0.01)\"));"
    qt_intersects_21 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (0 -0.01, 10 -0.01)\"));"

    qt_intersects_22 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_intersects_23 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_intersects_24 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))\"));"
    qt_intersects_25 "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))\"));"

    qt_intersects_26 "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(0, 0));"
    qt_intersects_27 "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(8, 8));"
    qt_intersects_28 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.000001, 0));"
    qt_intersects_29 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 3)'));"
    qt_intersects_30 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 5)'));"

    qt_intersects_31 "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_LineFromText(\"LINESTRING (4 4, 7 7)\"));"
    qt_intersects_32 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_intersects_33 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_intersects_34 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 3, 4 7)'));"
    qt_intersects_35 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_intersects_36 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_intersects_37 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((3 3, 3 8, 8 8, 8 3, 3 3))'));"
    qt_intersects_38 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 5.999999 0, 5.999999 9.999999, 5.000001 9.999999, 5.000001 0))'));"
    qt_intersects_39 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_intersects_40 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_intersects_41 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 3, 7 3, 7 8, 4 8, 4 3))'));"

    qt_intersects_42 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_intersects_43 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((2 2, 3 2, 3 3, 2 3, 2 2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_intersects_44 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((-2 -2, -3 -2, -3 -3, -2 -3, -2 -2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_intersects_45 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0)), ((0 5.000001, 5 5.000001, 5 6, 5.999999 6, 5.999999 10, 0 10, 0 5.000001)))'));"

    qt_intersects_46 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 1));"
    qt_intersects_47 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 0.999999));"
    qt_intersects_48 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(10, 10, 1));"
    qt_intersects_49 "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(5, 6, 0.999999));"

    qt_intersects_50 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(2, 1));"
    qt_intersects_51 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_intersects_52 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(3, 1));"

    qt_intersects_53 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_intersects_54 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_intersects_55 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_intersects_56 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_intersects_57 "SELECT ST_Intersects(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_intersects_58 "SELECT ST_Intersects(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_intersects_59 "SELECT ST_Intersects(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_intersects_60 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));"
    qt_intersects_61 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_intersects_62 "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_touches_1 "SELECT ST_Touches(ST_Point(0, 0), ST_Point(0, 0));"
    qt_touches_2 "SELECT ST_Touches(ST_Point(0, 0), ST_Point(5, 5));"

    qt_touches_3 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));"
    qt_touches_4 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_touches_5 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_touches_6 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_touches_7 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_touches_8 "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_touches_9 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_touches_10 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_touches_11 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_touches_12 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_touches_13 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_touches_14 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"
    qt_touches_15 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-3 5, 8 5)\"));"
    qt_touches_16 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-3 5, 15 5)\"));"

    qt_touches_17 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_touches_18 "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_touches_19 "SELECT ST_Touches(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Polygon('POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))'));"
    qt_touches_20 "SELECT ST_Touches(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Polygon('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'));"
    qt_touches_21 "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('POLYGON((2 10, 5 15, 8 10, 10 15, 5 20, 0 15, 2 10))'));"

    qt_touches_22 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(0, 3));"
    qt_touches_23 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5, 5));"
    qt_touches_24 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5, 6));"
    qt_touches_25 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(3, 3));"
    qt_touches_26 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.00001, 0));"

    qt_touches_27 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5 5, 7 7)'));"
    qt_touches_28 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5 5, 5 10)'));"
    qt_touches_29 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (0 3, 5 3)'));"
    qt_touches_30 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5.5 0, 5.5 10)'));"
    qt_touches_31 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_touches_32 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_touches_33 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 4, 4 7)'));"
    qt_touches_34 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_touches_35 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5 5, 5 8, 8 8, 8 5, 5 5))'));"
    qt_touches_36 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))'), ST_GeometryFromText('POLYGON((5 5, 5 8, 8 8, 8 5, 5 5))'));"
    qt_touches_37 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_touches_38 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'));"
    qt_touches_39 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((0 0, 0 -4, -4 -4, -4 0, 0 0))'));"
    qt_touches_40 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0))'));"
    qt_touches_41 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_touches_42 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_touches_43 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3 4, 8 4, 8 7, 3 7, 3 4))'));"
    
    qt_touches_44 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((15 0, 25 0, 25 10, 15 10, 15 0)), ((30 30, 35 35, 40 30, 30 30)))'),ST_GeometryFromText('MULTIPOLYGON (((10 0, 20 0, 20 10, 10 10, 10 0)))'));"
    qt_touches_45 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((3 4, 8 4, 8 7, 3 7, 3 4)), ((-8 -8, -8 -12, -12 -12, -12 -8, -8 -8)))'));"
    qt_touches_46 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((10.00001 0, 14.99999 0, 14.99999 10, 10.00001 10, 10.00001 0)), ((-8 -8, -8 -12, -12 -12, -12 -8, -8 -8)))'));"
    qt_touches_47 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((3 3, 8 3, 8 8, 3 8, 3 3)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"

    qt_touches_48 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(10, 15, 5));"
    qt_touches_49 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(10, 15, 5.00001));"
    qt_touches_50 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(3, 5, 2));"
    qt_touches_51 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(4.5, 5, 1.5));"
    qt_touches_52 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(20, 20, 1));"
    qt_touches_53 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(5.5, 5.5, 0.5));"
    qt_touches_54 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(1, 1, 0.5));"
    qt_touches_55 "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(8, 8, 2));"

    qt_touches_56 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(2, 1));"
    qt_touches_57 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(1, 2));"
    qt_touches_58 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_touches_59 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(3, 1));"

    qt_touches_60 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_touches_61 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_touches_62 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_touches_63 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_touches_64 "SELECT ST_Touches(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_touches_65 "SELECT ST_Touches(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_touches_66 "SELECT ST_Touches(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_touches_67 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));"
    qt_touches_68 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_touches_69 "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_disjoint_1 "SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(0, 0));"
    qt_disjoint_2 "SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(5, 5));"

    qt_disjoint_3 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));"
    qt_disjoint_4 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_disjoint_5 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_disjoint_6 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_disjoint_7 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_disjoint_8 "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_disjoint_9 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_disjoint_10 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_disjoint_11 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_disjoint_12 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_disjoint_13 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_disjoint_14 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"

    qt_disjoint_15 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_disjoint_16 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_disjoint_17 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))\"));"
    qt_disjoint_18 "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))\"));"

    qt_disjoint_19 "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(0, 0));"
    qt_disjoint_20 "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(8, 8));"
    qt_disjoint_21 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.000001, 0));"
    qt_disjoint_22 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 3)'));"
    qt_disjoint_23 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 5)'));"

    qt_disjoint_24 "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_LineFromText(\"LINESTRING (4 4, 7 7)\"));"
    qt_disjoint_25 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_disjoint_26 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_disjoint_27 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 3, 4 7)'));"
    qt_disjoint_28 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_disjoint_29 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_disjoint_30 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((3 3, 3 8, 8 8, 8 3, 3 3))'));"
    qt_disjoint_31 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 5.999999 0, 5.999999 9.999999, 5.000001 9.999999, 5.000001 0))'));"
    qt_disjoint_32 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_disjoint_33 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_disjoint_34 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 3, 7 3, 7 8, 4 8, 4 3))'));"

    qt_disjoint_35 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_disjoint_36 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((2 2, 3 2, 3 3, 2 3, 2 2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_disjoint_37 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((-2 -2, -3 -2, -3 -3, -2 -3, -2 -2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_disjoint_38 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0)), ((0 5.000001, 5 5.000001, 5 6, 5.999999 6, 5.999999 10, 0 10, 0 5.000001)))'));"

    qt_disjoint_39 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 1));"
    qt_disjoint_40 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 0.999999));"
    qt_disjoint_41 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(10, 10, 1));"
    qt_disjoint_42 "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(5, 6, 0.999999));"

    qt_disjoint_43 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(2, 1));"
    qt_disjoint_44 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_disjoint_45 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(3, 1));"

    qt_disjoint_46 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_disjoint_47 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_disjoint_48 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_disjoint_49 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_disjoint_50 "SELECT ST_Disjoint(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_disjoint_51 "SELECT ST_Disjoint(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_disjoint_52 "SELECT ST_Disjoint(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_disjoint_53 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));"
    qt_disjoint_54 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_disjoint_55 "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_distance_sphere_1 "SELECT ST_DISTANCE_SPHERE(116.35620117, 39.939093, 116.4274406433, 39.9020987219);"
    qt_angle_sphere_1 "SELECT ST_ANGLE_SPHERE(116.35620117, 39.939093, 116.4274406433, 39.9020987219);"
    qt_angle_sphere_2 "SELECT ST_ANGLE_SPHERE(0, 0, 45, 0);"

    // ST_Length tests for all geometry types
    qt_length_1 "SELECT ST_Length(ST_Point(0, 0));"
    qt_length_2 "SELECT ST_Length(ST_LineFromText(\"LINESTRING (0 0, 1 0, 1 1, 0 1)\"));"
    qt_length_3 "SELECT ST_Length(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_length_4 "SELECT ST_Length(ST_Circle(0, 0, 1));"
    qt_length_5 "SELECT ST_Length(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_length_6 "SELECT ST_Length(ST_GeometryFromText(\"LINESTRING (0 0, 1 0, 2 0, 3 0)\"));"
    qt_length_7 "SELECT ST_Length(ST_Point(24.7, 56.7));"
    qt_length_8 "SELECT ST_Length(ST_LineFromText(\"LINESTRING (0 0, 3 4)\"));"
    qt_length_9 "SELECT ST_Length(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"));"
    qt_length_10 "SELECT ST_Length(ST_Circle(10, 20, 5));"
    qt_length_11 "SELECT ST_Length(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))\"));"

    // ST_GeometryType tests for all geometry types
    qt_geometrytype_1 "SELECT ST_GeometryType(ST_Point(0, 0));"
    qt_geometrytype_2 "SELECT ST_GeometryType(ST_LineFromText(\"LINESTRING (0 0, 1 0, 1 1, 0 1)\"));"
    qt_geometrytype_3 "SELECT ST_GeometryType(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_geometrytype_4 "SELECT ST_GeometryType(ST_Circle(0, 0, 1));"
    qt_geometrytype_5 "SELECT ST_GeometryType(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_geometrytype_6 "SELECT ST_GeometryType(ST_GeometryFromText(\"LINESTRING (0 0, 1 0, 2 0, 3 0)\"));"

    // ST_Distance tests for all geometry type combinations
    qt_distance_1 "SELECT ST_Distance(ST_Point(0, 0), ST_Point(0, 0));"
    qt_distance_2 "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4));"
    qt_distance_3 "SELECT ST_Distance(ST_Point(3, 4), ST_Point(0, 0));"
    
    qt_distance_4 "SELECT ST_Distance(ST_Point(0, 0), ST_LineFromText(\"LINESTRING (0 0, 10 0)\"));"
    qt_distance_5 "SELECT ST_Distance(ST_Point(5, 5), ST_LineFromText(\"LINESTRING (0 0, 10 0)\"));"
    qt_distance_6 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_Point(0, 0));"
    qt_distance_7 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_Point(5, 5));"
    
    qt_distance_8 "SELECT ST_Distance(ST_Point(5, 5), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_9 "SELECT ST_Distance(ST_Point(15, 15), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_10 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"
    qt_distance_11 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(15, 15));"
    
    qt_distance_12 "SELECT ST_Distance(ST_Point(0, 0), ST_Circle(0, 0, 1));"
    qt_distance_13 "SELECT ST_Distance(ST_Point(2, 0), ST_Circle(0, 0, 1));"
    qt_distance_14 "SELECT ST_Distance(ST_Circle(0, 0, 1), ST_Point(0, 0));"
    qt_distance_15 "SELECT ST_Distance(ST_Circle(0, 0, 1), ST_Point(2, 0));"
    
    qt_distance_16 "SELECT ST_Distance(ST_Point(2, 2), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_distance_17 "SELECT ST_Distance(ST_Point(12, 12), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_distance_18 "SELECT ST_Distance(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(2, 2));"
    
    qt_distance_19 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_LineFromText(\"LINESTRING (0 0, 10 0)\"));"
    qt_distance_20 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 0, 5 0)\"), ST_LineFromText(\"LINESTRING (10 0, 15 0)\"));"
    qt_distance_21 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (10 0, 15 0)\"), ST_LineFromText(\"LINESTRING (0 0, 5 0)\"));"
    
    qt_distance_22 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (5 5, 5 15)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_23 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 5, 10 5)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_24 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (5 5, 5 15)\"));"
    
    qt_distance_25 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_Circle(5, 5, 1));"
    qt_distance_26 "SELECT ST_Distance(ST_Circle(5, 5, 1), ST_LineFromText(\"LINESTRING (0 0, 10 0)\"));"

    // circle-line tangent cases (distance should be 0)
    qt_distance_27 "SELECT ST_Distance(ST_Circle(0, 0, 1), ST_LineFromText(\"LINESTRING (-1 0.00000899320363724538, 1 0.00000899320363724538)\"));"
    
    qt_distance_28 "SELECT ST_Distance(ST_LineFromText(\"LINESTRING (12 2, 12 8)\"), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    
    qt_distance_29 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_30 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((15 0, 25 0, 25 10, 15 10, 15 0))\"));"
    
    qt_distance_31 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Circle(5, 5, 1));"
    qt_distance_32 "SELECT ST_Distance(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_distance_33 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Circle(20, 5, 1));"
    qt_distance_34 "SELECT ST_Distance(ST_Circle(0.001, 0, 50), ST_GeometryFromText(\"POLYGON ((-0.00045 -0.00045, 0.00045 -0.00045, 0.00045 0.00045, -0.00045 0.00045, -0.00045 -0.00045))\"));"
    
    qt_distance_35 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_distance_36 "SELECT ST_Distance(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_GeometryFromText(\"MULTIPOLYGON(((15 15, 15 20, 20 20, 20 15, 15 15)))\"));"
    
    qt_distance_37 "SELECT ST_Distance(ST_Circle(0, 0, 1), ST_Circle(0, 0, 1));"
    qt_distance_38 "SELECT ST_Distance(ST_Circle(0, 0, 1), ST_Circle(5, 0, 1));"
    qt_distance_39 "SELECT ST_Distance(ST_Circle(5, 0, 1), ST_Circle(0, 0, 1));"
    
    qt_distance_40 "SELECT ST_Distance(ST_Circle(2, 2, 1), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_distance_41 "SELECT ST_Distance(ST_Circle(20, 20, 1), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    
    qt_distance_42 "SELECT ST_Distance(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"));"
    qt_distance_43 "SELECT ST_Distance(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))\"), ST_GeometryFromText(\"MULTIPOLYGON(((15 15, 15 20, 20 20, 20 15, 15 15)))\"));"

    qt_astext_3 "SELECT ST_AsText(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_astext_4 "SELECT ST_AsText(ST_GeomFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_astext_5 "SELECT ST_AsText(ST_LineFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_astext_6 "SELECT ST_AsText(ST_LineStringFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_astext_7 "SELECT ST_AsText(ST_Point(24.7, 56.7));"

    qt_astext_8 "SELECT ST_AsText(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_astext_9 "SELECT ST_AsText(ST_PolyFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_astext_10 "SELECT ST_AsText(ST_PolygonFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"

    qt_astext_11 "SELECT ST_AsText(ST_GeometryFromText(\"MULTIPOLYGON (((0.0 0.0, 0.0 4.9, 5.3 5.3, 5.2 0.0, 0.0 0.0)), ((20.1 20.3, 20.4 30.6, 30.6 30.4, 30.0 20.2, 20.1 20.3)), ((50.2 50.3, 50.1 60.7, 60.7 60.9, 60.5 50.5, 50.2 50.3)), ((70.3 70.1, 70.5 80.5, 80.3 80.2, 80.0 70.0, 70.3 70.1)))\"));"
    qt_astext_12 "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_astext_13 "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 6, 10 6, 10 10, 8 10, 8 6)))'));"
    qt_astext_14 "SELECT ST_AsText(ST_GeometryFromText(\"MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0)), ((5 5, 5 15, 15 15, 15 5, 5 5)))\"));"
    qt_astext_15 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON (((-10 -10, -10 10, 10 10, 10 -10, -10 -10)), ((20 20, 20 30, 30 30, 30 20, 20 20), (25 25, 25 27, 27 27, 27 25, 25 25)))'));"
    qt_astext_16 "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((2 10, 5 15, 8 10, 10 15, 5 20, 0 15, 2 10)))'));"
    qt_astext_17 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5)), ((5 10, 10 5, 15 10, 10 15, 5 10)))'));"
    qt_astext_18 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3)), ((3 3, 3 7, 7 7, 7 3, 3 3)))'));"
    qt_astext_19 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 4 1, 2 4, 0 0)), ((2 4, 5 6, 3 8, 2 4)))'));"
    qt_astext_20 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-8 0, 1e-8 1e-8, 0 1e-8, 0 0)))'));"
    qt_astext_21 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-6 0, 1e-6 1e-6, 0 1e-6, 0 0)), ((1e-5 0, 1 0, 1 1e-5, 1e-5 1e-5, 1e-5 0)))'));"
    qt_astext_22 "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-6 0, 1e-6 1e-6, 0 1e-6, 0 0), (1e-8 1e-8, 1e-7 1e-8, 1e-7 1e-7, 1e-8 1e-7, 1e-8 1e-8)))'));"

    qt_x_1 "SELECT ST_X(ST_Point(1, 2));"
    qt_x_2 "SELECT ST_X(ST_Point(24.7, 56.7));"
    qt_y_1 "SELECT ST_Y(ST_Point(2, 1));"
    qt_y_2 "SELECT ST_Y(ST_Point(24.7, 56.7));"

    qt_area_square_meters_1 "SELECT ST_Area_Square_Meters(ST_Circle(0, 0, 1));"
    qt_area_square_km_1 "SELECT ST_Area_Square_Km(ST_Circle(0, 0, 1));"
    qt_area_square_meters_2 "SELECT ST_Area_Square_Meters(ST_Polygon(\"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))\"));"
    qt_area_square_km_2 "SELECT ST_Area_Square_Km(ST_Polygon(\"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))\"));"

    qt_angle_1 "SELECT ST_Angle(ST_Point(1, 0),ST_Point(0, 0),ST_Point(0, 1));"
    qt_angle_2 "SELECT ST_Angle(ST_Point(0, 0),ST_Point(1, 0),ST_Point(0, 1));"
    qt_angle_3 "SELECT ST_Angle(ST_Point(1, 0),ST_Point(0, 0),ST_Point(1, 0));"
    qt_angle_4 "SELECT ST_Angle(ST_Point(1, 0),ST_Point(-30, 0),ST_Point(150, 0));"

    qt_azimuth_1 "SELECT St_Azimuth(ST_Point(1, 0),ST_Point(0, 0));"
    qt_azimuth_2 "SELECT St_Azimuth(ST_Point(0, 0),ST_Point(1, 0));"
    qt_azimuth_3 "SELECT St_Azimuth(ST_Point(0, 0),ST_Point(0, 1));"
    qt_azimuth_4 "SELECT St_Azimuth(ST_Point(-30, 0),ST_Point(150, 0));"

    qt_astext_23 "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_Point(24.7, 56.7))));"
    qt_astext_24 "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"))));"
    qt_astext_25 "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_Polygon(\"POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))\"))));"

    qt_astext_26 "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_Point(24.7, 56.7))));"
    qt_astext_27 "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"))));"
    qt_astext_28 "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_Polygon(\"POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))\"))));"

    // table-driven tests for ST_Length/ST_GeometryType/ST_Distance
    sql "drop table if exists test_gis_table_cases"
    sql """
    CREATE TABLE test_gis_table_cases (
        `id` int NOT NULL,
        `wkt_a` varchar(512) NULL,
        `wkt_b` varchar(512) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    insert into test_gis_table_cases values
        (1, 'POINT(0 0)', 'POINT(0 0)'),
        (2, 'LINESTRING(0 0, 0 1)', 'POINT(0 0)'),
        (3, 'LINESTRING(0 0, 1 0)', 'LINESTRING(0 0, 1 0)'),
        (4, 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 'LINESTRING(0.2 0.2, 0.8 0.8)'),
        (5, 'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))', 'POINT(10 10)'),
        (6, 'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))', 'POINT(2.5 2.5)'),
        (7, null, null);
    """
    qt_geometrytype_7 "select id, ST_GeometryType(ST_GeomFromText(wkt_a)) from test_gis_table_cases order by id;"
    qt_length_12 "select id, ST_Length(ST_GeomFromText(wkt_a)) from test_gis_table_cases order by id;"
    qt_distance_44 "select id, ST_Distance(ST_GeomFromText(wkt_a), ST_GeomFromText(wkt_b)) from test_gis_table_cases order by id;"

    // test const
    sql "drop table if exists test_gis_const"
    sql """
    CREATE TABLE test_gis_const (
        `userid` varchar(32) NOT NULL COMMENT '个人账号id',
        `c_1` double NOT NULL COMMENT '发送时间',
        `c_2` double NOT NULL COMMENT '信源类型'
    ) ENGINE=OLAP
    UNIQUE KEY(`userid`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`userid`) BUCKETS 6
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "insert into test_gis_const values ('xxxx',78.73,31.5);"
    qt_sql_part_const_dis_sph "select st_distance_sphere(78.73,31.53,c_1,c_2) from test_gis_const; "
    qt_sql_part_const_ang_sph "select st_angle_sphere(78.73,31.53,c_1,c_2) from test_gis_const; "
}
