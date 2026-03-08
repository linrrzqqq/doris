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

#pragma once

#include <s2/s1angle.h>
#include <s2/s2edge_crosser.h>
#include <s2/s2edge_crossings.h>
#include <s2/s2edge_distances.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include <memory>
#include <ostream>
#include <vector>

#include "geo/geo_common.h"
#include "geo/wkt_parse_type.h"

namespace doris {

// Geometry mode selector used by spatial helper functions.
enum class GeoMode {
    Geometry,
    Geography,
};

class GeoUtil {
public:
    // Comparison tolerance in degrees.
    static constexpr double TOLERANCE = 1e-6;

    // Format point as "longitude latitude".
    static void print_s2point(std::ostream& os, const S2Point& point);

    // Compute spherical angular distance (degrees) between two points.
    static double compute_distance_to_point(const S2Point& p1, const S2Point& p2);

    // Compute point-to-segment distance (degrees) under the selected mode.
    template <GeoMode mode>
    static double compute_distance_to_line(const S2Point& point, const S2Point& line_start,
                                           const S2Point& line_end);

    // Convenience overload for a 2-point polyline segment.
    template <GeoMode mode>
    static double compute_distance_to_line(const S2Point& point, const S2Polyline* line);

    // Check whether two segments intersect under the selected mode.
    template <GeoMode mode>
    static bool is_segments_intersect(const S2Point& a1, const S2Point& a2, const S2Point& b1,
                                      const S2Point& b2);

    // Check whether a point is inside a polygon under the selected mode.
    template <GeoMode mode>
    static bool is_point_in_polygon(const S2Point& point, const S2Polygon* polygon);

    // Check whether two segments touch at boundary only.
    template <GeoMode mode>
    static bool is_line_touches_line(const S2Point& l1p1, const S2Point& l1p2, const S2Point& l2p1,
                                     const S2Point& l2p2);

    // Check whether a ring is closed (first point equals last point).
    static bool is_loop_closed(const std::vector<S2Point>& points);

    // Validate longitude/latitude values.
    static bool is_valid_lng_lat(double lng, double lat);

    // Convert longitude/latitude to an S2Point.
    // Returns GEO_PARSE_OK if and only if the conversion succeeds.
    static GeoParseStatus to_s2point(double lng, double lat, S2Point* point);
    static GeoParseStatus to_s2point(const GeoCoordinate& coord, S2Point* point);

    // Remove adjacent duplicate points in-place.
    static void remove_duplicate_points(std::vector<S2Point>* points);

    // Build S2 geometry objects from coordinate lists.
    static GeoParseStatus to_s2loop(const GeoCoordinateList& coords, std::unique_ptr<S2Loop>* loop);
    static GeoParseStatus to_s2polyline(const GeoCoordinateList& coords,
                                        std::unique_ptr<S2Polyline>* polyline);
    static GeoParseStatus to_s2polygon(const GeoCoordinateListList& coords_list,
                                       std::unique_ptr<S2Polygon>* polygon);
};

} // namespace doris
