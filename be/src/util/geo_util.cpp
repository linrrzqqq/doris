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

#include "util/geo_util.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <iomanip>

namespace doris {
namespace {

double compute_distance_to_line_geometry(const S2Point& point, const S2Point& line_start,
                                         const S2Point& line_end) {
    auto p = S2LatLng(point);
    auto a = S2LatLng(line_start);
    auto b = S2LatLng(line_end);

    double px = p.lng().degrees(), py = p.lat().degrees();
    double ax = a.lng().degrees(), ay = a.lat().degrees();
    double bx = b.lng().degrees(), by = b.lat().degrees();

    double dx = bx - ax, dy = by - ay;
    double len_sq = dx * dx + dy * dy;

    if (len_sq < 1e-30) {
        double ex = px - ax, ey = py - ay;
        return std::sqrt(ex * ex + ey * ey);
    }

    double t = ((px - ax) * dx + (py - ay) * dy) / len_sq;
    t = std::max(0.0, std::min(1.0, t));

    double nx = ax + t * dx;
    double ny = ay + t * dy;

    double ex = px - nx, ey = py - ny;
    return std::sqrt(ex * ex + ey * ey);
}

bool is_point_in_polygon_geometry(const S2Point& point, const S2Polygon* polygon) {
    auto p = S2LatLng(point);
    double px = p.lng().degrees(), py = p.lat().degrees();

    int crossings = 0;
    for (int i = 0; i < polygon->num_loops(); ++i) {
        const S2Loop* loop = polygon->loop(i);
        for (int j = 0; j < loop->num_vertices(); ++j) {
            auto va = S2LatLng(loop->vertex(j));
            auto vb = S2LatLng(loop->vertex((j + 1) % loop->num_vertices()));

            double ax = va.lng().degrees(), ay = va.lat().degrees();
            double bx = vb.lng().degrees(), by = vb.lat().degrees();

            if (ay > by) {
                std::swap(ax, bx);
                std::swap(ay, by);
            }

            if (py <= ay || py > by) {
                continue;
            }

            double intersect_x;
            if (std::abs(bx - ax) < 1e-15) {
                intersect_x = ax;
            } else {
                intersect_x = ax + (bx - ax) * (py - ay) / (by - ay);
            }

            if (px < intersect_x) {
                crossings++;
            }
        }
    }

    return (crossings % 2 == 1);
}

template <GeoMode mode>
bool is_line_touches_line_impl(const S2Point& l1p1, const S2Point& l1p2, const S2Point& l2p1,
                               const S2Point& l2p2) {
    int count = 0;
    if (GeoUtil::compute_distance_to_line<mode>(l1p1, l2p1, l2p2) < GeoUtil::TOLERANCE) {
        count++;
    }
    if (GeoUtil::compute_distance_to_line<mode>(l1p2, l2p1, l2p2) < GeoUtil::TOLERANCE) {
        count++;
    }
    if (GeoUtil::compute_distance_to_line<mode>(l2p1, l1p1, l1p2) < GeoUtil::TOLERANCE) {
        count++;
    }
    if (GeoUtil::compute_distance_to_line<mode>(l2p2, l1p1, l1p2) < GeoUtil::TOLERANCE) {
        count++;
    }

    int shared_endpoint_count = static_cast<int>(l1p1 == l2p1 || l1p1 == l2p2) +
                                static_cast<int>(l1p2 == l2p1 || l1p2 == l2p2);
    return count == 1 || (count == 2 && shared_endpoint_count == 1);
}

} // namespace

void GeoUtil::print_s2point(std::ostream& os, const S2Point& point) {
    S2LatLng coord(point);
    os << std::setprecision(15) << coord.lng().degrees() << " " << coord.lat().degrees();
}

double GeoUtil::compute_distance_to_point(const S2Point& p1, const S2Point& p2) {
    return S1Angle(p1, p2).degrees();
}

template <GeoMode mode>
double GeoUtil::compute_distance_to_line(const S2Point& point, const S2Point& line_start,
                                         const S2Point& line_end) {
    if constexpr (mode == GeoMode::Geography) {
        return S2::GetDistance(point, line_start, line_end).degrees();
    }
    return compute_distance_to_line_geometry(point, line_start, line_end);
}

template <GeoMode mode>
double GeoUtil::compute_distance_to_line(const S2Point& point, const S2Polyline* line) {
    return compute_distance_to_line<mode>(point, line->vertex(0), line->vertex(1));
}

bool is_segments_intersect_geometry(const S2Point& a1, const S2Point& a2, const S2Point& b1,
                                    const S2Point& b2) {
    S2LatLng la1(a1), la2(a2), lb1(b1), lb2(b2);
    double a1x = la1.lng().degrees(), a1y = la1.lat().degrees();
    double a2x = la2.lng().degrees(), a2y = la2.lat().degrees();
    double b1x = lb1.lng().degrees(), b1y = lb1.lat().degrees();
    double b2x = lb2.lng().degrees(), b2y = lb2.lat().degrees();

    double d1 = (b2x - b1x) * (a1y - b1y) - (b2y - b1y) * (a1x - b1x);
    double d2 = (b2x - b1x) * (a2y - b1y) - (b2y - b1y) * (a2x - b1x);
    double d3 = (a2x - a1x) * (b1y - a1y) - (a2y - a1y) * (b1x - a1x);
    double d4 = (a2x - a1x) * (b2y - a1y) - (a2y - a1y) * (b2x - a1x);

    if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
        return true;
    }

    static constexpr double CP_EPS = 1e-10;
    static constexpr double BB_EPS = 1e-12;
    auto on_segment = [](double sx, double sy, double ex, double ey, double px, double py) {
        return std::min(sx, ex) - BB_EPS <= px && px <= std::max(sx, ex) + BB_EPS &&
               std::min(sy, ey) - BB_EPS <= py && py <= std::max(sy, ey) + BB_EPS;
    };

    if (std::abs(d1) < CP_EPS && on_segment(b1x, b1y, b2x, b2y, a1x, a1y)) {
        return true;
    }
    if (std::abs(d2) < CP_EPS && on_segment(b1x, b1y, b2x, b2y, a2x, a2y)) {
        return true;
    }
    if (std::abs(d3) < CP_EPS && on_segment(a1x, a1y, a2x, a2y, b1x, b1y)) {
        return true;
    }
    if (std::abs(d4) < CP_EPS && on_segment(a1x, a1y, a2x, a2y, b2x, b2y)) {
        return true;
    }

    return false;
}

bool is_segments_intersect_geography(const S2Point& a1, const S2Point& a2, const S2Point& b1,
                                     const S2Point& b2) {
    S2EdgeCrosser crosser(&a1, &a2, &b1);
    if (crosser.EdgeOrVertexCrossing(&b2)) {
        return true;
    }

    S1ChordAngle snap(S2::kIntersectionMergeRadius);
    if (S2::IsDistanceLess(a1, b1, b2, snap)) {
        return true;
    }
    if (S2::IsDistanceLess(a2, b1, b2, snap)) {
        return true;
    }
    if (S2::IsDistanceLess(b1, a1, a2, snap)) {
        return true;
    }
    if (S2::IsDistanceLess(b2, a1, a2, snap)) {
        return true;
    }

    return false;
}

template <GeoMode mode>
bool GeoUtil::is_segments_intersect(const S2Point& a1, const S2Point& a2, const S2Point& b1,
                                    const S2Point& b2) {
    if (a1 == b1 || a1 == b2 || a2 == b1 || a2 == b2) {
        return true;
    }
    if constexpr (mode == GeoMode::Geography) {
        return is_segments_intersect_geography(a1, a2, b1, b2);
    }
    return is_segments_intersect_geometry(a1, a2, b1, b2);
}

template <GeoMode mode>
bool GeoUtil::is_point_in_polygon(const S2Point& point, const S2Polygon* polygon) {
    if constexpr (mode == GeoMode::Geography) {
        return polygon->Contains(point);
    }
    return is_point_in_polygon_geometry(point, polygon);
}

template <GeoMode mode>
bool GeoUtil::is_line_touches_line(const S2Point& l1p1, const S2Point& l1p2, const S2Point& l2p1,
                                   const S2Point& l2p2) {
    return is_line_touches_line_impl<mode>(l1p1, l1p2, l2p1, l2p2);
}

bool GeoUtil::is_loop_closed(const std::vector<S2Point>& points) {
    if (points.empty()) {
        return false;
    }
    return points.front() == points.back();
}

bool GeoUtil::is_valid_lng_lat(double lng, double lat) {
    return std::abs(lng) <= 180 && std::abs(lat) <= 90;
}

GeoParseStatus GeoUtil::to_s2point(double lng, double lat, S2Point* point) {
    if (!is_valid_lng_lat(lng, lat)) {
        return GEO_PARSE_COORD_INVALID;
    }
    S2LatLng ll = S2LatLng::FromDegrees(lat, lng);
    DCHECK(ll.is_valid()) << "invalid point, lng=" << lng << ", lat=" << lat;
    *point = ll.ToPoint();
    return GEO_PARSE_OK;
}

GeoParseStatus GeoUtil::to_s2point(const GeoCoordinate& coord, S2Point* point) {
    return to_s2point(coord.x, coord.y, point);
}

void GeoUtil::remove_duplicate_points(std::vector<S2Point>* points) {
    int lhs = 0;
    int rhs = 1;
    for (; rhs < (int)points->size(); ++rhs) {
        if ((*points)[rhs] != (*points)[lhs]) {
            lhs++;
            if (lhs != rhs) {
                (*points)[lhs] = (*points)[rhs];
            }
        }
    }
    points->resize(lhs + 1);
}

GeoParseStatus GeoUtil::to_s2loop(const GeoCoordinateList& coords, std::unique_ptr<S2Loop>* loop) {
    // 1. convert all coordinates to points
    std::vector<S2Point> points(coords.list.size());
    for (int i = 0; i < (int)coords.list.size(); ++i) {
        auto res = to_s2point(coords.list[i], &points[i]);
        if (res != GEO_PARSE_OK) {
            return res;
        }
    }
    // 2. check if it is a closed loop
    if (!is_loop_closed(points)) {
        return GEO_PARSE_LOOP_NOT_CLOSED;
    }
    // 3. remove duplicate points
    remove_duplicate_points(&points);
    // 4. remove last point
    points.resize(points.size() - 1);
    // 5. check if there is enough point
    if (points.size() < 3) {
        return GEO_PARSE_LOOP_LACK_VERTICES;
    }
    loop->reset(new S2Loop(points));
    if (!(*loop)->IsValid()) {
        return GEO_PARSE_LOOP_INVALID;
    }
    (*loop)->Normalize();
    return GEO_PARSE_OK;
}

GeoParseStatus GeoUtil::to_s2polyline(const GeoCoordinateList& coords,
                                      std::unique_ptr<S2Polyline>* polyline) {
    // 1. convert all coordinates to points
    std::vector<S2Point> points(coords.list.size());
    for (int i = 0; i < (int)coords.list.size(); ++i) {
        auto res = to_s2point(coords.list[i], &points[i]);
        if (res != GEO_PARSE_OK) {
            return res;
        }
    }
    // 2. remove duplicate points
    remove_duplicate_points(&points);
    // 3. check if there is enough point
    if (points.size() < 2) {
        return GEO_PARSE_POLYLINE_LACK_VERTICES;
    }
    polyline->reset(new S2Polyline(points));
    if (!(*polyline)->IsValid()) {
        return GEO_PARSE_POLYLINE_INVALID;
    }
    return GEO_PARSE_OK;
}

GeoParseStatus GeoUtil::to_s2polygon(const GeoCoordinateListList& coords_list,
                                     std::unique_ptr<S2Polygon>* polygon) {
    std::vector<std::unique_ptr<S2Loop>> loops(coords_list.list.size());
    for (int i = 0; i < (int)coords_list.list.size(); ++i) {
        auto res = to_s2loop(*coords_list.list[i], &loops[i]);
        if (res != GEO_PARSE_OK) {
            return res;
        }
        if (i != 0 && !(loops[0]->Contains(*loops[i]))) {
            return GEO_PARSE_POLYGON_NOT_HOLE;
        }
    }
    polygon->reset(new S2Polygon(std::move(loops)));
    return GEO_PARSE_OK;
}

template double GeoUtil::compute_distance_to_line<GeoMode::Geometry>(const S2Point&, const S2Point&,
                                                                     const S2Point&);
template double GeoUtil::compute_distance_to_line<GeoMode::Geography>(const S2Point&,
                                                                      const S2Point&,
                                                                      const S2Point&);

template double GeoUtil::compute_distance_to_line<GeoMode::Geometry>(const S2Point&,
                                                                     const S2Polyline*);
template double GeoUtil::compute_distance_to_line<GeoMode::Geography>(const S2Point&,
                                                                      const S2Polyline*);

template bool GeoUtil::is_segments_intersect<GeoMode::Geometry>(const S2Point&, const S2Point&,
                                                                const S2Point&, const S2Point&);
template bool GeoUtil::is_segments_intersect<GeoMode::Geography>(const S2Point&, const S2Point&,
                                                                 const S2Point&, const S2Point&);

template bool GeoUtil::is_point_in_polygon<GeoMode::Geometry>(const S2Point&, const S2Polygon*);
template bool GeoUtil::is_point_in_polygon<GeoMode::Geography>(const S2Point&, const S2Polygon*);

template bool GeoUtil::is_line_touches_line<GeoMode::Geometry>(const S2Point&, const S2Point&,
                                                               const S2Point&, const S2Point&);
template bool GeoUtil::is_line_touches_line<GeoMode::Geography>(const S2Point&, const S2Point&,
                                                                const S2Point&, const S2Point&);

} // namespace doris
