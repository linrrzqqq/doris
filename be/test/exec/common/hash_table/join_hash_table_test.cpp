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

#include "exec/common/hash_table/join_hash_table.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <functional>
#include <tuple>
#include <utility>
#include <vector>

namespace doris {

template <TJoinOp::type JoinOpType>
void check_null_bucket_matches(JoinNullBucketMode mode, bool with_other_conjuncts,
                               const std::vector<std::pair<uint32_t, uint32_t>>& expected) {
    JoinHashTable<uint32_t, std::hash<uint32_t>, false> table;
    table.prepare_build<JoinOpType>(4, 1, true, 0);
    const auto bucket_size = table.get_bucket_size();
    const std::array<uint32_t, 4> build_keys {0, 7, 9, 0};
    const std::array<uint32_t, 4> build_buckets {0, bucket_size, bucket_size,
                                                 table.hash(0) & (bucket_size - 1)};
    table.build(build_keys.data(), build_buckets.data(), build_keys.size(), mode);

    constexpr int probe_rows = 3;
    const std::array<uint32_t, probe_rows> probe_keys {11, 0, 42};
    const std::array<uint8_t, probe_rows> null_map {1, 0, 0};
    DorisVector<uint32_t> build_idx_map {bucket_size, table.hash(0) & (bucket_size - 1),
                                         table.hash(42) & (bucket_size - 1)};
    table.pre_build_idxs(build_idx_map);
    int probe_idx = 0;
    uint32_t build_idx = 0;
    bool probe_visited = false;
    std::vector<std::pair<uint32_t, uint32_t>> actual;
    for (int batch = 0; probe_idx < probe_rows; ++batch) {
        ASSERT_LT(batch, 8);
        std::array<uint32_t, 2> probe_idxs {};
        std::array<uint32_t, 2> build_idxs {};
        const auto [next_probe_idx, next_build_idx, matched] =
                table.template find_batch<JoinOpType>(
                        probe_keys.data(), build_idx_map.data(), probe_idx, build_idx, probe_rows,
                        probe_idxs.data(), probe_visited, build_idxs.data(), null_map.data(),
                        with_other_conjuncts, false, false);
        for (uint32_t row = 0; row < matched; ++row) {
            actual.emplace_back(probe_idxs[row], build_idxs[row]);
        }
        probe_idx = next_probe_idx;
        build_idx = next_build_idx;
    }
    EXPECT_EQ(actual, expected);
    if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN ||
                  JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                  JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                  JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        EXPECT_EQ(table.get_visited()[1], mode == JoinNullBucketMode::NULL_SAFE_EQUAL);
        EXPECT_EQ(table.get_visited()[2], mode == JoinNullBucketMode::NULL_SAFE_EQUAL);
        EXPECT_EQ(table.get_visited()[3], 1);
    }
}

TEST(JoinHashTableTest, NullSafeBucketMatchesResidualValues) {
    for (bool with_other_conjuncts : {false, true}) {
        check_null_bucket_matches<TJoinOp::INNER_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL,
                                                       with_other_conjuncts,
                                                       {{0, 2}, {0, 1}, {1, 3}});
        check_null_bucket_matches<TJoinOp::INNER_JOIN>(JoinNullBucketMode::DISCARD,
                                                       with_other_conjuncts, {{1, 3}});
    }
    check_null_bucket_matches<TJoinOp::FULL_OUTER_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL, false,
                                                        {{0, 2}, {0, 1}, {1, 3}, {2, 0}});
}

TEST(JoinHashTableTest, NullSafeBucketSemiAntiJoins) {
    check_null_bucket_matches<TJoinOp::LEFT_SEMI_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL, false,
                                                       {{0, 0}, {1, 0}});
    check_null_bucket_matches<TJoinOp::LEFT_ANTI_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL, false,
                                                       {{2, 0}});
    check_null_bucket_matches<TJoinOp::LEFT_ANTI_JOIN>(JoinNullBucketMode::DISCARD, false,
                                                       {{0, 0}, {2, 0}});
    check_null_bucket_matches<TJoinOp::RIGHT_SEMI_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL, false,
                                                        {});
    check_null_bucket_matches<TJoinOp::RIGHT_ANTI_JOIN>(JoinNullBucketMode::NULL_SAFE_EQUAL, false,
                                                        {});
}

TEST(JoinHashTableTest, NullAwareOtherConjunctSentinelResetsNullFlag) {
    JoinHashTable<uint32_t, std::hash<uint32_t>, false> table;
    table.prepare_build<TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN>(2, 8, false, 0);

    const std::array<uint32_t, 2> build_keys = {0, 7};
    const std::array<uint32_t, 2> build_buckets = {
            0, table.hash(build_keys[1]) & (table.get_bucket_size() - 1)};
    table.build(build_keys.data(), build_buckets.data(), build_keys.size(),
                JoinNullBucketMode::NULL_AWARE);

    const std::array<uint32_t, 1> probe_keys = {42};
    const uint32_t probe_bucket = table.hash(probe_keys[0]) & (table.get_bucket_size() - 1);
    const std::array<uint32_t, 1> build_idx_map = {table.get_first()[probe_bucket]};
    std::array<uint32_t, 9> probe_idxs = {};
    std::array<uint32_t, 9> build_idxs = {};
    std::array<uint8_t, 9> null_flags = {};
    null_flags[0] = 1; // A previous NULL-key probe left a NULL mark at this output position.

    const auto [probe_idx, build_idx, matched_cnt, picking_null_keys] =
            table.find_null_aware_with_other_conjuncts(
                    probe_keys.data(), build_idx_map.data(), 0, 0, probe_keys.size(),
                    probe_idxs.data(), build_idxs.data(), null_flags.data(), false, nullptr);

    EXPECT_EQ(1, probe_idx);
    EXPECT_EQ(0, build_idx);
    ASSERT_EQ(1, matched_cnt);
    EXPECT_FALSE(picking_null_keys);
    EXPECT_EQ(0, probe_idxs[0]);
    EXPECT_EQ(0, build_idxs[0]);
    EXPECT_EQ(0, null_flags[0]);
}

} // namespace doris
