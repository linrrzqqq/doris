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

#include "olap/rowset/beta_rowset.h"

#include <ctype.h>
#include <errno.h>
#include <fmt/format.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <ostream>
#include <utility>

#include "beta_rowset.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/crc32c.h"
#include "util/debug_points.h"
#include "util/doris_metrics.h"

namespace doris {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

std::string BetaRowset::local_segment_path_segcompacted(const std::string& tablet_path,
                                                        const RowsetId& rowset_id, int64_t begin,
                                                        int64_t end) {
    // {root_path}/data/{shard_id}/{tablet_id}/{schema_hash}/{rowset_id}_{begin_seg}-{end_seg}.dat
    return fmt::format("{}/{}_{}-{}.dat", tablet_path, rowset_id.to_string(), begin, end);
}

BetaRowset::BetaRowset(const TabletSchemaSPtr& schema, const RowsetMetaSharedPtr& rowset_meta,
                       std::string tablet_path)
        : Rowset(schema, rowset_meta, std::move(tablet_path)) {}

BetaRowset::~BetaRowset() = default;

Status BetaRowset::init() {
    return Status::OK(); // no op
}

Status BetaRowset::get_segment_num_rows(std::vector<uint32_t>* segment_rows) {
    DCHECK(_rowset_state_machine.rowset_state() == ROWSET_LOADED);

    RETURN_IF_ERROR(_load_segment_rows_once.call([this] {
        auto segment_count = num_segments();
        _segments_rows.resize(segment_count);
        for (int64_t i = 0; i != segment_count; ++i) {
            SegmentCacheHandle segment_cache_handle;
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                    std::static_pointer_cast<BetaRowset>(shared_from_this()), i,
                    &segment_cache_handle, false, false));
            const auto& tmp_segments = segment_cache_handle.get_segments();
            _segments_rows[i] = tmp_segments[0]->num_rows();
        }
        return Status::OK();
    }));
    segment_rows->assign(_segments_rows.cbegin(), _segments_rows.cend());
    return Status::OK();
}

Status BetaRowset::get_inverted_index_size(int64_t* index_size) {
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed, resource_id={}",
                                          _rowset_meta->resource_id());
    }

    if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
        for (const auto& index : _schema->inverted_indexes()) {
            for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
                auto seg_path = DORIS_TRY(segment_path(seg_id));
                int64_t file_size = 0;

                std::string inverted_index_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                index->index_id(), index->get_index_suffix());
                RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
                *index_size += file_size;
            }
        }
    } else {
        for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
            auto seg_path = DORIS_TRY(segment_path(seg_id));
            int64_t file_size = 0;

            std::string inverted_index_file_path = InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
            RETURN_IF_ERROR(fs->file_size(inverted_index_file_path, &file_size));
            *index_size += file_size;
        }
    }
    return Status::OK();
}

void BetaRowset::clear_inverted_index_cache() {
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = segment_path(i);
        if (!seg_path) {
            continue;
        }

        auto index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(*seg_path);
        for (const auto& column : tablet_schema()->columns()) {
            const TabletIndex* index_meta = tablet_schema()->inverted_index(*column);
            if (index_meta) {
                auto inverted_index_file_cache_key =
                        InvertedIndexDescriptor::get_index_file_cache_key(
                                index_path_prefix, index_meta->index_id(),
                                index_meta->get_index_suffix());
                (void)segment_v2::InvertedIndexSearcherCache::instance()->erase(
                        inverted_index_file_cache_key);
            }
        }
    }
}

Status BetaRowset::get_segments_size(std::vector<size_t>* segments_size) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed, resource_id={}",
                                          _rowset_meta->resource_id());
    }

    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));
        int64_t file_size;
        RETURN_IF_ERROR(fs->file_size(seg_path, &file_size));
        segments_size->push_back(file_size);
    }
    return Status::OK();
}

Status BetaRowset::load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments) {
    return load_segments(0, num_segments(), segments);
}

Status BetaRowset::load_segments(int64_t seg_id_begin, int64_t seg_id_end,
                                 std::vector<segment_v2::SegmentSharedPtr>* segments) {
    int64_t seg_id = seg_id_begin;
    while (seg_id < seg_id_end) {
        std::shared_ptr<segment_v2::Segment> segment;
        RETURN_IF_ERROR(load_segment(seg_id, nullptr, &segment));
        segments->push_back(std::move(segment));
        seg_id++;
    }
    return Status::OK();
}

Status BetaRowset::load_segment(int64_t seg_id, OlapReaderStatistics* stats,
                                segment_v2::SegmentSharedPtr* segment) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }

    DCHECK(seg_id >= 0);
    auto seg_path = DORIS_TRY(segment_path(seg_id));
    io::FileReaderOptions reader_options {
            .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                    : io::FileCachePolicy::NO_CACHE,
            .is_doris_table = true,
            .cache_base_path = "",
            .file_size = _rowset_meta->segment_file_size(static_cast<int>(seg_id)),
    };

    auto s = segment_v2::Segment::open(
            fs, seg_path, _rowset_meta->tablet_id(), static_cast<uint32_t>(seg_id), rowset_id(),
            _schema, reader_options, segment,
            _rowset_meta->inverted_index_file_info(static_cast<int>(seg_id)), stats);
    if (!s.ok()) {
        LOG(WARNING) << "failed to open segment. " << seg_path << " under rowset " << rowset_id()
                     << " : " << s.to_string();
        return s;
    }
    return Status::OK();
}

Status BetaRowset::create_reader(RowsetReaderSharedPtr* result) {
    // NOTE: We use std::static_pointer_cast for performance
    result->reset(new BetaRowsetReader(std::static_pointer_cast<BetaRowset>(shared_from_this())));
    return Status::OK();
}

Status BetaRowset::remove() {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::OK();
    }

    // TODO should we close and remove all segment reader first?
    VLOG_NOTICE << "begin to remove files in rowset " << rowset_id()
                << ", version:" << start_version() << "-" << end_version()
                << ", tabletid:" << _rowset_meta->tablet_id();
    // If the rowset was removed, it need to remove the fds in segment cache directly
    clear_cache();

    bool success = true;
    Status st;
    const auto& fs = io::global_local_filesystem();
    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        LOG(INFO) << "deleting " << seg_path;
        st = fs->delete_file(seg_path);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
            success = false;
        }

        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                const TabletIndex* index_meta = _schema->inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    st = fs->delete_file(inverted_index_file);
                    if (!st.ok()) {
                        LOG(WARNING) << st.to_string();
                        success = false;
                    }
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
                st = fs->delete_file(inverted_index_file);
                if (!st.ok()) {
                    LOG(WARNING) << st.to_string();
                    success = false;
                }
            }
        }
    }
    if (!success) {
        return Status::Error<ROWSET_DELETE_FILE_FAILED>("failed to remove files in rowset {}",
                                                        rowset_id().to_string());
    }
    return Status::OK();
}

void BetaRowset::do_close() {
    // do nothing.
}

Status BetaRowset::link_files_to(const std::string& dir, RowsetId new_rowset_id,
                                 size_t new_rowset_start_seg_id,
                                 std::set<int64_t>* without_index_uids) {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    const auto& local_fs = io::global_local_filesystem();
    Status status;
    std::vector<std::string> linked_success_files;
    Defer remove_linked_files {[&]() { // clear linked files if errors happen
        if (!status.ok()) {
            LOG(WARNING) << "will delete linked success files due to error " << status;
            std::vector<io::Path> paths;
            for (auto& file : linked_success_files) {
                paths.emplace_back(file);
                LOG(WARNING) << "will delete linked success file " << file << " due to error";
            }
            static_cast<void>(local_fs->batch_delete(paths));
            LOG(WARNING) << "done delete linked success files due to error " << status;
        }
    }};

    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path =
                local_segment_path(dir, new_rowset_id.to_string(), i + new_rowset_start_seg_id);
        bool dst_path_exist = false;
        if (!local_fs->exists(dst_path, &dst_path_exist).ok() || dst_path_exist) {
            status = Status::Error<FILE_ALREADY_EXIST>(
                    "failed to create hard link, file already exist: {}", dst_path);
            return status;
        }
        auto src_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        // TODO(lingbin): how external storage support link?
        //     use copy? or keep refcount to avoid being delete?
        if (!local_fs->link_file(src_path, dst_path).ok()) {
            status = Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                             src_path, dst_path, Errno::no());
            return status;
        }
        linked_success_files.push_back(dst_path);
        DBUG_EXECUTE_IF("fault_inject::BetaRowset::link_files_to::_link_inverted_index_file", {
            status = Status::Error<OS_ERROR>("fault_inject link_file error");
            return status;
        });
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : _schema->inverted_indexes()) {
                auto index_id = index->index_id();
                if (without_index_uids != nullptr && without_index_uids->count(index_id)) {
                    continue;
                }
                std::string inverted_index_src_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path),
                                index_id, index->get_index_suffix());
                std::string inverted_index_dst_file_path =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path),
                                index_id, index->get_index_suffix());
                bool index_file_exists = true;
                RETURN_IF_ERROR(local_fs->exists(inverted_index_src_file_path, &index_file_exists));
                if (index_file_exists) {
                    DBUG_EXECUTE_IF(
                            "fault_inject::BetaRowset::link_files_to::_link_inverted_index_file", {
                                status = Status::Error<OS_ERROR>(
                                        "fault_inject link_file error from={}, to={}",
                                        inverted_index_src_file_path, inverted_index_dst_file_path);
                                return status;
                            });
                    if (!local_fs->link_file(inverted_index_src_file_path,
                                             inverted_index_dst_file_path)
                                 .ok()) {
                        status = Status::Error<OS_ERROR>(
                                "fail to create hard link. from={}, to={}, errno={}",
                                inverted_index_src_file_path, inverted_index_dst_file_path,
                                Errno::no());
                        return status;
                    }
                    linked_success_files.push_back(inverted_index_dst_file_path);
                    LOG(INFO) << "success to create hard link. from="
                              << inverted_index_src_file_path << ", "
                              << "to=" << inverted_index_dst_file_path;
                } else {
                    LOG(WARNING) << "skip create hard link to not existed index file="
                                 << inverted_index_src_file_path;
                }
            }
        } else {
            if (_schema->has_inverted_index() &&
                (without_index_uids == nullptr || without_index_uids->empty())) {
                std::string inverted_index_file_src =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path));
                std::string inverted_index_file_dst =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path));
                bool index_dst_path_exist = false;

                if (!local_fs->exists(inverted_index_file_dst, &index_dst_path_exist).ok() ||
                    index_dst_path_exist) {
                    status = Status::Error<FILE_ALREADY_EXIST>(
                            "failed to create hard link, file already exist: {}",
                            inverted_index_file_dst);
                    return status;
                }
                if (!local_fs->link_file(inverted_index_file_src, inverted_index_file_dst).ok()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}",
                            inverted_index_file_src, inverted_index_file_dst, Errno::no());
                    return status;
                }
                linked_success_files.push_back(inverted_index_file_dst);
            }
        }
    }
    return Status::OK();
}

Status BetaRowset::copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    bool exists = false;
    for (int i = 0; i < num_segments(); ++i) {
        auto dst_path = local_segment_path(dir, new_rowset_id.to_string(), i);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(dst_path, &exists));
        if (exists) {
            return Status::Error<FILE_ALREADY_EXIST>("file already exist: {}", dst_path);
        }
        auto src_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(src_path, dst_path));
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_src_file_path =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(src_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    std::string inverted_index_dst_file_path =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(dst_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(
                            inverted_index_src_file_path, inverted_index_dst_file_path));
                    LOG(INFO) << "success to copy file. from=" << inverted_index_src_file_path
                              << ", "
                              << "to=" << inverted_index_dst_file_path;
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_src_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_path));
                std::string inverted_index_dst_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_path));
                RETURN_IF_ERROR(io::global_local_filesystem()->copy_path(inverted_index_src_file,
                                                                         inverted_index_dst_file));
                LOG(INFO) << "success to copy file. from=" << inverted_index_src_file << ", "
                          << "to=" << inverted_index_dst_file;
            }
        }
    }
    return Status::OK();
}

Status BetaRowset::upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) {
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    if (num_segments() < 1) {
        return Status::OK();
    }
    std::vector<io::Path> local_paths;
    local_paths.reserve(num_segments());
    std::vector<io::Path> dest_paths;
    dest_paths.reserve(num_segments());
    for (int i = 0; i < num_segments(); ++i) {
        // Note: Here we use relative path for remote.
        auto remote_seg_path = dest_fs.remote_segment_path(_rowset_meta->tablet_id(),
                                                           new_rowset_id.to_string(), i);
        auto local_seg_path = local_segment_path(_tablet_path, rowset_id().to_string(), i);
        dest_paths.emplace_back(remote_seg_path);
        local_paths.emplace_back(local_seg_path);
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                // if (column.has_inverted_index()) {
                const TabletIndex* index_meta = _schema->inverted_index(*column);
                if (index_meta) {
                    std::string remote_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(
                                            remote_seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    std::string local_inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(
                                            local_seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    dest_paths.emplace_back(remote_inverted_index_file);
                    local_paths.emplace_back(local_inverted_index_file);
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string remote_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        remote_seg_path));
                std::string local_inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(
                                        local_seg_path));
                dest_paths.emplace_back(remote_inverted_index_file);
                local_paths.emplace_back(local_inverted_index_file);
            }
        }
    }
    auto st = dest_fs.fs->batch_upload(local_paths, dest_paths);
    if (st.ok()) {
        DorisMetrics::instance()->upload_rowset_count->increment(1);
        DorisMetrics::instance()->upload_total_byte->increment(total_disk_size());
    } else {
        DorisMetrics::instance()->upload_fail_count->increment(1);
    }
    return st;
}

Status BetaRowset::check_file_exist() {
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::InternalError("fs is not initialized, resource_id={}",
                                     _rowset_meta->resource_id());
    }

    for (int i = 0; i < num_segments(); ++i) {
        auto seg_path = DORIS_TRY(segment_path(i));
        bool seg_file_exist = false;
        RETURN_IF_ERROR(fs->exists(seg_path, &seg_file_exist));
        if (!seg_file_exist) {
            return Status::InternalError("data file not existed: {}, rowset_id={}", seg_path,
                                         rowset_id().to_string());
        }
    }

    return Status::OK();
}

Status BetaRowset::check_current_rowset_segment() {
    const auto& fs = _rowset_meta->fs();
    if (!fs) {
        return Status::InternalError("fs is not initialized, resource_id={}",
                                     _rowset_meta->resource_id());
    }

    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));

        std::shared_ptr<segment_v2::Segment> segment;
        io::FileReaderOptions reader_options {
                .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                        : io::FileCachePolicy::NO_CACHE,
                .is_doris_table = true,
                .cache_base_path {},
                .file_size = _rowset_meta->segment_file_size(seg_id),
        };

        auto s = segment_v2::Segment::open(fs, seg_path, _rowset_meta->tablet_id(), seg_id,
                                           rowset_id(), _schema, reader_options, &segment,
                                           _rowset_meta->inverted_index_file_info(seg_id));
        if (!s.ok()) {
            LOG(WARNING) << "segment can not be opened. file=" << seg_path;
            return s;
        }
    }

    return Status::OK();
}

Status BetaRowset::add_to_binlog() {
    // FIXME(Drogon): not only local file system
    if (!is_local()) {
        DCHECK(false) << _rowset_meta->tablet_id() << ' ' << rowset_id();
        return Status::InternalError("should be local rowset. tablet_id={} rowset_id={}",
                                     _rowset_meta->tablet_id(), rowset_id().to_string());
    }

    const auto& fs = io::global_local_filesystem();
    auto segments_num = num_segments();
    VLOG_DEBUG << fmt::format("add rowset to binlog. rowset_id={}, segments_num={}",
                              rowset_id().to_string(), segments_num);

    Status status;
    std::vector<std::string> linked_success_files;
    Defer remove_linked_files {[&]() { // clear linked files if errors happen
        if (!status.ok()) {
            LOG(WARNING) << "will delete linked success files due to error "
                         << status.to_string_no_stack();
            std::vector<io::Path> paths;
            for (auto& file : linked_success_files) {
                paths.emplace_back(file);
                LOG(WARNING) << "will delete linked success file " << file << " due to error";
            }
            static_cast<void>(fs->batch_delete(paths));
            LOG(WARNING) << "done delete linked success files due to error "
                         << status.to_string_no_stack();
        }
    }};

    // The publish_txn might fail even if the add_to_binlog success, so we need to check
    // whether a file already exists before linking.
    auto errno_is_file_exists = []() { return Errno::no() == EEXIST; };

    // all segments are in the same directory, so cache binlog_dir without multi times check
    std::string binlog_dir;
    for (int i = 0; i < segments_num; ++i) {
        auto seg_file = local_segment_path(_tablet_path, rowset_id().to_string(), i);

        if (binlog_dir.empty()) {
            binlog_dir = std::filesystem::path(seg_file).parent_path().append("_binlog").string();

            bool exists = true;
            RETURN_IF_ERROR(fs->exists(binlog_dir, &exists));
            if (!exists) {
                RETURN_IF_ERROR(fs->create_directory(binlog_dir));
            }
        }

        auto binlog_file =
                (std::filesystem::path(binlog_dir) / std::filesystem::path(seg_file).filename())
                        .string();
        VLOG_DEBUG << "link " << seg_file << " to " << binlog_file;
        if (!fs->link_file(seg_file, binlog_file).ok() && !errno_is_file_exists()) {
            status = Status::Error<OS_ERROR>("fail to create hard link. from={}, to={}, errno={}",
                                             seg_file, binlog_file, Errno::no());
            return status;
        }
        linked_success_files.push_back(binlog_file);

        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : _schema->inverted_indexes()) {
                auto index_id = index->index_id();
                auto index_file = InvertedIndexDescriptor::get_index_file_path_v1(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_file), index_id,
                        index->get_index_suffix());
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                if (!fs->link_file(index_file, binlog_index_file).ok() && !errno_is_file_exists()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}", index_file,
                            binlog_index_file, Errno::no());
                    return status;
                }
                linked_success_files.push_back(binlog_index_file);
            }
        } else {
            if (_schema->has_inverted_index()) {
                auto index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_file));
                auto binlog_index_file = (std::filesystem::path(binlog_dir) /
                                          std::filesystem::path(index_file).filename())
                                                 .string();
                VLOG_DEBUG << "link " << index_file << " to " << binlog_index_file;
                if (!fs->link_file(index_file, binlog_index_file).ok() && !errno_is_file_exists()) {
                    status = Status::Error<OS_ERROR>(
                            "fail to create hard link. from={}, to={}, errno={}", index_file,
                            binlog_index_file, Errno::no());
                    return status;
                }
                linked_success_files.push_back(binlog_index_file);
            }
        }
    }

    return Status::OK();
}

Status BetaRowset::calc_file_crc(uint32_t* crc_value, int64_t* file_count) {
    const auto& fs = _rowset_meta->fs();
    DBUG_EXECUTE_IF("fault_inject::BetaRowset::calc_file_crc",
                    { return Status::Error<OS_ERROR>("fault_inject calc_file_crc error"); });
    if (num_segments() < 1) {
        *crc_value = 0x92a8fc17; // magic code from crc32c table
        return Status::OK();
    }

    // 1. pick up all the files including dat file and idx file
    std::vector<io::Path> file_paths;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto seg_path = DORIS_TRY(segment_path(seg_id));
        file_paths.emplace_back(seg_path);
        if (_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (auto& column : _schema->columns()) {
                const TabletIndex* index_meta = _schema->inverted_index(*column);
                if (index_meta) {
                    std::string inverted_index_file =
                            InvertedIndexDescriptor::get_index_file_path_v1(
                                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path),
                                    index_meta->index_id(), index_meta->get_index_suffix());
                    file_paths.emplace_back(std::move(inverted_index_file));
                }
            }
        } else {
            if (_schema->has_inverted_index()) {
                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_path_v2(
                        InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
                file_paths.emplace_back(std::move(inverted_index_file));
            }
        }
    }
    *crc_value = 0;
    *file_count = file_paths.size();
    if (!is_local()) {
        return Status::OK();
    }

    // 2. calculate the md5sum of each file
    const auto& local_fs = io::global_local_filesystem();
    DCHECK(!file_paths.empty());
    std::vector<std::string> all_file_md5;
    all_file_md5.reserve(file_paths.size());
    for (const auto& file_path : file_paths) {
        std::string file_md5sum;
        auto status = local_fs->md5sum(file_path, &file_md5sum);
        if (!status.ok()) {
            return status;
        }
        VLOG_CRITICAL << fmt::format("calc file_md5sum finished. file_path={}, md5sum={}",
                                     file_path.string(), file_md5sum);
        all_file_md5.emplace_back(std::move(file_md5sum));
    }
    std::sort(all_file_md5.begin(), all_file_md5.end());

    // 3. calculate the crc_value based on all_file_md5
    DCHECK(file_paths.size() == all_file_md5.size());
    for (auto& i : all_file_md5) {
        *crc_value = crc32c::Extend(*crc_value, i.data(), i.size());
    }

    return Status::OK();
}

Status BetaRowset::show_nested_index_file(rapidjson::Value* rowset_value,
                                          rapidjson::Document::AllocatorType& allocator) {
    const auto& fs = _rowset_meta->fs();
    auto storage_format = _schema->get_inverted_index_storage_format();
    std::string format_str;
    switch (storage_format) {
    case InvertedIndexStorageFormatPB::V1:
        format_str = "V1";
        break;
    case InvertedIndexStorageFormatPB::V2:
        format_str = "V2";
        break;
    case InvertedIndexStorageFormatPB::V3:
        format_str = "V3";
        break;
    default:
        return Status::InternalError("inverted index storage format error");
        break;
    }
    auto rs_id = rowset_id().to_string();
    rowset_value->AddMember("rowset_id", rapidjson::Value(rs_id.c_str(), allocator), allocator);
    rowset_value->AddMember("index_storage_format", rapidjson::Value(format_str.c_str(), allocator),
                            allocator);
    rapidjson::Value segments(rapidjson::kArrayType);
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        rapidjson::Value segment(rapidjson::kObjectType);
        segment.AddMember("segment_id", rapidjson::Value(seg_id).Move(), allocator);

        auto seg_path = DORIS_TRY(segment_path(seg_id));
        auto index_file_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(seg_path);
        auto index_file_reader = std::make_unique<IndexFileReader>(
                fs, std::string(index_file_path_prefix), storage_format);
        RETURN_IF_ERROR(index_file_reader->init());
        auto dirs = index_file_reader->get_all_directories();

        auto add_file_info_to_json = [&](const std::string& path,
                                         rapidjson::Value& json_value) -> Status {
            json_value.AddMember("idx_file_path", rapidjson::Value(path.c_str(), allocator),
                                 allocator);
            int64_t idx_file_size = 0;
            auto st = fs->file_size(path, &idx_file_size);
            if (st != Status::OK()) {
                LOG(WARNING) << "show nested index file get file size error, file: " << path
                             << ", error: " << st.msg();
                return st;
            }
            json_value.AddMember("idx_file_size", rapidjson::Value(idx_file_size).Move(),
                                 allocator);
            return Status::OK();
        };

        auto process_files = [&allocator, &index_file_reader](auto& index_meta,
                                                              rapidjson::Value& indices,
                                                              rapidjson::Value& index) -> Status {
            rapidjson::Value files_value(rapidjson::kArrayType);
            std::vector<std::string> files;
            auto ret = index_file_reader->open(&index_meta);
            if (!ret.has_value()) {
                LOG(INFO) << "IndexFileReader open error:" << ret.error();
                return Status::InternalError("IndexFileReader open error");
            }
            using T = std::decay_t<decltype(ret)>;
            auto reader = std::forward<T>(ret).value();
            reader->list(&files);
            for (auto& file : files) {
                rapidjson::Value file_value(rapidjson::kObjectType);
                auto size = reader->fileLength(file.c_str());
                file_value.AddMember("name", rapidjson::Value(file.c_str(), allocator), allocator);
                file_value.AddMember("size", rapidjson::Value(size).Move(), allocator);
                files_value.PushBack(file_value, allocator);
            }
            index.AddMember("files", files_value, allocator);
            indices.PushBack(index, allocator);
            return Status::OK();
        };

        if (storage_format != InvertedIndexStorageFormatPB::V1) {
            auto path = InvertedIndexDescriptor::get_index_file_path_v2(index_file_path_prefix);
            auto st = add_file_info_to_json(path, segment);
            if (!st.ok()) {
                return st;
            }
            rapidjson::Value indices(rapidjson::kArrayType);
            for (auto& dir : *dirs) {
                rapidjson::Value index(rapidjson::kObjectType);
                auto index_id = dir.first.first;
                auto index_suffix = dir.first.second;
                index.AddMember("index_id", rapidjson::Value(index_id).Move(), allocator);
                index.AddMember("index_suffix", rapidjson::Value(index_suffix.c_str(), allocator),
                                allocator);

                rapidjson::Value files_value(rapidjson::kArrayType);
                std::vector<std::string> files;
                doris::TabletIndexPB index_pb;
                index_pb.set_index_id(index_id);
                index_pb.set_index_suffix_name(index_suffix);
                TabletIndex index_meta;
                index_meta.init_from_pb(index_pb);

                auto status = process_files(index_meta, indices, index);
                if (!status.ok()) {
                    return status;
                }
            }
            segment.AddMember("indices", indices, allocator);
            segments.PushBack(segment, allocator);
        } else {
            rapidjson::Value indices(rapidjson::kArrayType);
            for (auto column : _rowset_meta->tablet_schema()->columns()) {
                const auto* index_meta = _rowset_meta->tablet_schema()->inverted_index(*column);
                if (index_meta == nullptr) {
                    continue;
                }
                rapidjson::Value index(rapidjson::kObjectType);
                auto index_id = index_meta->index_id();
                auto index_suffix = index_meta->get_index_suffix();
                index.AddMember("index_id", rapidjson::Value(index_id).Move(), allocator);
                index.AddMember("index_suffix", rapidjson::Value(index_suffix.c_str(), allocator),
                                allocator);
                auto path = InvertedIndexDescriptor::get_index_file_path_v1(index_file_path_prefix,
                                                                            index_id, index_suffix);
                auto st = add_file_info_to_json(path, index);
                if (!st.ok()) {
                    return st;
                }

                auto status = process_files(*index_meta, indices, index);
                if (!status.ok()) {
                    return status;
                }
            }
            segment.AddMember("indices", indices, allocator);
            segments.PushBack(segment, allocator);
        }
    }
    rowset_value->AddMember("segments", segments, allocator);
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris
