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

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/memtable.h"
#include "util/threadpool.h"

namespace doris {

class DataDir;
class MemTable;
class RowsetWriter;
class WorkloadGroup;

// the statistic of a certain flush handler.
// use atomic because it may be updated by multi threads
struct FlushStatistic {
    std::atomic_uint64_t flush_time_ns = 0;
    std::atomic_uint64_t flush_submit_count = 0;
    std::atomic_int64_t flush_running_count = 0;
    std::atomic_uint64_t flush_finish_count = 0;
    std::atomic_uint64_t flush_size_bytes = 0;
    std::atomic_uint64_t flush_disk_size_bytes = 0;
    std::atomic_uint64_t flush_wait_time_ns = 0;
};

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat);

// A thin wrapper of ThreadPoolToken to submit task.
// For a tablet, there may be multiple memtables, which will be flushed to disk
// one by one in the order of generation.
// If a memtable flush fails, then:
// 1. Immediately disallow submission of any subsequent memtable
// 2. For the memtables that have already been submitted, there is no need to flush,
//    because the entire job will definitely fail;
class FlushToken : public std::enable_shared_from_this<FlushToken> {
    ENABLE_FACTORY_CREATOR(FlushToken);

public:
    FlushToken(ThreadPool* thread_pool, std::shared_ptr<WorkloadGroup> wg_sptr)
            : _flush_status(Status::OK()), _thread_pool(thread_pool), _wg_wptr(wg_sptr) {}

    Status submit(std::shared_ptr<MemTable> mem_table);

    // error has happens, so we cancel this token
    // And remove all tasks in the queue.
    void cancel();

    // wait all tasks in token to be completed.
    Status wait();

    // get flush operations' statistics
    const FlushStatistic& get_stats() const { return _stats; }

    void set_rowset_writer(std::shared_ptr<RowsetWriter> rowset_writer) {
        _rowset_writer = rowset_writer;
    }

    const MemTableStat& memtable_stat() { return _memtable_stat; }

private:
    void _shutdown_flush_token() { _shutdown.store(true); }
    bool _is_shutdown() { return _shutdown.load(); }
    void _wait_submit_task_finish();
    void _wait_running_task_finish();

private:
    friend class MemtableFlushTask;

    void _flush_memtable(std::shared_ptr<MemTable> memtable_ptr, int32_t segment_id,
                         int64_t submit_task_time);

    Status _do_flush_memtable(MemTable* memtable, int32_t segment_id, int64_t* flush_size);

    Status _try_reserve_memory(const std::shared_ptr<ResourceContext>& resource_context,
                               int64_t size);

    // Records the current flush status of the tablet.
    // Note: Once its value is set to Failed, it cannot return to SUCCESS.
    std::shared_mutex _flush_status_lock;
    Status _flush_status;

    FlushStatistic _stats;

    std::shared_ptr<RowsetWriter> _rowset_writer = nullptr;

    MemTableStat _memtable_stat;

    std::atomic<bool> _shutdown = false;
    ThreadPool* _thread_pool = nullptr;

    std::mutex _mutex;
    std::condition_variable _submit_task_finish_cond;
    std::condition_variable _running_task_finish_cond;

    std::weak_ptr<WorkloadGroup> _wg_wptr;
};

// MemTableFlushExecutor is responsible for flushing memtables to disk.
// It encapsulate a ThreadPool to handle all tasks.
// Usage Example:
//      ...
//      std::shared_ptr<FlushHandler> flush_handler;
//      memTableFlushExecutor.create_flush_token(&flush_handler);
//      ...
//      flush_token->submit(memtable)
//      ...
class MemTableFlushExecutor {
public:
    MemTableFlushExecutor() = default;
    ~MemTableFlushExecutor() {
        _flush_pool->shutdown();
        _high_prio_flush_pool->shutdown();
    }

    // init should be called after storage engine is opened,
    // because it needs path hash of each data dir.
    void init(int num_disk);

    Status create_flush_token(std::shared_ptr<FlushToken>& flush_token,
                              std::shared_ptr<RowsetWriter> rowset_writer, bool is_high_priority,
                              std::shared_ptr<WorkloadGroup> wg_sptr);

    // return true if it already has any flushing task
    bool check_and_inc_has_any_flushing_task() {
        // need to use CAS instead of only `if (0 == _flushing_task_count)` statement,
        // to avoid concurrent entries both pass the if statement
        int expected_count = 0;
        if (!_flushing_task_count.compare_exchange_strong(expected_count, 1)) {
            return true;
        }
        DCHECK(expected_count == 0 && _flushing_task_count == 1);
        return false;
    }

    void inc_flushing_task() { _flushing_task_count++; }

    void dec_flushing_task() { _flushing_task_count--; }

    ThreadPool* flush_pool() { return _flush_pool.get(); }

private:
    std::unique_ptr<ThreadPool> _flush_pool;
    std::unique_ptr<ThreadPool> _high_prio_flush_pool;
    std::atomic<int> _flushing_task_count = 0;
};

} // namespace doris
