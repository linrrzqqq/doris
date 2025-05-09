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

#include "common/daemon.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gflags/gflags.h>

#include "runtime/memory/jemalloc_control.h"
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#endif
// IWYU pragma: no_include <bits/std_abs.h>
#include <butil/iobuf.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <ostream>
#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/be_proc_monitor.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/memory_reclamation.h"
#include "runtime/process_profile.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/algorithm_util.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/perf_counters.h"
#include "util/system_metrics.h"
#include "util/time.h"

namespace doris {
namespace {

std::atomic<int64_t> last_print_proc_mem = 0;
std::atomic<int32_t> refresh_cache_capacity_sleep_time_ms = 0;
std::atomic<int32_t> memory_gc_sleep_time = 0;
#ifdef USE_JEMALLOC
std::atomic<int32_t> je_reset_dirty_decay_sleep_time_ms = 0;
#endif

void update_rowsets_and_segments_num_metrics() {
    if (config::is_cloud_mode()) {
        // TODO(plat1ko): CloudStorageEngine
    } else {
        StorageEngine& engine = ExecEnv::GetInstance()->storage_engine().to_local();
        auto* metrics = DorisMetrics::instance();
        metrics->all_rowsets_num->set_value(engine.tablet_manager()->get_rowset_nums());
        metrics->all_segments_num->set_value(engine.tablet_manager()->get_segment_nums());
    }
}

} // namespace

void Daemon::tcmalloc_gc_thread() {
    // TODO All cache GC wish to be supported
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && \
        !defined(USE_JEMALLOC)

    // Limit size of tcmalloc cache via release_rate and max_cache_percent.
    // We adjust release_rate according to memory_pressure, which is usage percent of memory.
    int64_t max_cache_percent = 60;
    double release_rates[10] = {1.0, 1.0, 1.0, 5.0, 5.0, 20.0, 50.0, 100.0, 500.0, 2000.0};
    int64_t pressure_limit = 90;
    bool is_performance_mode = false;
    int64_t physical_limit_bytes =
            std::min(MemInfo::physical_mem() - MemInfo::sys_mem_available_low_water_mark(),
                     MemInfo::mem_limit());

    if (config::memory_mode == std::string("performance")) {
        max_cache_percent = 100;
        pressure_limit = 90;
        is_performance_mode = true;
        physical_limit_bytes = std::min(MemInfo::mem_limit(), MemInfo::physical_mem());
    } else if (config::memory_mode == std::string("compact")) {
        max_cache_percent = 20;
        pressure_limit = 80;
    }

    int last_ms = 0;
    const int kMaxLastMs = 30000;
    const int kIntervalMs = 10;
    size_t init_aggressive_decommit = 0;
    size_t current_aggressive_decommit = 0;
    size_t expected_aggressive_decommit = 0;
    int64_t last_memory_pressure = 0;

    MallocExtension::instance()->GetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                                    &init_aggressive_decommit);
    current_aggressive_decommit = init_aggressive_decommit;

    while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(kIntervalMs))) {
        size_t tc_used_bytes = 0;
        size_t tc_alloc_bytes = 0;
        size_t rss = PerfCounters::get_vm_rss();

        MallocExtension::instance()->GetNumericProperty("generic.total_physical_bytes",
                                                        &tc_alloc_bytes);
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes",
                                                        &tc_used_bytes);
        int64_t tc_cached_bytes = (int64_t)tc_alloc_bytes - (int64_t)tc_used_bytes;
        int64_t to_free_bytes =
                (int64_t)tc_cached_bytes - ((int64_t)tc_used_bytes * max_cache_percent / 100);
        to_free_bytes = std::max(to_free_bytes, (int64_t)0);

        int64_t memory_pressure = 0;
        int64_t rss_pressure = 0;
        int64_t alloc_bytes = std::max(rss, tc_alloc_bytes);
        memory_pressure = alloc_bytes * 100 / physical_limit_bytes;
        rss_pressure = rss * 100 / physical_limit_bytes;

        expected_aggressive_decommit = init_aggressive_decommit;
        if (memory_pressure > pressure_limit) {
            // We are reaching oom, so release cache aggressively.
            // Ideally, we should reuse cache and not allocate from system any more,
            // however, it is hard to set limit on cache of tcmalloc and doris
            // use mmap in vectorized mode.
            // Limit cache capactiy is enough.
            if (rss_pressure > pressure_limit) {
                int64_t min_free_bytes = alloc_bytes - physical_limit_bytes * 9 / 10;
                to_free_bytes = std::max(to_free_bytes, min_free_bytes);
                to_free_bytes = std::max(to_free_bytes, tc_cached_bytes * 30 / 100);
                // We assure that we have at least 500M bytes in cache.
                to_free_bytes = std::min(to_free_bytes, tc_cached_bytes - 500 * 1024 * 1024);
                expected_aggressive_decommit = 1;
            }
            last_ms = kMaxLastMs;
        } else if (memory_pressure > (pressure_limit - 10)) {
            // In most cases, adjusting release rate is enough, if memory are consumed quickly
            // we should release manually.
            if (last_memory_pressure <= (pressure_limit - 10)) {
                to_free_bytes = std::max(to_free_bytes, tc_cached_bytes * 10 / 100);
            }
        }

        int release_rate_index = memory_pressure / 10;
        double release_rate = 1.0;
        if (release_rate_index >= sizeof(release_rates) / sizeof(release_rates[0])) {
            release_rate = 2000.0;
        } else {
            release_rate = release_rates[release_rate_index];
        }
        MallocExtension::instance()->SetMemoryReleaseRate(release_rate);

        if ((current_aggressive_decommit != expected_aggressive_decommit) && !is_performance_mode) {
            MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                                            expected_aggressive_decommit);
            current_aggressive_decommit = expected_aggressive_decommit;
        }

        last_memory_pressure = memory_pressure;
        // We release at least 2% bytes once, frequent releasing hurts performance.
        if (to_free_bytes > (physical_limit_bytes * 2 / 100)) {
            last_ms += kIntervalMs;
            if (last_ms >= kMaxLastMs) {
                LOG(INFO) << "generic.current_allocated_bytes " << tc_used_bytes
                          << ", generic.total_physical_bytes " << tc_alloc_bytes << ", rss " << rss
                          << ", max_cache_percent " << max_cache_percent << ", release_rate "
                          << release_rate << ", memory_pressure " << memory_pressure
                          << ", physical_limit_bytes " << physical_limit_bytes << ", to_free_bytes "
                          << to_free_bytes << ", current_aggressive_decommit "
                          << current_aggressive_decommit;
                MallocExtension::instance()->ReleaseToSystem(to_free_bytes);
                last_ms = 0;
            }
        } else {
            last_ms = 0;
        }
    }
#endif
}

void refresh_process_memory_metrics() {
    doris::PerfCounters::refresh_proc_status();
    doris::MemInfo::refresh_proc_meminfo();
    doris::GlobalMemoryArbitrator::reset_refresh_interval_memory_growth();
    ExecEnv::GetInstance()->brpc_iobuf_block_memory_tracker()->set_consumption(
            butil::IOBuf::block_memory());
}

void refresh_common_allocator_metrics() {
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    doris::JemallocControl::refresh_allocator_mem();
    if (config::enable_system_metrics) {
        DorisMetrics::instance()->system_metrics()->update_allocator_metrics();
    }
#endif
    doris::GlobalMemoryArbitrator::refresh_memory_bvar();
}

void refresh_memory_state_after_memory_change() {
    if (abs(last_print_proc_mem - PerfCounters::get_vm_rss()) > 268435456) {
        last_print_proc_mem = PerfCounters::get_vm_rss();
        doris::MemTrackerLimiter::clean_tracker_limiter_group();
        doris::ProcessProfile::instance()->memory_profile()->enable_print_log_process_usage();
        doris::ProcessProfile::instance()->memory_profile()->refresh_memory_overview_profile();
        doris::JemallocControl::notify_je_purge_dirty_pages();
        LOG(INFO) << doris::GlobalMemoryArbitrator::
                        process_mem_log_str(); // print mem log when memory state by 256M
    }
}

void refresh_cache_capacity() {
    if (doris::GlobalMemoryArbitrator::cache_adjust_capacity_notify.load(
                std::memory_order_relaxed)) {
        // the last cache capacity adjustment has not been completed.
        // if not return, last_periodic_refreshed_cache_capacity_adjust_weighted may be modified, but notify is ignored.
        return;
    }
    if (refresh_cache_capacity_sleep_time_ms <= 0) {
        auto cache_capacity_reduce_mem_limit = int64_t(
                doris::MemInfo::soft_mem_limit() * config::cache_capacity_reduce_mem_limit_frac);
        int64_t process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();
        // the rule is like this:
        // 1. if the process mem usage < soft memlimit * 0.6, then do not need adjust cache capacity.
        // 2. if the process mem usage > soft memlimit * 0.6 and process mem usage < soft memlimit, then it will be adjusted to a lower value.
        // 3. if the process mem usage > soft memlimit, then the capacity is adjusted to 0.
        double new_cache_capacity_adjust_weighted =
                AlgoUtil::descent_by_step(10, cache_capacity_reduce_mem_limit,
                                          doris::MemInfo::soft_mem_limit(), process_memory_usage);
        if (new_cache_capacity_adjust_weighted !=
            doris::GlobalMemoryArbitrator::last_periodic_refreshed_cache_capacity_adjust_weighted) {
            doris::GlobalMemoryArbitrator::last_periodic_refreshed_cache_capacity_adjust_weighted =
                    new_cache_capacity_adjust_weighted;
            doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
            refresh_cache_capacity_sleep_time_ms = config::memory_gc_sleep_time_ms;
        } else {
            refresh_cache_capacity_sleep_time_ms = 0;
        }
    }
    refresh_cache_capacity_sleep_time_ms -= config::memory_maintenance_sleep_time_ms;
}

void je_reset_dirty_decay() {
#ifdef USE_JEMALLOC
    if (doris::JemallocControl::je_reset_dirty_decay_notify.load(std::memory_order_relaxed)) {
        // if not return, je_enable_dirty_page may be modified, but notify is ignored.
        return;
    }

    if (je_reset_dirty_decay_sleep_time_ms <= 0) {
        bool new_je_enable_dirty_page = true;
        if (doris::JemallocControl::je_enable_dirty_page) {
            // if Jemalloc dirty page is enabled and process memory exceed soft mem limit,
            // disable Jemalloc dirty page.
            new_je_enable_dirty_page = !GlobalMemoryArbitrator::is_exceed_soft_mem_limit();
        } else {
            // if Jemalloc dirty page is disabled and 10% free memory left to exceed soft mem limit,
            // enable Jemalloc dirty page, this is to avoid frequent changes
            // between enabling and disabling Jemalloc dirty pages, if the process memory does
            // not exceed the soft mem limit after turning off Jemalloc dirty pages,
            // but it will exceed soft mem limit after turning it on.
            new_je_enable_dirty_page = !GlobalMemoryArbitrator::is_exceed_soft_mem_limit(
                    int64_t(doris::MemInfo::soft_mem_limit() * 0.1));
        }

        if (doris::JemallocControl::je_enable_dirty_page != new_je_enable_dirty_page) {
            // `notify_je_reset_dirty_decay` only if `je_enable_dirty_page` changes.
            doris::JemallocControl::je_enable_dirty_page = new_je_enable_dirty_page;
            doris::JemallocControl::notify_je_reset_dirty_decay();
            je_reset_dirty_decay_sleep_time_ms = config::memory_gc_sleep_time_ms;
        } else {
            je_reset_dirty_decay_sleep_time_ms = 0;
        }
    }
    je_reset_dirty_decay_sleep_time_ms -= config::memory_maintenance_sleep_time_ms;
#endif
}

void memory_gc() {
    if (config::disable_memory_gc) {
        return;
    }
    if (memory_gc_sleep_time <= 0) {
        auto gc_func = [](const std::string& revoke_reason) {
            doris::ProcessProfile::instance()->memory_profile()->print_log_process_usage();
            if (doris::MemoryReclamation::revoke_process_memory(revoke_reason)) {
                // If there is not enough memory to be gc, the process memory usage will not be printed in the next continuous gc.
                doris::ProcessProfile::instance()
                        ->memory_profile()
                        ->enable_print_log_process_usage();
            }
        };

        if (doris::GlobalMemoryArbitrator::sys_mem_available() <
            doris::MemInfo::sys_mem_available_low_water_mark()) {
            gc_func("sys available memory less than low water mark");
        } else if (doris::GlobalMemoryArbitrator::process_memory_usage() >
                   doris::MemInfo::mem_limit()) {
            gc_func("process memory used exceed limit");
        }
        memory_gc_sleep_time = config::memory_gc_sleep_time_ms;
    }
    memory_gc_sleep_time -= config::memory_maintenance_sleep_time_ms;
}

void Daemon::memory_maintenance_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::memory_maintenance_sleep_time_ms))) {
        // step 1. Refresh process memory metrics.
        refresh_process_memory_metrics();

        // step 2. Refresh jemalloc/tcmalloc metrics.
        refresh_common_allocator_metrics();

        // step 3. Update and print memory stat when the memory changes by 256M.
        refresh_memory_state_after_memory_change();

        // step 4. Asyn Refresh cache capacity
        // TODO adjust cache capacity based on smoothstep (smooth gradient).
        refresh_cache_capacity();

        // step 5. Cancel top memory task when process memory exceed hard limit.
        memory_gc();

        // step 6. Refresh weighted memory ratio of workload groups.
        doris::ExecEnv::GetInstance()->workload_group_mgr()->do_sweep();
        doris::ExecEnv::GetInstance()->workload_group_mgr()->refresh_wg_weighted_memory_limit();

        // step 7: handle paused queries(caused by memory insufficient)
        doris::ExecEnv::GetInstance()->workload_group_mgr()->handle_paused_queries();

        // step 8. Flush memtable
        doris::GlobalMemoryArbitrator::notify_memtable_memory_refresh();
        // TODO notify flush memtable

        // step 9. Reset Jemalloc dirty page decay.
        je_reset_dirty_decay();
    }
}

void Daemon::memtable_memory_refresh_thread() {
    // Refresh the memory statistics of the load channel tracker more frequently,
    // which helps to accurately control the memory of LoadChannelMgr.
    do {
        std::unique_lock<std::mutex> l(doris::GlobalMemoryArbitrator::memtable_memory_refresh_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::GlobalMemoryArbitrator::memtable_memory_refresh_notify.load(
                       std::memory_order_relaxed)) {
            doris::GlobalMemoryArbitrator::memtable_memory_refresh_cv.wait_for(
                    l, std::chrono::milliseconds(100));
        }
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }

        Defer defer {[&]() {
            doris::GlobalMemoryArbitrator::memtable_memory_refresh_notify.store(
                    false, std::memory_order_relaxed);
        }};
        doris::ExecEnv::GetInstance()->memtable_memory_limiter()->refresh_mem_tracker();
    } while (true);
}

/*
 * this thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void Daemon::calculate_metrics_thread() {
    int64_t last_ts = -1L;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    do {
        DorisMetrics::instance()->metric_registry()->trigger_all_hooks(true);

        if (last_ts == -1L) {
            last_ts = GetMonoTimeMicros() / 1000;
            lst_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            if (config::enable_system_metrics) {
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);
            }
        } else {
            int64_t current_ts = GetMonoTimeMicros() / 1000;
            long interval = (current_ts - last_ts) / 1000;
            last_ts = current_ts;

            // 1. query bytes per second
            int64_t current_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval + 1);
            DorisMetrics::instance()->query_scan_bytes_per_second->set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            if (config::enable_system_metrics) {
                // 2. max disk io util
                DorisMetrics::instance()->system_metrics()->update_max_disk_io_util_percent(
                        lst_disks_io_time, 15);

                // update lst map
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

                // 3. max network traffic
                int64_t max_send = 0;
                int64_t max_receive = 0;
                DorisMetrics::instance()->system_metrics()->get_max_net_traffic(
                        lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
                DorisMetrics::instance()->system_metrics()->update_max_network_send_bytes_rate(
                        max_send);
                DorisMetrics::instance()->system_metrics()->update_max_network_receive_bytes_rate(
                        max_receive);
                // update lst map
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);

                DorisMetrics::instance()->system_metrics()->update_be_avail_cpu_num();
            }
            update_rowsets_and_segments_num_metrics();
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(15)));
}

void Daemon::report_runtime_query_statistics_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::report_query_statistics_interval_ms))) {
        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->report_runtime_query_statistics();
    }
}

void Daemon::je_reset_dirty_decay_thread() const {
    do {
        std::unique_lock<std::mutex> l(doris::JemallocControl::je_reset_dirty_decay_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::JemallocControl::je_reset_dirty_decay_notify.load(
                       std::memory_order_relaxed)) {
            doris::JemallocControl::je_reset_dirty_decay_cv.wait_for(
                    l, std::chrono::milliseconds(100));
        }
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }

        Defer defer {[&]() {
            doris::JemallocControl::je_reset_dirty_decay_notify.store(false,
                                                                      std::memory_order_relaxed);
        }};
#ifdef USE_JEMALLOC
        if (config::disable_memory_gc || !config::enable_je_purge_dirty_pages) {
            continue;
        }

        // There is a significant difference only when dirty_decay_ms is equal to 0 or not.
        //
        // 1. When dirty_decay_ms is not equal to 0, the free memory will be cached in the Jemalloc
        // dirty page first. even if dirty_decay_ms is equal to 1, the Jemalloc dirty page will not
        // be released to the system exactly after 1ms, it will be released according to the decay rule.
        // The Jemalloc document specifies that dirty_decay_ms is an approximate time.
        //
        // 2. It has been observed in an actual cluster that even if dirty_decay_ms is changed
        // from th default 5000 to 1, Jemalloc dirty page will still cache a large amount of memory, everything
        // seems to be the same as `dirty_decay_ms:5000`. only when dirty_decay_ms is changed to 0,
        // jemalloc dirty page will stop caching and free memory will be released to the system immediately.
        // of course, performance will be affected.
        //
        // 3. After reducing dirty_decay_ms, manually calling `decay_all_arena_dirty_pages` may release dirty pages
        // as soon as possible, but no relevant experimental data can be found, so it is simple and safe
        // to adjust dirty_decay_ms only between zero and non-zero.

        if (doris::JemallocControl::je_enable_dirty_page) {
            doris::JemallocControl::je_reset_all_arena_dirty_decay_ms(config::je_dirty_decay_ms);
        } else {
            doris::JemallocControl::je_reset_all_arena_dirty_decay_ms(0);
        }
#endif
    } while (true);
}

void Daemon::cache_adjust_capacity_thread() {
    do {
        std::unique_lock<std::mutex> l(doris::GlobalMemoryArbitrator::cache_adjust_capacity_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::GlobalMemoryArbitrator::cache_adjust_capacity_notify.load(
                       std::memory_order_relaxed)) {
            doris::GlobalMemoryArbitrator::cache_adjust_capacity_cv.wait_for(
                    l, std::chrono::milliseconds(100));
        }
        double adjust_weighted = std::min<double>(
                GlobalMemoryArbitrator::last_periodic_refreshed_cache_capacity_adjust_weighted,
                GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted);
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }

        Defer defer {[&]() {
            doris::GlobalMemoryArbitrator::cache_adjust_capacity_notify.store(
                    false, std::memory_order_relaxed);
        }};
        if (config::disable_memory_gc) {
            continue;
        }
        if (GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted ==
            adjust_weighted) {
            LOG(INFO) << fmt::format(
                    "[MemoryGC] adjust cache capacity end, adjust_weighted {} has not been "
                    "modified.",
                    adjust_weighted);
            continue;
        }
        std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");
        auto freed_mem = CacheManager::instance()->for_each_cache_refresh_capacity(adjust_weighted,
                                                                                   profile.get());
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] adjust cache capacity end, free memory {}, details: {}",
                PrettyPrinter::print(freed_mem, TUnit::BYTES), ss.str());
        GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted = adjust_weighted;
    } while (true);
}

void Daemon::cache_prune_stale_thread() {
    int32_t interval = config::cache_periodic_prune_stale_sweep_sec;
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
        if (config::cache_periodic_prune_stale_sweep_sec <= 0) {
            LOG(WARNING) << "config of cache clean interval is: [" << interval
                         << "], it means the cache prune stale thread is disabled, will wait 3s "
                            "and check again.";
            interval = 3;
            continue;
        }
        if (config::disable_memory_gc) {
            continue;
        }
        CacheManager::instance()->for_each_cache_prune_stale();
        interval = config::cache_periodic_prune_stale_sweep_sec;
    }
}

void Daemon::be_proc_monitor_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::be_proc_monitor_interval_ms))) {
        LOG(INFO) << "log be thread num, " << BeProcMonitor::get_be_thread_info();
    }
}

void Daemon::calculate_workload_group_metrics_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::workload_group_metrics_interval_ms))) {
        ExecEnv::GetInstance()->workload_group_mgr()->refresh_workload_group_metrics();
    }
}

void Daemon::start() {
    Status st;
    st = Thread::create(
            "Daemon", "tcmalloc_gc_thread", [this]() { this->tcmalloc_gc_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_maintenance_thread", [this]() { this->memory_maintenance_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memtable_memory_refresh_thread",
            [this]() { this->memtable_memory_refresh_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;

    if (config::enable_metric_calculator) {
        st = Thread::create(
                "Daemon", "calculate_metrics_thread",
                [this]() { this->calculate_metrics_thread(); }, &_threads.emplace_back());
        CHECK(st.ok()) << st;
    }
    st = Thread::create(
            "Daemon", "je_reset_dirty_decay_thread",
            [this]() { this->je_reset_dirty_decay_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "cache_adjust_capacity_thread",
            [this]() { this->cache_adjust_capacity_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "cache_prune_stale_thread", [this]() { this->cache_prune_stale_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "query_runtime_statistics_thread",
            [this]() { this->report_runtime_query_statistics_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;

    if (config::enable_be_proc_monitor) {
        st = Thread::create(
                "Daemon", "be_proc_monitor_thread", [this]() { this->be_proc_monitor_thread(); },
                &_threads.emplace_back());
    }
    CHECK(st.ok()) << st;

    st = Thread::create(
            "Daemon", "workload_group_metrics",
            [this]() { this->calculate_workload_group_metrics_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
}

void Daemon::stop() {
    LOG(INFO) << "Doris daemon is stopping.";
    if (_stop_background_threads_latch.count() == 0) {
        LOG(INFO) << "Doris daemon stop returned since no bg threads latch.";
        return;
    }
    _stop_background_threads_latch.count_down();
    for (auto&& t : _threads) {
        if (t) {
            t->join();
        }
    }
    LOG(INFO) << "Doris daemon stopped after background threads are joined.";
}

} // namespace doris
