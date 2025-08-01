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

package org.apache.doris.qe;

import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.metrics.EventSwitchParser;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.GroupCommitBlockSink;
import org.apache.doris.qe.VariableMgr.VarAttr;
import org.apache.doris.thrift.TGroupCommitMode;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.thrift.TSerdeDialect;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TConfiguration;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.security.InvalidParameterException;
import java.security.SecureRandom;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * System variable.
 **/
public class SessionVariable implements Serializable, Writable {
    public static final Logger LOG = LogManager.getLogger(SessionVariable.class);

    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String LOCAL_EXCHANGE_FREE_BLOCKS_LIMIT = "local_exchange_free_blocks_limit";
    public static final String SCAN_QUEUE_MEM_LIMIT = "scan_queue_mem_limit";
    public static final String NUM_SCANNER_THREADS = "num_scanner_threads";
    public static final String MIN_SCANNER_CONCURRENCY = "min_scanner_concurrnency";
    public static final String MIN_SCAN_SCHEDULER_CONCURRENCY = "min_scan_scheduler_concurrency";
    public static final String QUERY_TIMEOUT = "query_timeout";
    public static final String ANALYZE_TIMEOUT = "analyze_timeout";

    public static final String INTERNAL_CACHE_HOT_SPOT_TIMEOUT = "cache_hot_spot_insert_timeout_second";

    public static final String MAX_EXECUTION_TIME = "max_execution_time";
    public static final String INSERT_TIMEOUT = "insert_timeout";
    public static final String ENABLE_PROFILE = "enable_profile";
    public static final String RPC_VERBOSE_PROFILE_MAX_INSTANCE_COUNT = "rpc_verbose_profile_max_instance_count";
    public static final String AUTO_PROFILE_THRESHOLD_MS = "auto_profile_threshold_ms";
    public static final String SQL_MODE = "sql_mode";
    public static final String WORKLOAD_VARIABLE = "workload_group";
    public static final String RESOURCE_VARIABLE = "resource_group";
    public static final String AUTO_COMMIT = "autocommit";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String TX_READ_ONLY = "tx_read_only";
    public static final String TRANSACTION_READ_ONLY = "transaction_read_only";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";
    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String CHARACTER_SET_SERVER = "character_set_server";
    public static final String COLLATION_CONNECTION = "collation_connection";
    public static final String COLLATION_DATABASE = "collation_database";
    public static final String COLLATION_SERVER = "collation_server";
    public static final String SQL_AUTO_IS_NULL = "sql_auto_is_null";
    public static final String SQL_SELECT_LIMIT = "sql_select_limit";
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";
    public static final String AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
    public static final String QUERY_CACHE_TYPE = "query_cache_type";
    public static final String INTERACTIVE_TIMTOUT = "interactive_timeout";
    public static final String WAIT_TIMEOUT = "wait_timeout";
    public static final String NET_WRITE_TIMEOUT = "net_write_timeout";
    public static final String NET_READ_TIMEOUT = "net_read_timeout";
    public static final String TIME_ZONE = "time_zone";
    public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
    public static final String NET_BUFFER_LENGTH = "net_buffer_length";
    public static final String HAVE_QUERY_CACHE =  "have_query_cache";
    // mem limit can't smaller than bufferpool's default page size
    public static final int MIN_EXEC_MEM_LIMIT = 2097152;
    public static final String BATCH_SIZE = "batch_size";
    public static final String BROKER_LOAD_BATCH_SIZE = "broker_load_batch_size";
    public static final String DISABLE_STREAMING_PREAGGREGATIONS = "disable_streaming_preaggregations";
    public static final String ENABLE_DISTINCT_STREAMING_AGGREGATION = "enable_distinct_streaming_aggregation";
    public static final String DISABLE_COLOCATE_PLAN = "disable_colocate_plan";
    public static final String ENABLE_BUCKET_SHUFFLE_JOIN = "enable_bucket_shuffle_join";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String PARALLEL_PIPELINE_TASK_NUM = "parallel_pipeline_task_num";
    public static final String PROFILE_LEVEL = "profile_level";
    public static final String MAX_INSTANCE_NUM = "max_instance_num";
    public static final String DML_PLAN_RETRY_TIMES = "DML_PLAN_RETRY_TIMES";
    public static final String ENABLE_INSERT_STRICT = "enable_insert_strict";
    public static final String INSERT_MAX_FILTER_RATIO = "insert_max_filter_ratio";

    public static final String ENABLE_SERVER_SIDE_PREPARED_STATEMENT = "enable_server_side_prepared_statement";
    public static final String MAX_PREPARED_STMT_COUNT = "max_prepared_stmt_count";
    public static final String ENABLE_GROUP_COMMIT_FULL_PREPARE = "enable_group_commit_full_prepare";
    public static final String PREFER_JOIN_METHOD = "prefer_join_method";

    public static final String ENABLE_FOLD_CONSTANT_BY_BE = "enable_fold_constant_by_be";
    public static final String DEBUG_SKIP_FOLD_CONSTANT = "debug_skip_fold_constant";

    public static final String ENABLE_REWRITE_ELEMENT_AT_TO_SLOT = "enable_rewrite_element_at_to_slot";
    public static final String ENABLE_ODBC_TRANSCATION = "enable_odbc_transcation";
    public static final String ENABLE_BINARY_SEARCH_FILTERING_PARTITIONS = "enable_binary_search_filtering_partitions";
    public static final String ENABLE_SQL_CACHE = "enable_sql_cache";
    public static final String ENABLE_QUERY_CACHE = "enable_query_cache";
    public static final String QUERY_CACHE_FORCE_REFRESH = "query_cache_force_refresh";
    public static final String QUERY_CACHE_ENTRY_MAX_BYTES = "query_cache_entry_max_bytes";
    public static final String QUERY_CACHE_ENTRY_MAX_ROWS = "query_cache_entry_max_rows";

    public static final String ENABLE_COST_BASED_JOIN_REORDER = "enable_cost_based_join_reorder";

    // if set to true, some of stmt will be forwarded to master FE to get result
    public static final String FORWARD_TO_MASTER = "forward_to_master";
    // user can set instance num after exchange, no need to be equal to nums of before exchange
    public static final String PARALLEL_EXCHANGE_INSTANCE_NUM = "parallel_exchange_instance_num";
    public static final String SHOW_HIDDEN_COLUMNS = "show_hidden_columns";
    public static final String USE_V2_ROLLUP = "use_v2_rollup";
    public static final String REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL = "rewrite_count_distinct_to_bitmap_hll";
    public static final String EVENT_SCHEDULER = "event_scheduler";
    public static final String STORAGE_ENGINE = "storage_engine";
    // Compatible with datagrip mysql
    public static final String DEFAULT_STORAGE_ENGINE = "default_storage_engine";
    public static final String DEFAULT_TMP_STORAGE_ENGINE = "default_tmp_storage_engine";

    // Compatible with  mysql
    public static final String PROFILLING = "profiling";

    public static final String DIV_PRECISION_INCREMENT = "div_precision_increment";

    // see comment of `doris_max_scan_key_num` and `max_pushdown_conditions_per_column` in BE config
    public static final String MAX_SCAN_KEY_NUM = "max_scan_key_num";
    public static final String MAX_PUSHDOWN_CONDITIONS_PER_COLUMN = "max_pushdown_conditions_per_column";

    // when true, the partition column must be set to NOT NULL.
    public static final String ALLOW_PARTITION_COLUMN_NULLABLE = "allow_partition_column_nullable";

    public static final String FORCE_SORT_ALGORITHM = "force_sort_algorithm";
    public static final String FULL_SORT_MAX_BUFFERED_BYTES = "full_sort_max_buffered_bytes";

    // runtime filter run mode
    public static final String RUNTIME_FILTER_MODE = "runtime_filter_mode";
    // Size in bytes of Bloom Filters used for runtime filters. Actual size of filter will
    // be rounded up to the nearest power of two.
    public static final String RUNTIME_BLOOM_FILTER_SIZE = "runtime_bloom_filter_size";
    // Minimum runtime bloom filter size, in bytes
    public static final String RUNTIME_BLOOM_FILTER_MIN_SIZE = "runtime_bloom_filter_min_size";
    // Maximum runtime bloom filter size, in bytes
    public static final String RUNTIME_BLOOM_FILTER_MAX_SIZE = "runtime_bloom_filter_max_size";
    public static final String USE_RF_DEFAULT = "use_rf_default";
    // Time in ms to wait until runtime filters are delivered.
    public static final String RUNTIME_FILTER_WAIT_TIME_MS = "runtime_filter_wait_time_ms";
    public static final String runtime_filter_wait_infinitely = "runtime_filter_wait_infinitely";

    // Maximum number of bloom runtime filters allowed per query
    public static final String RUNTIME_FILTERS_MAX_NUM = "runtime_filters_max_num";
    // Runtime filter type used, For testing, Corresponds to TRuntimeFilterType
    public static final String RUNTIME_FILTER_TYPE = "runtime_filter_type";
    // if the right table is greater than this value in the hash join,  we will ignore IN filter
    public static final String RUNTIME_FILTER_MAX_IN_NUM = "runtime_filter_max_in_num";

    public static final String ENABLE_SYNC_RUNTIME_FILTER_SIZE = "enable_sync_runtime_filter_size";

    public static final String ENABLE_PARALLEL_RESULT_SINK = "enable_parallel_result_sink";

    public static final String HIVE_TEXT_COMPRESSION = "hive_text_compression";

    public static final String READ_CSV_EMPTY_LINE_AS_NULL = "read_csv_empty_line_as_null";

    public static final String BE_NUMBER_FOR_TEST = "be_number_for_test";

    // max ms to wait transaction publish finish when exec insert stmt.
    public static final String INSERT_VISIBLE_TIMEOUT_MS = "insert_visible_timeout_ms";

    public static final String DELETE_WITHOUT_PARTITION = "delete_without_partition";

    public static final String ENABLE_VARIANT_ACCESS_IN_ORIGINAL_PLANNER = "enable_variant_access_in_original_planner";

    // set the default parallelism for send batch when execute InsertStmt operation,
    // if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config,
    // then the coordinator be will use the value of `max_send_batch_parallelism_per_job`
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";

    // turn off all automatic join reorder algorithms
    public static final String DISABLE_JOIN_REORDER = "disable_join_reorder";

    public static final String MAX_JOIN_NUMBER_OF_REORDER = "max_join_number_of_reorder";

    public static final String ENABLE_NEREIDS_DML = "enable_nereids_dml";
    public static final String ENABLE_NEREIDS_DML_WITH_PIPELINE = "enable_nereids_dml_with_pipeline";
    public static final String ENABLE_STRICT_CONSISTENCY_DML = "enable_strict_consistency_dml";

    public static final String ENABLE_BUSHY_TREE = "enable_bushy_tree";

    public static final String MAX_JOIN_NUMBER_BUSHY_TREE = "max_join_number_bushy_tree";
    public static final String ENABLE_PARTITION_TOPN = "enable_partition_topn";
    public static final String PARTITION_TOPN_MAX_PARTITIONS = "partition_topn_max_partitions";
    public static final String PARTITION_TOPN_PER_PARTITION_ROWS = "partition_topn_pre_partition_rows";

    public static final String GLOBAL_PARTITION_TOPN_THRESHOLD = "global_partition_topn_threshold";

    public static final String ENABLE_INFER_PREDICATE = "enable_infer_predicate";

    public static final long DEFAULT_INSERT_VISIBLE_TIMEOUT_MS = 60_000;

    public static final String ENABLE_VECTORIZED_ENGINE = "enable_vectorized_engine";

    public static final String EXTRACT_WIDE_RANGE_EXPR = "extract_wide_range_expr";

    // If user set a very small value, use this value instead.
    public static final long MIN_INSERT_VISIBLE_TIMEOUT_MS = 1000;

    public static final String ENABLE_PIPELINE_ENGINE = "enable_pipeline_engine";

    public static final String ENABLE_PIPELINE_X_ENGINE = "enable_pipeline_x_engine";

    public static final String ENABLE_SHARED_SCAN = "enable_shared_scan";

    public static final String IGNORE_STORAGE_DATA_DISTRIBUTION = "ignore_storage_data_distribution";

    public static final String USE_SERIAL_EXCHANGE = "use_serial_exchange";

    public static final String ENABLE_PARALLEL_SCAN = "enable_parallel_scan";

    // Limit the max count of scanners to prevent generate too many scanners.
    public static final String PARALLEL_SCAN_MAX_SCANNERS_COUNT = "parallel_scan_max_scanners_count";

    // Avoid splitting small segments, each scanner should scan `parallel_scan_min_rows_per_scanner` rows.
    public static final String PARALLEL_SCAN_MIN_ROWS_PER_SCANNER = "parallel_scan_min_rows_per_scanner";

    public static final String ENABLE_LOCAL_SHUFFLE = "enable_local_shuffle";

    public static final String FORCE_TO_LOCAL_SHUFFLE = "force_to_local_shuffle";

    public static final String ENABLE_LOCAL_MERGE_SORT = "enable_local_merge_sort";

    public static final String ENABLE_SHARED_EXCHANGE_SINK_BUFFER = "enable_shared_exchange_sink_buffer";

    public static final String ENABLE_AGG_STATE = "enable_agg_state";

    public static final String ENABLE_RPC_OPT_FOR_PIPELINE = "enable_rpc_opt_for_pipeline";

    public static final String ENABLE_SINGLE_DISTINCT_COLUMN_OPT = "enable_single_distinct_column_opt";

    public static final String CPU_RESOURCE_LIMIT = "cpu_resource_limit";

    public static final String CLOUD_ENABLE_MULTI_CLUSTER_SYNC_LOAD = "enable_multi_cluster_sync_load";

    public static final String ENABLE_PARALLEL_OUTFILE = "enable_parallel_outfile";

    public static final String SQL_QUOTE_SHOW_CREATE = "sql_quote_show_create";

    public static final String RETURN_OBJECT_DATA_AS_BINARY = "return_object_data_as_binary";

    public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";

    public static final String AUTO_BROADCAST_JOIN_THRESHOLD = "auto_broadcast_join_threshold";

    public static final String PARALLEL_PREPARE_THRESHOLD = "parallel_prepare_threshold";

    public static final String ENABLE_PROJECTION = "enable_projection";

    public static final String ENABLE_SHORT_CIRCUIT_QUERY = "enable_short_circuit_query";

    public static final String ENABLE_SHORT_CIRCUIT_QUERY_ACCESS_COLUMN_STORE
                    = "enable_short_circuit_query_access_column_store";

    public static final String CHECK_OVERFLOW_FOR_DECIMAL = "check_overflow_for_decimal";

    public static final String DECIMAL_OVERFLOW_SCALE = "decimal_overflow_scale";

    public static final String TRIM_TAILING_SPACES_FOR_EXTERNAL_TABLE_QUERY
            = "trim_tailing_spaces_for_external_table_query";

    public static final String ENABLE_DPHYP_OPTIMIZER = "enable_dphyp_optimizer";
    public static final String DPHYPER_LIMIT = "dphyper_limit";
    public static final String ENABLE_LEFT_ZIG_ZAG = "enable_left_zig_zag";
    public static final String ENABLE_HBO_OPTIMIZATION = "enable_hbo_optimization";
    public static final String ENABLE_HBO_INFO_COLLECTION = "enable_hbo_info_collection";
    public static final String ENABLE_HBO_NONSTRICT_MATCHING_MODE = "enable_hbo_nonstrict_matching_mode";
    public static final String HBO_RFSAFE_THRESHOLD = "hbo_rfsafe_threshold";
    public static final String HBO_ROW_MATCHING_THRESHOLD = "hbo_row_matching_threshold";
    public static final String HBO_SKEW_RATIO_THRESHOLD = "hbo_skew_ratio_threshold";
    public static final String NTH_OPTIMIZED_PLAN = "nth_optimized_plan";

    public static final String ENABLE_NEREIDS_PLANNER = "enable_nereids_planner";
    public static final String ENABLE_NEREIDS_DISTRIBUTE_PLANNER = "enable_nereids_distribute_planner";
    public static final String DISABLE_NEREIDS_RULES = "disable_nereids_rules";
    public static final String ENABLE_NEREIDS_RULES = "enable_nereids_rules";
    public static final String ENABLE_VISITOR_REWRITER_DEPTH_THRESHOLD = "enable_visitor_rewriter_depth_threshold";
    public static final String DISABLE_NEREIDS_EXPRESSION_RULES = "disable_nereids_expression_rules";
    public static final String ENABLE_FALLBACK_TO_ORIGINAL_PLANNER = "enable_fallback_to_original_planner";
    public static final String ENABLE_NEREIDS_TIMEOUT = "enable_nereids_timeout";
    public static final String NEREIDS_TIMEOUT_SECOND = "nereids_timeout_second";

    public static final String FORBID_UNKNOWN_COLUMN_STATS = "forbid_unknown_col_stats";
    public static final String BROADCAST_RIGHT_TABLE_SCALE_FACTOR = "broadcast_right_table_scale_factor";
    public static final String LEFT_SEMI_OR_ANTI_PROBE_FACTOR = "left_semi_or_anti_probe_factor";
    public static final String BROADCAST_ROW_COUNT_LIMIT = "broadcast_row_count_limit";

    // percentage of EXEC_MEM_LIMIT
    public static final String BROADCAST_HASHTABLE_MEM_LIMIT_PERCENTAGE = "broadcast_hashtable_mem_limit_percentage";

    public static final String REWRITE_OR_TO_IN_PREDICATE_THRESHOLD = "rewrite_or_to_in_predicate_threshold";

    public static final String NEREIDS_STAR_SCHEMA_SUPPORT = "nereids_star_schema_support";

    public static final String NEREIDS_CBO_PENALTY_FACTOR = "nereids_cbo_penalty_factor";
    public static final String ENABLE_NEREIDS_TRACE = "enable_nereids_trace";
    public static final String ENABLE_EXPR_TRACE = "enable_expr_trace";

    public static final String ENABLE_DPHYP_TRACE = "enable_dphyp_trace";

    public static final String ENABLE_FOLD_NONDETERMINISTIC_FN = "enable_fold_nondeterministic_fn";

    public static final String ENABLE_RUNTIME_FILTER_PRUNE =
            "enable_runtime_filter_prune";

    public static final String ENABLE_RUNTIME_FILTER_PARTITION_PRUNE =
            "enable_runtime_filter_partition_prune";

    static final String SESSION_CONTEXT = "session_context";

    public static final String DEFAULT_ORDER_BY_LIMIT = "default_order_by_limit";

    public static final String ENABLE_SINGLE_REPLICA_INSERT = "enable_single_replica_insert";

    public static final String ENABLE_FAST_ANALYZE_INSERT_INTO_VALUES = "enable_fast_analyze_into_values";

    public static final String ENABLE_FUNCTION_PUSHDOWN = "enable_function_pushdown";

    public static final String ENABLE_EXT_FUNC_PRED_PUSHDOWN = "enable_ext_func_pred_pushdown";

    public static final String ENABLE_COMMON_EXPR_PUSHDOWN = "enable_common_expr_pushdown";

    public static final String FRAGMENT_TRANSMISSION_COMPRESSION_CODEC = "fragment_transmission_compression_codec";

    public static final String ENABLE_LOCAL_EXCHANGE = "enable_local_exchange";

    public static final String SKIP_STORAGE_ENGINE_MERGE = "skip_storage_engine_merge";

    public static final String SKIP_DELETE_PREDICATE = "skip_delete_predicate";

    public static final String SKIP_DELETE_SIGN = "skip_delete_sign";

    public static final String SKIP_DELETE_BITMAP = "skip_delete_bitmap";

    public static final String SKIP_MISSING_VERSION = "skip_missing_version";

    public static final String SKIP_BAD_TABLET = "skip_bad_tablet";

    public static final String ENABLE_PUSH_DOWN_NO_GROUP_AGG = "enable_push_down_no_group_agg";

    public static final String ENABLE_CBO_STATISTICS = "enable_cbo_statistics";

    public static final String ENABLE_SAVE_STATISTICS_SYNC_JOB = "enable_save_statistics_sync_job";

    public static final String ENABLE_ELIMINATE_SORT_NODE = "enable_eliminate_sort_node";

    public static final String NEREIDS_TRACE_EVENT_MODE = "nereids_trace_event_mode";

    public static final String PARTITION_PRUNING_EXPAND_THRESHOLD = "partition_pruning_expand_threshold";

    public static final String ENABLE_SHARE_HASH_TABLE_FOR_BROADCAST_JOIN
            = "enable_share_hash_table_for_broadcast_join";

    // support unicode in label, table, column, common name check
    public static final String ENABLE_UNICODE_NAME_SUPPORT = "enable_unicode_name_support";

    public static final String GROUP_CONCAT_MAX_LEN = "group_concat_max_len";
    public static final String USE_ONE_PHASE_AGG_FOR_GROUP_CONCAT_WITH_ORDER
            = "use_one_phase_agg_for_group_concat_with_order";

    public static final String ENABLE_TWO_PHASE_READ_OPT = "enable_two_phase_read_opt";
    public static final String TOPN_OPT_LIMIT_THRESHOLD = "topn_opt_limit_threshold";
    public static final String TOPN_FILTER_RATIO = "topn_filter_ratio";
    public static final String ENABLE_SNAPSHOT_POINT_QUERY = "enable_snapshot_point_query";

    public static final String ENABLE_FILE_CACHE = "enable_file_cache";

    public static final String DISABLE_FILE_CACHE = "disable_file_cache";

    public static final String FILE_CACHE_BASE_PATH = "file_cache_base_path";

    public static final String ENABLE_INVERTED_INDEX_QUERY = "enable_inverted_index_query";

    public static final String ENABLE_COMMON_EXPR_PUSHDOWN_FOR_INVERTED_INDEX
            = "enable_common_expr_pushdown_for_inverted_index";

    public static final String ENABLE_PUSHDOWN_COUNT_ON_INDEX = "enable_count_on_index_pushdown";
    public static final String ENABLE_NO_NEED_READ_DATA_OPT = "enable_no_need_read_data_opt";

    public static final String GROUP_BY_AND_HAVING_USE_ALIAS_FIRST = "group_by_and_having_use_alias_first";
    public static final String DROP_TABLE_IF_CTAS_FAILED = "drop_table_if_ctas_failed";

    public static final String MAX_TABLE_COUNT_USE_CASCADES_JOIN_REORDER = "max_table_count_use_cascades_join_reorder";
    public static final int MIN_JOIN_REORDER_TABLE_COUNT = 2;

    public static final String JOIN_REORDER_TIME_LIMIT = "join_order_time_limit";
    public static final String SHOW_USER_DEFAULT_ROLE = "show_user_default_role";

    public static final String ENABLE_PAGE_CACHE = "enable_page_cache";

    public static final String MINIDUMP_PATH = "minidump_path";

    public static final String TRACE_NEREIDS = "trace_nereids";

    public static final String PLAN_NEREIDS_DUMP = "plan_nereids_dump";

    public static final String DUMP_NEREIDS_MEMO = "dump_nereids_memo";

    // fix replica to query. If num = 1, query the smallest replica, if 2 is the second smallest replica.
    public static final String USE_FIX_REPLICA = "use_fix_replica";

    public static final String DRY_RUN_QUERY = "dry_run_query";

    // Split size for ExternalFileScanNode. Default value 0 means use the block size of HDFS/S3.
    public static final String FILE_SPLIT_SIZE = "file_split_size";

    public static final String NUM_PARTITIONS_IN_BATCH_MODE = "num_partitions_in_batch_mode";

    public static final String NUM_FILES_IN_BATCH_MODE = "num_files_in_batch_mode";

    public static final String FETCH_SPLITS_MAX_WAIT_TIME = "fetch_splits_max_wait_time_ms";

    /**
     * use insert stmt as the unified backend for all loads
     */
    public static final String ENABLE_UNIFIED_LOAD = "enable_unified_load";

    public static final String ENABLE_ORDERED_SCAN_RANGE_LOCATIONS = "enable_ordered_scan_range_locations";

    public static final String ENABLE_PARQUET_LAZY_MAT = "enable_parquet_lazy_materialization";

    public static final String ENABLE_ORC_LAZY_MAT = "enable_orc_lazy_materialization";

    public static final String ORC_TINY_STRIPE_THRESHOLD_BYTES = "orc_tiny_stripe_threshold_bytes";

    public static final String ORC_ONCE_MAX_READ_BYTES = "orc_once_max_read_bytes";

    public static final String ORC_MAX_MERGE_DISTANCE_BYTES = "orc_max_merge_distance_bytes";

    public static final String ENABLE_PARQUET_FILTER_BY_MIN_MAX = "enable_parquet_filter_by_min_max";

    public static final String ENABLE_ORC_FILTER_BY_MIN_MAX = "enable_orc_filter_by_min_max";

    public static final String CHECK_ORC_INIT_SARGS_SUCCESS = "check_orc_init_sargs_success";

    public static final String INLINE_CTE_REFERENCED_THRESHOLD = "inline_cte_referenced_threshold";

    public static final String ENABLE_CTE_MATERIALIZE = "enable_cte_materialize";

    public static final String ENABLE_ANALYZE_COMPLEX_TYPE_COLUMN = "enable_analyze_complex_type_column";

    public static final String EXTERNAL_TABLE_ANALYZE_PART_NUM = "external_table_analyze_part_num";

    public static final String ENABLE_STRONG_CONSISTENCY = "enable_strong_consistency_read";

    public static final String GROUP_COMMIT = "group_commit";

    public static final String ENABLE_PREPARED_STMT_AUDIT_LOG = "enable_prepared_stmt_audit_log";

    public static final String PARALLEL_SYNC_ANALYZE_TASK_NUM = "parallel_sync_analyze_task_num";

    public static final String TRUNCATE_CHAR_OR_VARCHAR_COLUMNS = "truncate_char_or_varchar_columns";

    public static final String CBO_CPU_WEIGHT = "cbo_cpu_weight";

    public static final String CBO_MEM_WEIGHT = "cbo_mem_weight";

    public static final String CBO_NET_WEIGHT = "cbo_net_weight";

    public static final String ROUND_PRECISE_DECIMALV2_VALUE = "round_precise_decimalv2_value";

    public static final String JDBC_CLICKHOUSE_QUERY_FINAL = "jdbc_clickhouse_query_final";

    public static final String ENABLE_JDBC_ORACLE_NULL_PREDICATE_PUSH_DOWN
            = "enable_jdbc_oracle_null_predicate_push_down";

    public static final String ENABLE_JDBC_CAST_PREDICATE_PUSH_DOWN = "enable_jdbc_cast_predicate_push_down";

    public static final String ENABLE_MEMTABLE_ON_SINK_NODE =
            "enable_memtable_on_sink_node";

    public static final String LOAD_STREAM_PER_NODE = "load_stream_per_node";

    public static final String ENABLE_UNIQUE_KEY_PARTIAL_UPDATE = "enable_unique_key_partial_update";

    public static final String PARTIAL_UPDATE_NEW_KEY_BEHAVIOR = "partial_update_new_key_behavior";

    public static final String INVERTED_INDEX_CONJUNCTION_OPT_THRESHOLD = "inverted_index_conjunction_opt_threshold";
    public static final String INVERTED_INDEX_MAX_EXPANSIONS = "inverted_index_max_expansions";

    public static final String INVERTED_INDEX_SKIP_THRESHOLD = "inverted_index_skip_threshold";
    // compatible read for inverted index between x86 and arm,
    // used for cross-platform (x86/arm) inverted index compatibility
    // may removed in the future
    public static final String INVERTED_INDEX_COMPATIBLE_READ = "inverted_index_compatible_read";

    public static final String AUTO_ANALYZE_START_TIME = "auto_analyze_start_time";

    public static final String AUTO_ANALYZE_END_TIME = "auto_analyze_end_time";

    public static final String SQL_DIALECT = "sql_dialect";

    public static final String SERDE_DIALECT = "serde_dialect";

    public static final String EXPAND_RUNTIME_FILTER_BY_INNER_JION = "expand_runtime_filter_by_inner_join";

    public static final String TEST_QUERY_CACHE_HIT = "test_query_cache_hit";

    public static final String ENABLE_AUTO_ANALYZE = "enable_auto_analyze";

    public static final String FORCE_SAMPLE_ANALYZE = "force_sample_analyze";

    public static final String ENABLE_AUTO_ANALYZE_INTERNAL_CATALOG = "enable_auto_analyze_internal_catalog";

    public static final String ENABLE_PARTITION_ANALYZE = "enable_partition_analyze";

    public static final String AUTO_ANALYZE_TABLE_WIDTH_THRESHOLD = "auto_analyze_table_width_threshold";

    public static final String ENABLE_DECIMAL256 = "enable_decimal256";

    public static final String STATS_INSERT_MERGE_ITEM_COUNT = "stats_insert_merge_item_count";

    public static final String HUGE_TABLE_DEFAULT_SAMPLE_ROWS = "huge_table_default_sample_rows";
    public static final String HUGE_TABLE_LOWER_BOUND_SIZE_IN_BYTES = "huge_table_lower_bound_size_in_bytes";
    public static final String PARTITION_SAMPLE_COUNT = "partition_sample_count";
    public static final String PARTITION_SAMPLE_ROW_COUNT = "partition_sample_row_count";
    public static final String FETCH_HIVE_ROW_COUNT_SYNC = "fetch_hive_row_count_sync";

    // for spill to disk
    public static final String ENABLE_SPILL = "enable_spill";
    public static final String ENABLE_FORCE_SPILL = "enable_force_spill";
    public static final String ENABLE_RESERVE_MEMORY = "enable_reserve_memory";
    public static final String SPILL_MIN_REVOCABLE_MEM = "spill_min_revocable_mem";
    public static final String SPILL_SORT_MEM_LIMIT = "spill_sort_mem_limit";
    // spill_sort_batch_bytes controls the memory size of a sindle block data of spill sort.
    public static final String SPILL_SORT_BATCH_BYTES = "spill_sort_batch_bytes";
    public static final String SPILL_AGGREGATION_PARTITION_COUNT = "spill_aggregation_partition_count";
    public static final String SPILL_STREAMING_AGG_MEM_LIMIT = "spill_streaming_agg_mem_limit";
    public static final String SPILL_HASH_JOIN_PARTITION_COUNT = "spill_hash_join_partition_count";
    public static final String SPILL_REVOCABLE_MEMORY_HIGH_WATERMARK_PERCENT =
            "spill_revocable_memory_high_watermark_percent";
    public static final String DATA_QUEUE_MAX_BLOCKS = "data_queue_max_blocks";
    public static final String LOW_MEMORY_MODE_BUFFER_LIMIT = "low_memory_mode_buffer_limit";
    public static final String DUMP_HEAP_PROFILE_WHEN_MEM_LIMIT_EXCEEDED = "dump_heap_profile_when_mem_limit_exceeded";

    public static final String GENERATE_STATS_FACTOR = "generate_stats_factor";

    public static final String HUGE_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS
            = "huge_table_auto_analyze_interval_in_millis";

    public static final String EXTERNAL_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS
            = "external_table_auto_analyze_interval_in_millis";

    public static final String TABLE_STATS_HEALTH_THRESHOLD
            = "table_stats_health_threshold";

    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE
            = "enable_materialized_view_rewrite";

    public static final String ENABLE_DML_MATERIALIZED_VIEW_REWRITE
            = "enable_dml_materialized_view_rewrite";

    public static final String ENABLE_DML_MATERIALIZED_VIEW_REWRITE_WHEN_BASE_TABLE_UNAWARENESS
            = "enable_dml_materialized_view_rewrite_when_base_table_unawareness";

    public static final String ALLOW_MODIFY_MATERIALIZED_VIEW_DATA
            = "allow_modify_materialized_view_data";

    public static final String ENABLE_MATERIALIZED_VIEW_REWRITE_WHEN_BASE_TABLE_UNAWARENESS
            = "materialized_view_rewrite_enable_contain_external_table";

    public static final String MATERIALIZED_VIEW_REWRITE_SUCCESS_CANDIDATE_NUM
            = "materialized_view_rewrite_success_candidate_num";

    public static final String ENABLE_MATERIALIZED_VIEW_UNION_REWRITE
            = "enable_materialized_view_union_rewrite";

    public static final String ENABLE_MATERIALIZED_VIEW_NEST_REWRITE
            = "enable_materialized_view_nest_rewrite";

    public static final String ENABLE_SYNC_MV_COST_BASED_REWRITE
            = "enable_sync_mv_cost_based_rewrite";

    public static final String MATERIALIZED_VIEW_REWRITE_DURATION_THRESHOLD_MS
            = "materialized_view_rewrite_duration_threshold_ms";

    public static final String MATERIALIZED_VIEW_RELATION_MAPPING_MAX_COUNT
            = "materialized_view_relation_mapping_max_count";

    public static final String CREATE_TABLE_PARTITION_MAX_NUM
            = "create_table_partition_max_num";

    public static final String ENABLE_PUSHDOWN_MINMAX_ON_UNIQUE = "enable_pushdown_minmax_on_unique";

    public static final String HIVE_PARQUET_USE_COLUMN_NAMES = "hive_parquet_use_column_names";

    public static final String HIVE_ORC_USE_COLUMN_NAMES = "hive_orc_use_column_names";

    public static final String KEEP_CARRIAGE_RETURN = "keep_carriage_return";

    public static final String ENABLE_PUSHDOWN_STRING_MINMAX = "enable_pushdown_string_minmax";

    // When set use fix replica = true, the fixed replica maybe bad, try to use the health one if
    // this session variable is set to true.
    public static final String FALLBACK_OTHER_REPLICA_WHEN_FIXED_CORRUPT = "fallback_other_replica_when_fixed_corrupt";

    public static final String DESCRIBE_EXTEND_VARIANT_COLUMN = "describe_extend_variant_column";

    public static final String FORCE_JNI_SCANNER = "force_jni_scanner";

    public static final String ENABLE_COUNT_PUSH_DOWN_FOR_EXTERNAL_TABLE = "enable_count_push_down_for_external_table";

    public static final String SHOW_ALL_FE_CONNECTION = "show_all_fe_connection";

    public static final String MAX_MSG_SIZE_OF_RESULT_RECEIVER = "max_msg_size_of_result_receiver";

    public static final String BYPASS_WORKLOAD_GROUP = "bypass_workload_group";

    public static final String QUERY_SLOT_COUNT = "query_slot_count";

    public static final String MAX_COLUMN_READER_NUM = "max_column_reader_num";

    public static final String USE_MAX_LENGTH_OF_VARCHAR_IN_CTAS = "use_max_length_of_varchar_in_ctas";

    public static final String ENABLE_ES_PARALLEL_SCROLL = "enable_es_parallel_scroll";

    public static final String EXCHANGE_MULTI_BLOCKS_BYTE_SIZE = "exchange_multi_blocks_byte_size";

    public static final List<String> DEBUG_VARIABLES = ImmutableList.of(
            SKIP_DELETE_PREDICATE,
            SKIP_DELETE_BITMAP,
            SKIP_DELETE_SIGN,
            SKIP_STORAGE_ENGINE_MERGE,
            SHOW_HIDDEN_COLUMNS
    );

    public static final String ENABLE_STATS = "enable_stats";

    public static final String LIMIT_ROWS_FOR_SINGLE_INSTANCE = "limit_rows_for_single_instance";

    public static final String FETCH_REMOTE_SCHEMA_TIMEOUT_SECONDS = "fetch_remote_schema_timeout_seconds";

    public static final String MAX_FETCH_REMOTE_TABLET_COUNT = "max_fetch_remote_schema_tablet_count";

    public static final String DISABLE_INVERTED_INDEX_V1_FOR_VARIANT = "disable_inverted_index_v1_for_variant";

    // disable variant flatten nested as session variable, default is true,
    // which means disable variant flatten nested when create table
    public static final String DISABLE_VARIANT_FLATTEN_NESTED = "disable_variant_flatten_nested";

    // CLOUD_VARIABLES_BEGIN
    public static final String CLOUD_CLUSTER = "cloud_cluster";
    public static final String DISABLE_EMPTY_PARTITION_PRUNE = "disable_empty_partition_prune";
    public static final String CLOUD_PARTITION_VERSION_CACHE_TTL_MS =
            "cloud_partition_version_cache_ttl_ms";
    // CLOUD_VARIABLES_BEGIN

    public static final String ENABLE_MATCH_WITHOUT_INVERTED_INDEX = "enable_match_without_inverted_index";
    public static final String ENABLE_FALLBACK_ON_MISSING_INVERTED_INDEX = "enable_fallback_on_missing_inverted_index";
    public static final String ENABLE_INVERTED_INDEX_SEARCHER_CACHE = "enable_inverted_index_searcher_cache";
    public static final String ENABLE_INVERTED_INDEX_QUERY_CACHE = "enable_inverted_index_query_cache";

    public static final String IN_LIST_VALUE_COUNT_THRESHOLD = "in_list_value_count_threshold";

    public static final String ENABLE_ADAPTIVE_PIPELINE_TASK_SERIAL_READ_ON_LIMIT =
                                    "enable_adaptive_pipeline_task_serial_read_on_limit";
    public static final String ADAPTIVE_PIPELINE_TASK_SERIAL_READ_ON_LIMIT =
                                    "adaptive_pipeline_task_serial_read_on_limit";
    public static final String REQUIRE_SEQUENCE_IN_INSERT = "require_sequence_in_insert";

    public static final String MINIMUM_OPERATOR_MEMORY_REQUIRED_KB = "minimum_operator_memory_required_kb";

    public static final String ENABLE_PHRASE_QUERY_SEQUENYIAL_OPT = "enable_phrase_query_sequential_opt";

    public static final String ENABLE_COOLDOWN_REPLICA_AFFINITY =
            "enable_cooldown_replica_affinity";
    public static final String SKIP_CHECKING_ACID_VERSION_FILE = "skip_checking_acid_version_file";

    public static final String READ_HIVE_JSON_IN_ONE_COLUMN = "read_hive_json_in_one_column";

    /**
     * Inserting overwrite for auto partition table allows creating partition for
     * datas which cannot find partition to overwrite.
     */
    public static final String ENABLE_AUTO_CREATE_WHEN_OVERWRITE = "enable_auto_create_when_overwrite";

    public static final String ENABLE_TEXT_VALIDATE_UTF8 = "enable_text_validate_utf8";

    public static final String ENABLE_SQL_CONVERTOR_FEATURES = "enable_sql_convertor_features";

    public static final String ENABLE_SCHEMA_SCAN_FROM_MASTER_FE = "enable_schema_scan_from_master_fe";

    public static final String SHOW_COLUMN_COMMENT_IN_DESCRIBE = "show_column_comment_in_describe";

    public static final String SQL_CONVERTOR_CONFIG = "sql_convertor_config";

    public static final String PREFER_UDF_OVER_BUILTIN = "prefer_udf_over_builtin";
    public static final String ENABLE_ADD_INDEX_FOR_NEW_DATA = "enable_add_index_for_new_data";

    public static final String SKEW_REWRITE_JOIN_SALT_EXPLODE_FACTOR = "skew_rewrite_join_salt_explode_factor";

    public static final String SKEW_REWRITE_AGG_BUCKET_NUM = "skew_rewrite_agg_bucket_num";

    public static final String HOT_VALUE_THRESHOLD = "hot_value_threshold";

    @VariableMgr.VarAttr(name = HOT_VALUE_THRESHOLD, needForward = true,
            description = {"value 在每百行中的最低出现次数",
                    "The minimum number of occurrences of 'value' per hundred lines"})
    private double hotValueThreshold = 33; // by percentage

    public void setHotValueThreshold(double threshold) {
        this.hotValueThreshold = threshold;
    }

    public static double getHotValueThreshold() {
        if (ConnectContext.get() != null) {
            if (ConnectContext.get().getState().isInternal()) {
                return 0.0;
            } else {
                return ConnectContext.get().getSessionVariable().hotValueThreshold;
            }
        } else {
            return Double.parseDouble(VariableMgr.getDefaultValue(HOT_VALUE_THRESHOLD));
        }
    }

    public static final String ENABLE_STRICT_CAST = "enable_strict_cast";

    /**
     * If set false, user couldn't submit analyze SQL and FE won't allocate any related resources.
     */
    @VariableMgr.VarAttr(name = ENABLE_STATS)
    public  boolean enableStats = true;

    // session origin value
    public Map<SessionVariableField, String> sessionOriginValue = new HashMap<>();
    // check stmt is or not [select /*+ SET_VAR(...)*/ ...]
    // if it is setStmt, we needn't collect session origin value
    public boolean isSingleSetVar = false;

    @VariableMgr.VarAttr(name = EXPAND_RUNTIME_FILTER_BY_INNER_JION)
    public boolean expandRuntimeFilterByInnerJoin = true;

    @VariableMgr.VarAttr(name = "enable_aggregate_cse", needForward = true)
    public boolean enableAggregateCse = true;

    @VariableMgr.VarAttr(name = JDBC_CLICKHOUSE_QUERY_FINAL, needForward = true,
            description = {"是否在查询 ClickHouse JDBC 外部表时，对查询 SQL 添加 FINAL 关键字。",
                    "Whether to add the FINAL keyword to the query SQL when querying ClickHouse JDBC external tables."})
    public boolean jdbcClickhouseQueryFinal = false;

    @VariableMgr.VarAttr(name = ENABLE_JDBC_ORACLE_NULL_PREDICATE_PUSH_DOWN, needForward = true,
            description = {"是否允许将 NULL 谓词下推到 Oracle JDBC 外部表。",
                    "Whether to allow NULL predicates to be pushed down to Oracle JDBC external tables."})
    public boolean enableJdbcOracleNullPredicatePushDown = false;

    @VariableMgr.VarAttr(name = ENABLE_JDBC_CAST_PREDICATE_PUSH_DOWN, needForward = true,
            description = {"是否允许将带有 CAST 表达式的谓词下推到 JDBC 外部表。",
                    "Whether to allow predicates with CAST expressions to be pushed down to JDBC external tables."})
    public boolean enableJdbcCastPredicatePushDown = true;

    @VariableMgr.VarAttr(name = ROUND_PRECISE_DECIMALV2_VALUE, affectQueryResult = true)
    public boolean roundPreciseDecimalV2Value = false;

    @VariableMgr.VarAttr(name = INSERT_VISIBLE_TIMEOUT_MS, needForward = true)
    public long insertVisibleTimeoutMs = DEFAULT_INSERT_VISIBLE_TIMEOUT_MS;

    // max memory used on every backend. Default value to 100G.
    @VariableMgr.VarAttr(name = EXEC_MEM_LIMIT, needForward = true)
    public long maxExecMemByte = 100147483648L;

    @VariableMgr.VarAttr(name = SCAN_QUEUE_MEM_LIMIT, needForward = true,
            description = {"每个 Scan Instance 的 block queue 能够保存多少字节的 block",
                    "How many bytes of block can be saved in the block queue of each Scan Instance"})
    // 100MB
    public long maxScanQueueMemByte = 2147483648L / 20;

    @VariableMgr.VarAttr(name = NUM_SCANNER_THREADS, needForward = true, description = {
            "ScanNode扫描数据的最大并发，默认为0，采用BE的doris_scanner_thread_pool_thread_num",
            "The max threads to read data of ScanNode, "
                    + "default 0, use doris_scanner_thread_pool_thread_num in be.conf"
    })
    public int numScannerThreads = 0;

    @VariableMgr.VarAttr(name = LOCAL_EXCHANGE_FREE_BLOCKS_LIMIT)
    public int localExchangeFreeBlocksLimit = 4;

    @VariableMgr.VarAttr(name = MIN_SCANNER_CONCURRENCY, needForward = true, description = {
        "Scanner 的最小并发度，默认为1", "The min concurrency of Scanner, default 1"
    })
    public int minScannerConcurrency = 1;

    @VariableMgr.VarAttr(name = MIN_SCAN_SCHEDULER_CONCURRENCY, needForward = true, description = {
        "ScanScheduler 的最小并发度，默认值 0 表示使用 Scan 线程池线程数量的两倍", "The min concurrency of ScanScheduler, "
            + "default 0 means use twice the number of Scan thread pool threads"
    })
    public int minScanSchedulerConcurrency = 0;

    // By default, the number of Limit items after OrderBy is changed from 65535 items
    // before v1.2.0 (not included), to return all items by default
    @VariableMgr.VarAttr(name = DEFAULT_ORDER_BY_LIMIT, affectQueryResult = true)
    private long defaultOrderByLimit = -1;

    // query timeout in second.
    @VariableMgr.VarAttr(name = QUERY_TIMEOUT, needForward = true,
            checker = "checkQueryTimeoutValid", setter = "setQueryTimeoutS")
    private int queryTimeoutS = 900;

    // query timeout in second.
    @VariableMgr.VarAttr(name = ANALYZE_TIMEOUT, flag = VariableMgr.GLOBAL, needForward = true)
    public int analyzeTimeoutS = 43200;

    // insert timeout in second
    @VariableMgr.VarAttr(name = INTERNAL_CACHE_HOT_SPOT_TIMEOUT, flag = VariableMgr.GLOBAL, needForward = true)
    public int cacheHotSpotTimeoutS = 300;

    // The global max_execution_time value provides the default for the session value for new connections.
    // The session value applies to SELECT executions executed within the session that include
    // no MAX_EXECUTION_TIME(N) optimizer hint or for which N is 0.
    // https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html
    // So that it is == query timeout in doris
    @VariableMgr.VarAttr(name = MAX_EXECUTION_TIME, checker = "checkMaxExecutionTimeMSValid",
                        setter = "setMaxExecutionTimeMS")
    public int maxExecutionTimeMS = 900000;

    @VariableMgr.VarAttr(name = INSERT_TIMEOUT, needForward = true)
    public int insertTimeoutS = 14400;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = ENABLE_PROFILE, needForward = true)
    public boolean enableProfile = true;

    @VariableMgr.VarAttr(name = RPC_VERBOSE_PROFILE_MAX_INSTANCE_COUNT, needForward = true)
    public int rpcVerboseProfileMaxInstanceCount = 5;

    // When enable_profile is true, profile of queries that costs more than autoProfileThresholdMs
    // will be stored to disk.
    @VariableMgr.VarAttr(name = AUTO_PROFILE_THRESHOLD_MS, needForward = true)
    public int autoProfileThresholdMs = -1;

    @VariableMgr.VarAttr(name = "runtime_filter_prune_for_external", fuzzy = true)
    public boolean runtimeFilterPruneForExternal = true;

    @VariableMgr.VarAttr(name = "runtime_filter_jump_threshold")
    public int runtimeFilterJumpThreshold = 2;

    // using hashset instead of group by + count can improve performance
    //        but may cause rpc failed when cluster has less BE
    // Whether this switch is turned on depends on the BE number
    @VariableMgr.VarAttr(name = ENABLE_SINGLE_DISTINCT_COLUMN_OPT)
    public boolean enableSingleDistinctColumnOpt = false;

    // Set sqlMode to empty string
    @VariableMgr.VarAttr(name = SQL_MODE, needForward = true, affectQueryResult = true)
    public long sqlMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;

    @VariableMgr.VarAttr(name = WORKLOAD_VARIABLE, needForward = true)
    public String workloadGroup = "";

    @VariableMgr.VarAttr(name = BYPASS_WORKLOAD_GROUP, needForward = true, description = {
            "查询是否绕开WorkloadGroup的限制，目前仅支持绕开查询排队的逻辑",
            "whether bypass workload group's limitation, currently only support bypass query queue"})
    public boolean bypassWorkloadGroup = false;

    @VariableMgr.VarAttr(name = QUERY_SLOT_COUNT, needForward = true, checker = "checkQuerySlotCount",
            description = {
                "每个查询占用的slot的数量，workload group的query slot的总数等于设置的最大并发数",
                "Number of slots occupied by each query, the total number of query slots "
                        + "of the workload group equals the maximum number of concurrent requests"})
    public int wgQuerySlotCount = 1;

    public void checkQuerySlotCount(String slotCnt) {
        Long slotCount = Long.valueOf(slotCnt);
        if (slotCount < 1 || slotCount > 1025) {
            throw new InvalidParameterException("query_slot_count should be between 1 and 1024)");
        }
    }

    @VariableMgr.VarAttr(name = MAX_COLUMN_READER_NUM)
    public int maxColumnReaderNum = 20000;

    @VariableMgr.VarAttr(name = RESOURCE_VARIABLE)
    public String resourceGroup = "";

    // this is used to make mysql client happy
    // autocommit is actually a boolean value, but @@autocommit is type of BIGINT.
    // So we need to set convertBoolToLongMethod to make "select @@autocommit" happy.
    @VariableMgr.VarAttr(name = AUTO_COMMIT, convertBoolToLongMethod = "convertBoolToLong")
    public boolean autoCommit = true;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = TX_ISOLATION)
    public String txIsolation = "REPEATABLE-READ";

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TX_READ_ONLY)
    public boolean txReadonly = false;

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TRANSACTION_READ_ONLY)
    public boolean transactionReadonly = false;

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TRANSACTION_ISOLATION)
    public String transactionIsolation = "REPEATABLE-READ";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = CHARACTER_SET_CLIENT)
    public String charsetClient = "utf8mb4";
    @VariableMgr.VarAttr(name = CHARACTER_SET_CONNNECTION)
    public String charsetConnection = "utf8mb4";
    @VariableMgr.VarAttr(name = CHARACTER_SET_RESULTS)
    public String charsetResults = "utf8mb4";
    @VariableMgr.VarAttr(name = CHARACTER_SET_SERVER)
    public String charsetServer = "utf8mb4";
    @VariableMgr.VarAttr(name = COLLATION_CONNECTION)
    public String collationConnection = "utf8mb4_0900_bin";
    @VariableMgr.VarAttr(name = COLLATION_DATABASE)
    public String collationDatabase = "utf8mb4_0900_bin";

    @VariableMgr.VarAttr(name = COLLATION_SERVER)
    public String collationServer = "utf8mb4_0900_bin";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = SQL_AUTO_IS_NULL)
    public boolean sqlAutoIsNull = false;

    @VariableMgr.VarAttr(name = SQL_SELECT_LIMIT, needForward = true, affectQueryResult = true)
    private long sqlSelectLimit = Long.MAX_VALUE;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = MAX_ALLOWED_PACKET)
    public int maxAllowedPacket = 16777216;

    @VariableMgr.VarAttr(name = AUTO_INCREMENT_INCREMENT)
    public int autoIncrementIncrement = 1;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = QUERY_CACHE_TYPE)
    public int queryCacheType = 0;

    // The number of seconds the server waits for activity on an interactive connection before closing it
    @VariableMgr.VarAttr(name = INTERACTIVE_TIMTOUT)
    public int interactiveTimeout = 3600;

    // The number of seconds the server waits for activity on a noninteractive connection before closing it.
    @VariableMgr.VarAttr(name = WAIT_TIMEOUT)
    public int waitTimeoutS = 28800;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_WRITE_TIMEOUT)
    public int netWriteTimeout = 600;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_READ_TIMEOUT)
    public int netReadTimeout = 600;

    // The current time zone
    @VariableMgr.VarAttr(name = TIME_ZONE, needForward = true, affectQueryResult = true)
    public String timeZone = TimeUtils.getSystemTimeZone().getID();

    @VariableMgr.VarAttr(name = PARALLEL_EXCHANGE_INSTANCE_NUM)
    public int exchangeInstanceParallel = 100;

    @VariableMgr.VarAttr(name = SQL_SAFE_UPDATES)
    public int sqlSafeUpdates = 0;

    // only
    @VariableMgr.VarAttr(name = NET_BUFFER_LENGTH, flag = VariableMgr.READ_ONLY)
    public int netBufferLength = 16384;

    @VariableMgr.VarAttr(name = HAVE_QUERY_CACHE, flag = VariableMgr.READ_ONLY)
    public boolean haveQueryCache = false;

    // 4096 minus 16 + 16 bytes padding that in padding pod array
    @VariableMgr.VarAttr(name = BATCH_SIZE, fuzzy = true, checker = "checkBatchSize", needForward = true)
    public int batchSize = 4064;

    // 16352 + 16 + 16 = 16384
    @VariableMgr.VarAttr(name = BROKER_LOAD_BATCH_SIZE, fuzzy = true, checker = "checkBatchSize")
    public int brokerLoadBatchSize = 16352;

    @VariableMgr.VarAttr(name = DISABLE_STREAMING_PREAGGREGATIONS, fuzzy = true)
    public boolean disableStreamPreaggregations = false;

    @VariableMgr.VarAttr(name = ENABLE_DISTINCT_STREAMING_AGGREGATION, fuzzy = true)
    public boolean enableDistinctStreamingAggregation = true;

    @VariableMgr.VarAttr(name = DISABLE_COLOCATE_PLAN)
    public boolean disableColocatePlan = false;

    @VariableMgr.VarAttr(name = ENABLE_BUCKET_SHUFFLE_JOIN, varType = VariableAnnotation.EXPERIMENTAL_ONLINE)
    public boolean enableBucketShuffleJoin = true;

    /**
     * explode function row count enlarge factor.
     */
    @VariableMgr.VarAttr(name = GENERATE_STATS_FACTOR, checker = "checkGenerateStatsFactor",
            setter = "setGenerateStatsFactor")
    public int generateStatsFactor = 5;

    @VariableMgr.VarAttr(name = PREFER_JOIN_METHOD)
    public String preferJoinMethod = "broadcast";

    @VariableMgr.VarAttr(name = FRAGMENT_TRANSMISSION_COMPRESSION_CODEC)
    public String fragmentTransmissionCompressionCodec = "none";

    // whether sync load to other cluster
    @VariableMgr.VarAttr(name = CLOUD_ENABLE_MULTI_CLUSTER_SYNC_LOAD, needForward = true)
    public static boolean cloudEnableMultiClusterSyncLoad = false;

    /*
     * the parallel exec instance num for one Fragment in one BE
     * 1 means disable this feature
     */
    @VariableMgr.VarAttr(name = PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM, needForward = true, fuzzy = false,
                        setter = "setFragmentInstanceNum", varType = VariableAnnotation.DEPRECATED)
    public int parallelExecInstanceNum = 8;

    @VariableMgr.VarAttr(name = PARALLEL_PIPELINE_TASK_NUM, fuzzy = true, needForward = true,
                        setter = "setPipelineTaskNum")
    public int parallelPipelineTaskNum = 0;


    public enum IgnoreSplitType {
        NONE,
        IGNORE_JNI,
        IGNORE_NATIVE
    }

    public static final String IGNORE_SPLIT_TYPE = "ignore_split_type";
    @VariableMgr.VarAttr(name = IGNORE_SPLIT_TYPE,
            checker = "checkIgnoreSplitType",
            options = {"NONE", "IGNORE_JNI", "IGNORE_NATIVE"},
            description = {"忽略指定类型的split", "Ignore splits of the specified type"})
    public String ignoreSplitType = IgnoreSplitType.NONE.toString();

    public static final String USE_CONSISTENT_HASHING_FOR_EXTERNAL_SCAN = "use_consistent_hash_for_external_scan";
    @VariableMgr.VarAttr(name = USE_CONSISTENT_HASHING_FOR_EXTERNAL_SCAN,
            description = {"对外表采用一致性hash的方式做split的分发",
                    "Use consistent hashing to split the appearance for external scan"})
    public boolean useConsistentHashForExternalScan = false;

    @VariableMgr.VarAttr(name = PROFILE_LEVEL, fuzzy = false,
            setter = "setProfileLevel", checker = "checkProfileLevel",
            description = { "查询profile的级别，1表示只收集 MergedProfile 级别的 Counter，2 表示打印详细信息，"
                            + "3 表示打开一些可能导致性能回退的 Counter", "The level of query profile, "
                            + "1 means only collect Counter of MergedProfile, 2 means print detailed information,"
                            + " 3 means open some Counters that may cause performance degradation"})
    public int profileLevel = 1;

    @VariableMgr.VarAttr(name = MAX_INSTANCE_NUM)
    public int maxInstanceNum = 64;

    @VariableMgr.VarAttr(name = DML_PLAN_RETRY_TIMES, needForward = true, description = {
            "写入规划的最大重试次数。为了避免死锁，写入规划时采用了分阶段加锁。当在两次加锁中间，表结构发生变更时，会尝试重新规划。"
                    + "此变量限制重新规划的最大尝试次数。",
            "Maximum retry attempts for write planning. To avoid deadlocks, "
                    + "phased locking is adopted during write planning. "
                    + "When changes occur to the table structure between two locking phases, "
                    + "re-planning will be attempted. "
                    + "This variable limits the maximum number of retry attempts for re-planning."
    })
    public int dmlPlanRetryTimes = 3;

    @VariableMgr.VarAttr(name = ENABLE_INSERT_STRICT, needForward = true)
    public boolean enableInsertStrict = true;

    @VariableMgr.VarAttr(name = INSERT_MAX_FILTER_RATIO, needForward = true)
    public double insertMaxFilterRatio = 1.0;

    @VariableMgr.VarAttr(name = ENABLE_ODBC_TRANSCATION)
    public boolean enableOdbcTransaction = false;

    @VariableMgr.VarAttr(
            name = ENABLE_BINARY_SEARCH_FILTERING_PARTITIONS,
            fuzzy = true,
            description = {
                "是否允许使用二分查找算法去过滤分区。默认开。",
                "Whether to allow use binary search algorithm to filter partitions. ON by default."
            }
    )
    public boolean enableBinarySearchFilteringPartitions = true;

    @VariableMgr.VarAttr(name = ENABLE_SQL_CACHE, fuzzy = true)
    public boolean enableSqlCache = false;

    @VariableMgr.VarAttr(name = ENABLE_QUERY_CACHE)
    public boolean enableQueryCache = false;

    @VarAttr(name = QUERY_CACHE_FORCE_REFRESH)
    private boolean queryCacheForceRefresh = false;

    @VarAttr(name = QUERY_CACHE_ENTRY_MAX_BYTES)
    private long queryCacheEntryMaxBytes = 5242880;

    @VarAttr(name = QUERY_CACHE_ENTRY_MAX_ROWS)
    private long queryCacheEntryMaxRows = 500000;

    @VariableMgr.VarAttr(name = FORWARD_TO_MASTER)
    public boolean forwardToMaster = true;

    @VariableMgr.VarAttr(name = USE_V2_ROLLUP)
    public boolean useV2Rollup = false;

    @VariableMgr.VarAttr(name = REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL)
    public boolean rewriteCountDistinct = true;

    // compatible with some mysql client connect, say DataGrip of JetBrains
    @VariableMgr.VarAttr(name = EVENT_SCHEDULER)
    public String eventScheduler = "OFF";
    @VariableMgr.VarAttr(name = STORAGE_ENGINE)
    public String storageEngine = "olap";
    @VariableMgr.VarAttr(name = DEFAULT_STORAGE_ENGINE)
    public String defaultStorageEngine = "olap";
    @VariableMgr.VarAttr(name = DEFAULT_TMP_STORAGE_ENGINE)
    public String defaultTmpStorageEngine = "olap";

    @VariableMgr.VarAttr(name = MAX_SCAN_KEY_NUM)
    public int maxScanKeyNum = 48;
    @VariableMgr.VarAttr(name = MAX_PUSHDOWN_CONDITIONS_PER_COLUMN)
    public int maxPushdownConditionsPerColumn = 1024;
    @VariableMgr.VarAttr(name = SHOW_HIDDEN_COLUMNS, flag = VariableMgr.SESSION_ONLY, needForward = true)
    public boolean showHiddenColumns = false;

    @VariableMgr.VarAttr(name = ALLOW_PARTITION_COLUMN_NULLABLE, description = {
            "是否允许 NULLABLE 列作为 PARTITION 列。开启后，RANGE PARTITION 允许 NULLABLE PARTITION 列"
                    + "（LIST PARTITION当前不支持）。默认开。",
            "Whether to allow NULLABLE columns as PARTITION columns. When ON, RANGE PARTITION allows "
                    + "NULLABLE PARTITION columns (LIST PARTITION is not supported currently). ON by default." })
    public boolean allowPartitionColumnNullable = true;

    @VariableMgr.VarAttr(name = DELETE_WITHOUT_PARTITION, needForward = true)
    public boolean deleteWithoutPartition = false;

    @VariableMgr.VarAttr(name = SEND_BATCH_PARALLELISM, needForward = true)
    public int sendBatchParallelism = 1;

    @VariableMgr.VarAttr(name = ENABLE_VARIANT_ACCESS_IN_ORIGINAL_PLANNER)
    public boolean enableVariantAccessInOriginalPlanner = false;

    @VariableMgr.VarAttr(name = EXTRACT_WIDE_RANGE_EXPR, needForward = true)
    public boolean extractWideRangeExpr = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_DML, varType = VariableAnnotation.REMOVED)
    public boolean enableNereidsDML = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_DML_WITH_PIPELINE,
            varType = VariableAnnotation.REMOVED, description = { "在新优化器中，使用pipeline引擎执行DML",
                    "execute DML with pipeline engine in Nereids" })
    public boolean enableNereidsDmlWithPipeline = true;

    @VariableMgr.VarAttr(name = ENABLE_STRICT_CONSISTENCY_DML, needForward = true)
    public boolean enableStrictConsistencyDml = true;

    @VariableMgr.VarAttr(name = ENABLE_VECTORIZED_ENGINE, varType = VariableAnnotation.REMOVED)
    public boolean enableVectorizedEngine = true;

    @VariableMgr.VarAttr(name = ENABLE_PIPELINE_ENGINE, fuzzy = false, needForward = true,
            varType = VariableAnnotation.REMOVED)
    private boolean enablePipelineEngine = true;

    @VariableMgr.VarAttr(name = ENABLE_PIPELINE_X_ENGINE, fuzzy = false, varType = VariableAnnotation.REMOVED)
    private boolean enablePipelineXEngine = true;

    @VariableMgr.VarAttr(name = ENABLE_SHARED_SCAN, fuzzy = false, varType = VariableAnnotation.EXPERIMENTAL,
            needForward = true)
    private boolean enableSharedScan = false;

    @VariableMgr.VarAttr(name = ENABLE_PARALLEL_SCAN, fuzzy = true, varType = VariableAnnotation.EXPERIMENTAL,
            needForward = true)
    private boolean enableParallelScan = true;

    @VariableMgr.VarAttr(name = PARALLEL_SCAN_MAX_SCANNERS_COUNT, fuzzy = true,
            varType = VariableAnnotation.EXPERIMENTAL, needForward = true)
    private int parallelScanMaxScannersCount = 0;

    @VariableMgr.VarAttr(name = PARALLEL_SCAN_MIN_ROWS_PER_SCANNER, fuzzy = true,
            varType = VariableAnnotation.EXPERIMENTAL, needForward = true)
    private long parallelScanMinRowsPerScanner = 2097152; // 2M

    @VariableMgr.VarAttr(name = IGNORE_STORAGE_DATA_DISTRIBUTION, fuzzy = false,
            varType = VariableAnnotation.EXPERIMENTAL, needForward = true)
    private boolean ignoreStorageDataDistribution = true;

    @VariableMgr.VarAttr(name = USE_SERIAL_EXCHANGE, fuzzy = true,
            varType = VariableAnnotation.EXPERIMENTAL, needForward = true)
    private boolean useSerialExchange = false;

    @VariableMgr.VarAttr(
            name = ENABLE_LOCAL_SHUFFLE, fuzzy = false, varType = VariableAnnotation.EXPERIMENTAL,
            description = {"是否在pipelineX引擎上开启local shuffle优化",
                    "Whether to enable local shuffle on pipelineX engine."})
    private boolean enableLocalShuffle = true;

    @VariableMgr.VarAttr(
                name = FORCE_TO_LOCAL_SHUFFLE, fuzzy = false, varType = VariableAnnotation.EXPERIMENTAL,
                description = {"是否在pipelineX引擎上强制开启local shuffle优化",
                        "Whether to force to local shuffle on pipelineX engine."})
    private boolean forceToLocalShuffle = false;

    @VariableMgr.VarAttr(name = ENABLE_LOCAL_MERGE_SORT)
    private boolean enableLocalMergeSort = true;

    @VariableMgr.VarAttr(name = ENABLE_SHARED_EXCHANGE_SINK_BUFFER, fuzzy = true)
    private boolean enableSharedExchangeSinkBuffer = true;

    @VariableMgr.VarAttr(name = ENABLE_AGG_STATE, fuzzy = false, varType = VariableAnnotation.EXPERIMENTAL,
            needForward = true)
    public boolean enableAggState = false;

    @VariableMgr.VarAttr(name = ENABLE_PARALLEL_OUTFILE)
    public boolean enableParallelOutfile = false;

    @VariableMgr.VarAttr(name = CPU_RESOURCE_LIMIT)
    public int cpuResourceLimit = -1;

    @VariableMgr.VarAttr(name = SQL_QUOTE_SHOW_CREATE)
    public boolean sqlQuoteShowCreate = true;

    @VariableMgr.VarAttr(name = TRIM_TAILING_SPACES_FOR_EXTERNAL_TABLE_QUERY, needForward = true)
    public boolean trimTailingSpacesForExternalTableQuery = false;

    // the maximum size in bytes for a table that will be broadcast to all be nodes
    // when performing a join, By setting this value to -1 broadcasting can be disabled.
    // Default value is 1Gto
    @VariableMgr.VarAttr(name = AUTO_BROADCAST_JOIN_THRESHOLD)
    public double autoBroadcastJoinThreshold = 0.8;

    @VariableMgr.VarAttr(name = PARALLEL_PREPARE_THRESHOLD, fuzzy = true)
    public int parallelPrepareThreshold = 32;

    @VariableMgr.VarAttr(name = READ_HIVE_JSON_IN_ONE_COLUMN,
            description = {"在读取hive json的时候，由于存在一些不支持的json格式，我们默认会报错。为了让用户使用体验更好，"
                    + "当该变量为true的时候，将一整行json读取到第一列中，用户可以自行选择对一整行json进行处理，例如JSON_PARSE。"
                    + "需要表的第一列的数据类型为string.",
                    "When reading hive json, we will report an error by default because there are some unsupported "
                    + "json formats. In order to provide users with a better experience, when this variable is true,"
                    + "a whole line of json is read into the first column. Users can choose to process a whole line"
                    + "of json, such as JSON_PARSE. The data type of the first column of the table needs to"
                    + "be string."})
    private boolean readHiveJsonInOneColumn = false;

    @VariableMgr.VarAttr(name = ENABLE_COST_BASED_JOIN_REORDER)
    private boolean enableJoinReorderBasedCost = false;

    @VariableMgr.VarAttr(name = ENABLE_FOLD_CONSTANT_BY_BE, fuzzy = true)
    public boolean enableFoldConstantByBe = false;
    @VariableMgr.VarAttr(name = DEBUG_SKIP_FOLD_CONSTANT)
    public boolean debugSkipFoldConstant = false;

    @VariableMgr.VarAttr(name = ENABLE_REWRITE_ELEMENT_AT_TO_SLOT, fuzzy = true)
    private boolean enableRewriteElementAtToSlot = true;

    @VariableMgr.VarAttr(name = FORCE_SORT_ALGORITHM, needForward = true, description = { "强制指定SortNode的排序算法",
            "Force the sort algorithm of SortNode to be specified" })
    public String forceSortAlgorithm = "";

    @VariableMgr.VarAttr(name = FULL_SORT_MAX_BUFFERED_BYTES, needForward = true,
            setter = "setFullSortMaxBufferedBytes")
    public long fullSortMaxBufferedBytes = 64L * 1024L * 1024L;

    @VariableMgr.VarAttr(name = "ignore_runtime_filter_error", needForward = true, description = { "在rf遇到错误的时候忽略该rf",
            "Ignore the rf when it encounters an error" })
    public boolean ignoreRuntimeFilterError = false;

    @VariableMgr.VarAttr(name = "enable_fixed_len_to_uint32_v2", needForward = true, description = {
            "使用新版本fixed_len_to_uint32_v2,对datetimev2类型bloom filter做了优化",
            "Using the new version fixed_len_to_uint32_v2, the datetimev2 type bloom filter has been optimized" })
    public boolean enableFixedLenToUint32V2 = true;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_MODE, needForward = true)
    private String runtimeFilterMode = "GLOBAL";

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_SIZE, needForward = true)
    private int runtimeBloomFilterSize = 2097152;

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_MIN_SIZE, needForward = true)
    private int runtimeBloomFilterMinSize = 1048576;

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_MAX_SIZE, needForward = true)
    private int runtimeBloomFilterMaxSize = 67108864;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_WAIT_TIME_MS, needForward = true)
    private int runtimeFilterWaitTimeMs = 1000;

    @VariableMgr.VarAttr(name = runtime_filter_wait_infinitely, fuzzy = true, needForward = true)
    private boolean runtimeFilterWaitInfinitely = false;

    @VariableMgr.VarAttr(name = RUNTIME_FILTERS_MAX_NUM, needForward = true)
    private int runtimeFiltersMaxNum = 10;

    // Set runtimeFilterType to IN_OR_BLOOM filter
    @VariableMgr.VarAttr(name = RUNTIME_FILTER_TYPE, fuzzy = true, needForward = true)
    private int runtimeFilterType = 12;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_MAX_IN_NUM, needForward = true, fuzzy = true)
    private int runtimeFilterMaxInNum = 40960;

    @VariableMgr.VarAttr(name = ENABLE_SYNC_RUNTIME_FILTER_SIZE, needForward = true, fuzzy = true)
    private boolean enableSyncRuntimeFilterSize = true;

    @VariableMgr.VarAttr(name = "runtime_filter_max_build_row_count", needForward = true, fuzzy = false)
    public long runtimeFilterMaxBuildRowCount = 64L * 1024L * 1024L;

    @VariableMgr.VarAttr(name = ENABLE_PARALLEL_RESULT_SINK, needForward = true, fuzzy = true)
    private boolean enableParallelResultSink = true;

    @VariableMgr.VarAttr(name = "sort_phase_num", fuzzy = true, needForward = true,
            description = {"如设置为1，则只生成1阶段sort，设置为2，则只生成2阶段sort，设置其它值，优化器根据代价选择sort类型",
                    "set the number of sort phases 1 or 2. if set other value, let cbo decide the sort type"})
    public int sortPhaseNum = 0;

    @VariableMgr.VarAttr(name = HIVE_TEXT_COMPRESSION, fuzzy = true, needForward = true)
    private String hiveTextCompression = "plain";

    @VariableMgr.VarAttr(name = READ_CSV_EMPTY_LINE_AS_NULL, needForward = true,
            description = {"在读取csv文件时是否读取csv的空行为null",
                    "Determine whether to read empty rows in CSV files as NULL when reading CSV files."})
    public boolean readCsvEmptyLineAsNull = false;

    @VariableMgr.VarAttr(name = USE_RF_DEFAULT)
    public boolean useRuntimeFilterDefaultSize = false;

    @VariableMgr.VarAttr(name = "enable_topn_lazy_materialization", needForward = true,
            fuzzy = false,
            varType = VariableAnnotation.EXPERIMENTAL)
    public boolean enableTopnLazyMaterialization = true;

    @VariableMgr.VarAttr(name = DISABLE_INVERTED_INDEX_V1_FOR_VARIANT, needForward = true)
    private boolean disableInvertedIndexV1ForVaraint = true;

    @VariableMgr.VarAttr(name = DISABLE_VARIANT_FLATTEN_NESTED, needForward = true)
    private boolean disableVariantFlattenNested = true;

    public int getBeNumberForTest() {
        return beNumberForTest;
    }

    @VariableMgr.VarAttr(name = DESCRIBE_EXTEND_VARIANT_COLUMN, needForward = true)
    public boolean enableDescribeExtendVariantColumn = false;

    @VariableMgr.VarAttr(name = PROFILLING)
    public boolean profiling = false;

    public void setBeNumberForTest(int beNumberForTest) {
        this.beNumberForTest = beNumberForTest;
    }

    @VariableMgr.VarAttr(name = BE_NUMBER_FOR_TEST)
    private int beNumberForTest = -1;

    public double getCboCpuWeight() {
        return cboCpuWeight;
    }

    public void setCboCpuWeight(double cboCpuWeight) {
        this.cboCpuWeight = cboCpuWeight;
    }

    public double getCboMemWeight() {
        return cboMemWeight;
    }

    public void setCboMemWeight(double cboMemWeight) {
        this.cboMemWeight = cboMemWeight;
    }

    public double getCboNetWeight() {
        return cboNetWeight;
    }

    public void setCboNetWeight(double cboNetWeight) {
        this.cboNetWeight = cboNetWeight;
    }

    @VariableMgr.VarAttr(name = "enable_init_join_order")
    public boolean enableInitJoinOrder = true;

    @VariableMgr.VarAttr(name = CBO_CPU_WEIGHT)
    private double cboCpuWeight = 1.0;

    @VariableMgr.VarAttr(name = CBO_MEM_WEIGHT)
    private double cboMemWeight = 1.0;

    @VariableMgr.VarAttr(name = CBO_NET_WEIGHT)
    private double cboNetWeight = 1.5;

    @VariableMgr.VarAttr(name = DISABLE_JOIN_REORDER)
    private boolean disableJoinReorder = false;

    @VariableMgr.VarAttr(name = MAX_JOIN_NUMBER_OF_REORDER)
    private int maxJoinNumberOfReorder = 63;

    @VariableMgr.VarAttr(name = ENABLE_BUSHY_TREE, needForward = true)
    private boolean enableBushyTree = false;

    public int getMaxJoinNumBushyTree() {
        return maxJoinNumBushyTree;
    }

    public void setMaxJoinNumBushyTree(int maxJoinNumBushyTree) {
        this.maxJoinNumBushyTree = maxJoinNumBushyTree;
    }

    public int getMaxJoinNumberOfReorder() {
        return maxJoinNumberOfReorder;
    }

    public void setMaxJoinNumberOfReorder(int maxJoinNumberOfReorder) {
        this.maxJoinNumberOfReorder = maxJoinNumberOfReorder;
    }

    @VariableMgr.VarAttr(name = MAX_JOIN_NUMBER_BUSHY_TREE)
    private int maxJoinNumBushyTree = 8;

    @VariableMgr.VarAttr(name = ENABLE_PARTITION_TOPN)
    private boolean enablePartitionTopN = true;

    @VariableMgr.VarAttr(name = PARTITION_TOPN_MAX_PARTITIONS, needForward = true, description = {
            "这个阈值决定了partition_topn计算时的最大分区数量，超过这个阈值后且输入总行数少于预估总量，剩余的数据将直接透传给下一个算子",
            "This threshold determines how many partitions will be allocated for window function get topn."
                    + " if this threshold is exceeded and input rows less than the estimated total rows, the remaining"
                    + " data will be pass through to other node directly."
    })
    private int partitionTopNMaxPartitions = 1024;

    @VariableMgr.VarAttr(name = PARTITION_TOPN_PER_PARTITION_ROWS, needForward = true, description = {
            "这个数值用于partition_topn预估每个分区的行数，用来计算所有分区的预估数据总量，决定是否能透传下一个算子",
            "This value is used for partition_topn to estimate the number of rows in each partition, to calculate "
            + " the estimated total amount of data for all partitions, and to determine whether the next operator "
            + " can be passed transparently."
    })
    private int partitionTopNPerPartitionRows = 1000;

    @VariableMgr.VarAttr(name = GLOBAL_PARTITION_TOPN_THRESHOLD)
    private double globalPartitionTopNThreshold = 100;

    @VariableMgr.VarAttr(name = ENABLE_INFER_PREDICATE)
    private boolean enableInferPredicate = true;

    @VariableMgr.VarAttr(name = RETURN_OBJECT_DATA_AS_BINARY)
    private boolean returnObjectDataAsBinary = false;

    @VariableMgr.VarAttr(name = BLOCK_ENCRYPTION_MODE)
    private String blockEncryptionMode = "";

    @VariableMgr.VarAttr(name = ENABLE_PROJECTION)
    private boolean enableProjection = true;

    @VariableMgr.VarAttr(name = ENABLE_SHORT_CIRCUIT_QUERY)
    private boolean enableShortCircuitQuery = true;

    @VariableMgr.VarAttr(name = ENABLE_SHORT_CIRCUIT_QUERY_ACCESS_COLUMN_STORE)
    private boolean enableShortCircuitQueryAcessColumnStore = true;

    @VariableMgr.VarAttr(name = CHECK_OVERFLOW_FOR_DECIMAL, affectQueryResult = true)
    private boolean checkOverflowForDecimal = true;

    @VariableMgr.VarAttr(name = DECIMAL_OVERFLOW_SCALE, needForward = true, affectQueryResult = true, description = {
            "当decimal数值计算结果精度溢出时，计算结果最多可保留的小数位数", "When the precision of the result of"
            + " a decimal numerical calculation overflows,"
            + "the maximum number of decimal scale that the result can be retained"
    })
    public int decimalOverflowScale = 6;

    @VariableMgr.VarAttr(name = ENABLE_DPHYP_OPTIMIZER)
    public boolean enableDPHypOptimizer = false;

    /**
     * This variable is used to select n-th optimized plan in memo.
     * It can allow us select different plans for the same SQL statement
     * and these plans can be used to evaluate the cost model.
     */
    @VariableMgr.VarAttr(name = NTH_OPTIMIZED_PLAN)
    private int nthOptimizedPlan = 1;

    @VariableMgr.VarAttr(name = LIMIT_ROWS_FOR_SINGLE_INSTANCE,
            description = {"当一个 ScanNode 上没有过滤条件，且 limit 值小于这个阈值时，"
                    + "系统会将这个算子的并发度调整为1，以减少简单查询的扇出",
                    "When a ScanNode has no filter conditions and the limit value is less than this threshold, "
                            + "the system will adjust the concurrency of this operator to 1 "
                            + "to reduce the fan-out of simple queries"})
    public long limitRowsForSingleInstance = 10000;

    public boolean isEnableLeftZigZag() {
        return enableLeftZigZag;
    }

    public void setEnableLeftZigZag(boolean enableLeftZigZag) {
        this.enableLeftZigZag = enableLeftZigZag;
    }

    @VariableMgr.VarAttr(name = ENABLE_LEFT_ZIG_ZAG)
    private boolean enableLeftZigZag = false;

    public boolean isEnableHboOptimization() {
        return enableHboOptimization;
    }

    public double getHboRfSafeThreshold() {
        return hboRfSafeThreshold;
    }

    public double getHboRowMatchingThreshold() {
        return hboRowMatchingThreshold;
    }

    public int getHboSkewRatioThreshold() {
        return hboSkewRatioThreshold;
    }

    @VariableMgr.VarAttr(name = ENABLE_HBO_OPTIMIZATION)
    private boolean enableHboOptimization = false;

    public boolean isEnableHboInfoCollection() {
        return enableHboInfoCollection;
    }

    public boolean isEnableHboNonStrictMatchingMode() {
        return enableHboNonStrictMatchingMode;
    }

    @VariableMgr.VarAttr(name = ENABLE_HBO_INFO_COLLECTION, flag = VariableMgr.GLOBAL)
    private boolean enableHboInfoCollection = false;

    @VariableMgr.VarAttr(name = ENABLE_HBO_NONSTRICT_MATCHING_MODE)
    private boolean enableHboNonStrictMatchingMode = false;

    @VariableMgr.VarAttr(name = HBO_RFSAFE_THRESHOLD, needForward = true)
    private double hboRfSafeThreshold = 0.5;

    @VariableMgr.VarAttr(name = HBO_ROW_MATCHING_THRESHOLD, needForward = true)
    private double hboRowMatchingThreshold = 0.1;

    @VariableMgr.VarAttr(name = HBO_SKEW_RATIO_THRESHOLD, needForward = true)
    private int hboSkewRatioThreshold = 5;

    /**
     * as the new optimizer is not mature yet, use this var
     * to control whether to use new optimizer, remove it when
     * the new optimizer is fully developed. I hope that day
     * would be coming soon.
     */
    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_PLANNER, needForward = true, varType = VariableAnnotation.REMOVED)
    private boolean enableNereidsPlanner = true;

    @VariableMgr.VarAttr(name = DISABLE_NEREIDS_RULES, needForward = true)
    private String disableNereidsRules = "";

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_RULES, needForward = true)
    public String enableNereidsRules = "";

    @VariableMgr.VarAttr(name = ENABLE_VISITOR_REWRITER_DEPTH_THRESHOLD, needForward = true, description = {
            "当查询计划的深度小于或等于这个阈值时，使用visitor rewriter去加速改写，否则使用stack rewriter去改写，"
                    + "防止StackOverflowError",
            "When the depth of the query plan is less than or equal to this threshold, use visitor rewriter to "
                    + "speed up rewriting, otherwise use stack rewriter to rewrite to prevent StackOverflowError"
    })
    public int enableVisitorRewriterDepthThreshold = 100;

    @VariableMgr.VarAttr(name = DISABLE_NEREIDS_EXPRESSION_RULES, needForward = true,
            setter = "setDisableNereidsExpressionRules")
    private String disableNereidsExpressionRules = "";

    private BitSet disableNereidsExpressionRuleSet = new BitSet();

    @VariableMgr.VarAttr(name = "filter_cost_factor", needForward = true)
    public double filterCostFactor = 0.0001;

    @VariableMgr.VarAttr(name = NEREIDS_STAR_SCHEMA_SUPPORT)
    private boolean nereidsStarSchemaSupport = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_DISTRIBUTE_PLANNER, needForward = true,
            fuzzy = false, varType = VariableAnnotation.EXPERIMENTAL, description = {
                "使用新的nereids的分布式规划器的开关，这个分布式规划器可以规划出一些更高效的查询计划，比如在某些情况下，"
                        + "可以把左表shuffle到右表去做bucket shuffle join",
                "The switch to use new DistributedPlanner of nereids, this planner can planning some "
                        + "more efficient query plans, e.g. in certain situations, shuffle left side to "
                        + "right side to do bucket shuffle join"
            }
    )
    private boolean enableNereidsDistributePlanner = true;

    @VariableMgr.VarAttr(name = REWRITE_OR_TO_IN_PREDICATE_THRESHOLD, fuzzy = true)
    private int rewriteOrToInPredicateThreshold = 2;

    @VariableMgr.VarAttr(name = "push_topn_to_agg", fuzzy = false, needForward = true)
    public boolean pushTopnToAgg = true;

    @VariableMgr.VarAttr(name = NEREIDS_CBO_PENALTY_FACTOR, needForward = true)
    private double nereidsCboPenaltyFactor = 0.7;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_TRACE)
    private boolean enableNereidsTrace = false;

    @VariableMgr.VarAttr(name = ENABLE_EXPR_TRACE)
    private boolean enableExprTrace = false;

    @VariableMgr.VarAttr(name = ENABLE_DPHYP_TRACE, needForward = true)
    public boolean enableDpHypTrace = false;

    @VariableMgr.VarAttr(name = BROADCAST_RIGHT_TABLE_SCALE_FACTOR)
    private double broadcastRightTableScaleFactor = 0.0;

    @VariableMgr.VarAttr(name = LEFT_SEMI_OR_ANTI_PROBE_FACTOR)
    private double leftSemiOrAntiProbeFactor = 0.05;

    @VariableMgr.VarAttr(name = BROADCAST_ROW_COUNT_LIMIT, needForward = true)
    private double broadcastRowCountLimit = 30000000;

    @VariableMgr.VarAttr(name = BROADCAST_HASHTABLE_MEM_LIMIT_PERCENTAGE, needForward = true)
    private double broadcastHashtableMemLimitPercentage = 0.2;

    @VariableMgr.VarAttr(name = ENABLE_RUNTIME_FILTER_PRUNE, needForward = true, fuzzy = true)
    public boolean enableRuntimeFilterPrune = true;

    @VariableMgr.VarAttr(name = ENABLE_RUNTIME_FILTER_PARTITION_PRUNE, needForward = true, fuzzy = true)
    public boolean enableRuntimeFilterPartitionPrune = true;

    /**
     * The client can pass some special information by setting this session variable in the format: "k1:v1;k2:v2".
     * For example, trace_id can be passed to trace the query request sent by the user.
     * set session_context="trace_id:1234565678";
     */
    @VariableMgr.VarAttr(name = SESSION_CONTEXT, needForward = true)
    public String sessionContext = "";

    @VariableMgr.VarAttr(name = ENABLE_SINGLE_REPLICA_INSERT,
            needForward = true, varType = VariableAnnotation.EXPERIMENTAL)
    public boolean enableSingleReplicaInsert = false;

    @VariableMgr.VarAttr(
            name = ENABLE_FAST_ANALYZE_INSERT_INTO_VALUES, fuzzy = true,
            description = {
                    "跳过大部分的优化规则，快速分析insert into values语句",
                    "Skip most optimization rules and quickly analyze insert into values statements"
            }
    )
    private boolean enableFastAnalyzeInsertIntoValues = true;

    @VariableMgr.VarAttr(name = ENABLE_FUNCTION_PUSHDOWN, fuzzy = true)
    public boolean enableFunctionPushdown = false;

    @VariableMgr.VarAttr(name = ENABLE_EXT_FUNC_PRED_PUSHDOWN, needForward = true,
            description = {"启用外部表（如通过ODBC或JDBC访问的表）查询中谓词的函数下推",
                    "Enable function pushdown for predicates in queries to external tables "
                    + "(such as tables accessed via ODBC or JDBC)"})
    public boolean enableExtFuncPredPushdown = true;

    @VariableMgr.VarAttr(name = FORBID_UNKNOWN_COLUMN_STATS)
    public boolean forbidUnknownColStats = false;

    @VariableMgr.VarAttr(name = ENABLE_COMMON_EXPR_PUSHDOWN, fuzzy = true)
    public boolean enableCommonExprPushdown = true;

    @VariableMgr.VarAttr(name = ENABLE_LOCAL_EXCHANGE, fuzzy = false, flag = VariableMgr.INVISIBLE,
            varType = VariableAnnotation.DEPRECATED)
    public boolean enableLocalExchange = true;

    /**
     * For debug purpose, don't merge unique key and agg key when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_STORAGE_ENGINE_MERGE, needForward = true)
    public boolean skipStorageEngineMerge = false;

    /**
     * For debug purpose, skip delete predicate when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_DELETE_PREDICATE, needForward = true)
    public boolean skipDeletePredicate = false;

    /**
     * For debug purpose, skip delete sign when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_DELETE_SIGN, needForward = true)
    public boolean skipDeleteSign = false;

    /**
     * For debug purpose, skip delete bitmap when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_DELETE_BITMAP, needForward = true)
    public boolean skipDeleteBitmap = false;

    // This variable replace the original FE config `recover_with_skip_missing_version`.
    // In some scenarios, all replicas of tablet are having missing versions, and the tablet is unable to recover.
    // This config can control the behavior of query. When it is set to `true`, the query will ignore the
    // visible version recorded in FE partition, use the replica version. If the replica on BE has missing versions,
    // the query will directly skip this missing version, and only return the data of the existing versions.
    // Besides, the query will always try to select the one with the highest lastSuccessVersion among all surviving
    // BE replicas, so as to recover as much data as possible.
    // You should only open it in the emergency scenarios mentioned above, only used for temporary recovery queries.
    // This variable conflicts with the use_fix_replica variable, when the use_fix_replica variable is not -1,
    // this variable will not work.
    @VariableMgr.VarAttr(name = SKIP_MISSING_VERSION)
    public boolean skipMissingVersion = false;

    // This variable is used to control whether to skip the bad tablet.
    // In some scenarios, user has a huge amount of data and only a single replica was specified when creating
    // the table, if one of the tablet is damaged, the table will not be able to be select. If the user does not care
    // about the integrity of the data, they can use this variable to temporarily skip the bad tablet for querying and
    // load the remaining data into a new table.
    @VariableMgr.VarAttr(name = SKIP_BAD_TABLET)
    public boolean skipBadTablet = false;

    // This variable is used to avoid FE fallback to the original parser. When we execute SQL in regression tests
    // for nereids, fallback will cause the Doris return the correct result although the syntax is unsupported
    // in nereids for some mistaken modification. You should set it on the
    @VariableMgr.VarAttr(name = ENABLE_FALLBACK_TO_ORIGINAL_PLANNER, varType = VariableAnnotation.REMOVED)
    public boolean enableFallbackToOriginalPlanner = false;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_TIMEOUT, needForward = true)
    public boolean enableNereidsTimeout = true;

    @VariableMgr.VarAttr(name = NEREIDS_TIMEOUT_SECOND, needForward = true)
    public int nereidsTimeoutSecond = 30;

    @VariableMgr.VarAttr(name = ENABLE_PUSH_DOWN_NO_GROUP_AGG)
    public boolean enablePushDownNoGroupAgg = true;

    /**
     * The current statistics are only used for CBO test,
     * and are not available to users. (work in progress)
     */
    @VariableMgr.VarAttr(name = ENABLE_CBO_STATISTICS)
    public boolean enableCboStatistics = false;

    @VariableMgr.VarAttr(name = ENABLE_ELIMINATE_SORT_NODE)
    public boolean enableEliminateSortNode = true;

    @VariableMgr.VarAttr(name = PARTITION_PRUNING_EXPAND_THRESHOLD, fuzzy = true)
    public int partitionPruningExpandThreshold = 10;

    @VariableMgr.VarAttr(name = ENABLE_SHARE_HASH_TABLE_FOR_BROADCAST_JOIN, fuzzy = true)
    public boolean enableShareHashTableForBroadcastJoin = true;

    @VariableMgr.VarAttr(name = ENABLE_UNICODE_NAME_SUPPORT, needForward = true)
    public boolean enableUnicodeNameSupport = true;

    @VariableMgr.VarAttr(name = GROUP_CONCAT_MAX_LEN, affectQueryResult = true)
    public long groupConcatMaxLen = 2147483646;

    @VariableMgr.VarAttr(
            name = USE_ONE_PHASE_AGG_FOR_GROUP_CONCAT_WITH_ORDER,
            needForward = true,
            fuzzy = true,
            description = {
                    "允许使用一阶段聚合来执行带有order的group_concat函数",
                    "Enable to use one stage aggregation to execute the group_concat function with order"
            }
    )
    public boolean useOnePhaseAggForGroupConcatWithOrder = false;

    // Whether enable two phase read optimization
    // 1. read related rowids along with necessary column data
    // 2. spawn fetch RPC to other nodes to get related data by sorted rowids
    @VariableMgr.VarAttr(name = ENABLE_TWO_PHASE_READ_OPT, fuzzy = true)
    public boolean enableTwoPhaseReadOpt = true;
    @VariableMgr.VarAttr(name = TOPN_OPT_LIMIT_THRESHOLD)
    public long topnOptLimitThreshold = 1024;
    @VariableMgr.VarAttr(name = TOPN_FILTER_RATIO)
    public double topnFilterRatio = 0.5;
    @VariableMgr.VarAttr(name = ENABLE_SNAPSHOT_POINT_QUERY)
    public boolean enableSnapshotPointQuery = true;

    @VariableMgr.VarAttr(name = ENABLE_SERVER_SIDE_PREPARED_STATEMENT, needForward = true, description = {
            "是否启用开启服务端prepared statement", "Set whether to enable server side prepared statement."})
    public boolean enableServeSidePreparedStatement = true;

    @VariableMgr.VarAttr(name = MAX_PREPARED_STMT_COUNT,  flag = VariableMgr.GLOBAL,
            needForward = true, description = {
                "服务端prepared statement最大个数", "the maximum prepared statements server holds."})
    public int maxPreparedStmtCount = 100000;

    @VariableMgr.VarAttr(name = ENABLE_GROUP_COMMIT_FULL_PREPARE)
    public boolean enableGroupCommitFullPrepare = true;

    // Default value is false, which means the group by and having clause
    // should first use column name not alias. According to mysql.
    @VariableMgr.VarAttr(name = GROUP_BY_AND_HAVING_USE_ALIAS_FIRST, varType = VariableAnnotation.DEPRECATED)
    public boolean groupByAndHavingUseAliasFirst = false;

    // Whether disable block file cache. Block cache only works when FE's query options sets disableFileCache false
    // along with BE's config `enable_file_cache` true
    @VariableMgr.VarAttr(name = DISABLE_FILE_CACHE, needForward = true)
    public boolean disableFileCache = false;

    // Whether enable block file cache. Only take effect when BE config item enable_file_cache is true.
    @VariableMgr.VarAttr(name = ENABLE_FILE_CACHE, needForward = true, description = {
            "是否启用file cache。该变量只有在be.conf中enable_file_cache=true时才有效，"
                    + "如果be.conf中enable_file_cache=false，该BE节点的file cache处于禁用状态。",
            "Set wether to use file cache. This variable takes effect only if the BE config enable_file_cache=true. "
                    + "The cache is not used when BE config enable_file_cache=false."})
    public boolean enableFileCache = false;

    // Specify base path for file cache, or chose a random path.
    @VariableMgr.VarAttr(name = FILE_CACHE_BASE_PATH, needForward = true, description = {
            "指定block file cache在BE上的存储路径，默认 'random'，随机选择BE配置的存储路径。",
            "Specify the storage path of the block file cache on BE, default 'random', "
                    + "and randomly select the storage path configured by BE."})
    public String fileCacheBasePath = "random";

    // Whether enable query with inverted index.
    @VariableMgr.VarAttr(name = ENABLE_INVERTED_INDEX_QUERY, needForward = true, description = {
            "是否启用inverted index query。", "Set whether to use inverted index query."})
    public boolean enableInvertedIndexQuery = true;

    // Whether enable query expr with inverted index.
    @VariableMgr.VarAttr(name = ENABLE_COMMON_EXPR_PUSHDOWN_FOR_INVERTED_INDEX, fuzzy = true, needForward = true,
            description = {"是否启用表达式上使用 inverted index。", "Set whether to use inverted index query for expr."})
    public boolean enableCommonExpPushDownForInvertedIndex = true;

    // Whether enable pushdown count agg to scan node when using inverted index match.
    @VariableMgr.VarAttr(name = ENABLE_PUSHDOWN_COUNT_ON_INDEX, needForward = true, description = {
            "是否启用count_on_index pushdown。", "Set whether to pushdown count_on_index."})
    public boolean enablePushDownCountOnIndex = true;

    // Whether enable no need read data opt in segment_iterator.
    @VariableMgr.VarAttr(name = ENABLE_NO_NEED_READ_DATA_OPT, needForward = true, description = {
            "是否启用no_need_read_data opt。", "Set whether to enable no_need_read_data opt."})
    public boolean enableNoNeedReadDataOpt = true;

    // Whether enable pushdown minmax to scan node of unique table.
    @VariableMgr.VarAttr(name = ENABLE_PUSHDOWN_MINMAX_ON_UNIQUE, needForward = true, description = {
        "是否启用pushdown minmax on unique table。", "Set whether to pushdown minmax on unique table."})
    public boolean enablePushDownMinMaxOnUnique = false;

    // Whether enable push down string type minmax to scan node.
    @VariableMgr.VarAttr(name = ENABLE_PUSHDOWN_STRING_MINMAX, needForward = true, description = {
        "是否启用string类型min max下推。", "Set whether to enable push down string type minmax."})
    public boolean enablePushDownStringMinMax = false;

    // Whether drop table when create table as select insert data appear error.
    @VariableMgr.VarAttr(name = DROP_TABLE_IF_CTAS_FAILED, needForward = true)
    public boolean dropTableIfCtasFailed = true;

    @VariableMgr.VarAttr(name = MAX_TABLE_COUNT_USE_CASCADES_JOIN_REORDER, needForward = true)
    public int maxTableCountUseCascadesJoinReorder = 10;

    @VariableMgr.VarAttr(name = JOIN_REORDER_TIME_LIMIT, needForward = true)
    public long joinReorderTimeLimit = 1000;

    // If this is true, the result of `show roles` will return all user default role
    @VariableMgr.VarAttr(name = SHOW_USER_DEFAULT_ROLE, needForward = true)
    public boolean showUserDefaultRole = false;

    // Default value is -1, which means not fix replica
    @VariableMgr.VarAttr(name = USE_FIX_REPLICA, needForward = true)
    public int useFixReplica = -1;

    @VariableMgr.VarAttr(name = DUMP_NEREIDS_MEMO)
    public boolean dumpNereidsMemo = false;

    @VariableMgr.VarAttr(name = "memo_max_group_expression_size")
    public int memoMaxGroupExpressionSize = 10000;

    @VariableMgr.VarAttr(name = DPHYPER_LIMIT)
    public int dphyperLimit = 1000;


    @VariableMgr.VarAttr(
            name = ENABLE_PAGE_CACHE,
            description = {"控制是否启用page cache。默认为 true。",
                "Controls whether to use page cache. "
                    + "The default value is true."},
            needForward = true)
    public boolean enablePageCache = true;

    @VariableMgr.VarAttr(name = ENABLE_FOLD_NONDETERMINISTIC_FN)
    public boolean enableFoldNondeterministicFn = false;

    @VariableMgr.VarAttr(name = TRACE_NEREIDS)
    public boolean traceNereids = false;

    @VariableMgr.VarAttr(name = PLAN_NEREIDS_DUMP)
    public boolean planNereidsDump = false;

    // If set to true, all query will be executed without returning result
    @VariableMgr.VarAttr(name = DRY_RUN_QUERY, needForward = true)
    public boolean dryRunQuery = false;

    @VariableMgr.VarAttr(name = FILE_SPLIT_SIZE, needForward = true)
    public long fileSplitSize = 0;

    @VariableMgr.VarAttr(
            name = NUM_PARTITIONS_IN_BATCH_MODE,
            fuzzy = true,
            description = {"如果分区数量超过阈值，BE将通过batch方式获取scan ranges。作用于 Hive、Hudi、MaxCompute 表。",
                    "If the number of partitions exceeds the threshold, scan ranges will be got through batch mode."},
            needForward = true)
    public int numPartitionsInBatchMode = 1024;

    @VariableMgr.VarAttr(
            name = NUM_FILES_IN_BATCH_MODE,
            fuzzy = true,
            description = {"如果文件数量超过阈值，BE将通过batch方式获取scan ranges",
                    "If the number of files exceeds the threshold, scan ranges will be got through batch mode."},
            needForward = true)
    public int numFilesInBatchMode = 1024;

    @VariableMgr.VarAttr(
            name = FETCH_SPLITS_MAX_WAIT_TIME,
            description = {"batch方式中BE获取splits的最大等待时间",
                    "The max wait time of getting splits in batch mode."},
            needForward = true)
    public long fetchSplitsMaxWaitTime = 1000;

    @VariableMgr.VarAttr(
            name = ENABLE_PARQUET_LAZY_MAT,
            fuzzy = true,
            description = {"控制 parquet reader 是否启用延迟物化技术。默认为 true。",
                    "Controls whether to use lazy materialization technology in parquet reader. "
                            + "The default value is true."},
            needForward = true)
    public boolean enableParquetLazyMat = true;

    @VariableMgr.VarAttr(
            name = ENABLE_ORC_LAZY_MAT,
            fuzzy = true,
            description = {"控制 orc reader 是否启用延迟物化技术。默认为 true。",
                    "Controls whether to use lazy materialization technology in orc reader. "
                            + "The default value is true."},
            needForward = true)
    public boolean enableOrcLazyMat = true;

    @VariableMgr.VarAttr(
            name = ORC_TINY_STRIPE_THRESHOLD_BYTES,
            fuzzy = true,
            description = {"在orc文件中如果一个stripe的字节大小小于`orc_tiny_stripe_threshold`,"
                    + "我们认为该stripe为 tiny stripe。对于多个连续的tiny stripe我们会进行读取优化，即一次性读多个tiny stripe."
                    + "如果你不想使用该优化，可以将该值设置为0。默认为 8M。",
                    "In an orc file, if the byte size of a stripe is less than `orc_tiny_stripe_threshold`,"
                            + "we consider the stripe to be a tiny stripe. For multiple consecutive tiny stripes,"
                            + "we will perform read optimization, that is, read multiple tiny stripes at a time."
                            + "If you do not want to use this optimization, you can set this value to 0."
                            + "The default is 8M."},
            needForward = true,
            setter = "setOrcTinyStripeThresholdBytes")
    public long orcTinyStripeThresholdBytes  = 8L * 1024L * 1024L;


    @VariableMgr.VarAttr(
            name = ORC_ONCE_MAX_READ_BYTES,
            fuzzy = true,
            description = {"在使用tiny stripe读取优化的时候，会对多个tiny stripe合并成一次IO，"
                    + "该参数用来控制每次IO请求的最大字节大小。你不应该将值设置的小于`orc_tiny_stripe_threshold`。默认为 8M。",
                    "When using tiny stripe read optimization, multiple tiny stripes will be merged into one IO."
                            + "This parameter is used to control the maximum byte size of each IO request."
                            + "You should not set the value less than `orc_tiny_stripe_threshold`."
                            + "The default is 8M."},
            needForward = true,
            setter = "setOrcOnceMaxReadBytes")
    public long orcOnceMaxReadBytes = 8L * 1024L * 1024L;


    @VariableMgr.VarAttr(
            name = ORC_MAX_MERGE_DISTANCE_BYTES,
            fuzzy = true,
            description = {"在使用tiny stripe读取优化的时候，由于tiny stripe并不一定连续。"
                    + "当两个tiny stripe之间距离大于该参数时，我们不会将其合并成一次IO。默认为 1M。",
                    "When using tiny stripe read optimization, since tiny stripes are not necessarily continuous,"
                            + "when the distance between two tiny stripes is greater than this parameter,"
                            + "we will not merge them into one IO. The default value is 1M."},
            needForward = true,
            setter = "setOrcMaxMergeDistanceBytes")
    public long orcMaxMergeDistanceBytes = 1024L * 1024L;


    @VariableMgr.VarAttr(
            name = ENABLE_PARQUET_FILTER_BY_MIN_MAX,
            fuzzy = true,
            description = {"控制 parquet reader 是否启用 min-max 值过滤。默认为 true。",
                    "Controls whether to filter by min-max values in parquet reader. "
                            + "The default value is true."},
            needForward = true)
    public boolean enableParquetFilterByMinMax = true;

    @VariableMgr.VarAttr(
            name = ENABLE_ORC_FILTER_BY_MIN_MAX,
            description = {"控制 orc reader 是否启用 min-max 值过滤。默认为 true。",
                    "Controls whether to filter by min-max values in orc reader. "
                            + "The default value is true."},
            needForward = true)
    public boolean enableOrcFilterByMinMax = true;

    @VariableMgr.VarAttr(
            name = CHECK_ORC_INIT_SARGS_SUCCESS,
            description = {"是否检查orc init sargs是否成功。默认为 false。",
                    "Whether to check whether orc init sargs is successful. "
                            + "The default value is false."},
            needForward = true)
    public boolean checkOrcInitSargsSuccess = false;

    @VariableMgr.VarAttr(
            name = EXTERNAL_TABLE_ANALYZE_PART_NUM,
            description = {"收集外表统计信息行数时选取的采样分区数，默认-1表示全部分区",
                    "Number of sample partition for collecting external table line number, "
                            + "default -1 means all partitions"},
            needForward = false)
    public int externalTableAnalyzePartNum = -1;

    @VariableMgr.VarAttr(name = INLINE_CTE_REFERENCED_THRESHOLD)
    public int inlineCTEReferencedThreshold = 1;

    @VariableMgr.VarAttr(name = ENABLE_CTE_MATERIALIZE)
    public boolean enableCTEMaterialize = true;

    @VariableMgr.VarAttr(name = ENABLE_ORDERED_SCAN_RANGE_LOCATIONS)
    public boolean enableOrderedScanRangeLocations = false;

    @VariableMgr.VarAttr(name = ENABLE_ANALYZE_COMPLEX_TYPE_COLUMN)
    public boolean enableAnalyzeComplexTypeColumn = false;

    @VariableMgr.VarAttr(name = ENABLE_STRONG_CONSISTENCY, description = {"用以开启强一致读。Doris 默认支持同一个会话内的"
            + "强一致性，即同一个会话内对数据的变更操作是实时可见的。如需要会话间的强一致读，则需将此变量设置为true。",
            "Used to enable strong consistent reading. By default, Doris supports strong consistency "
                    + "within the same session, that is, changes to data within the same session are visible in "
                    + "real time. If you want strong consistent reads between sessions, set this variable to true. "
    })
    public boolean enableStrongConsistencyRead = false;

    @VariableMgr.VarAttr(name = PARALLEL_SYNC_ANALYZE_TASK_NUM)
    public int parallelSyncAnalyzeTaskNum = 2;

    @VariableMgr.VarAttr(name = TRUNCATE_CHAR_OR_VARCHAR_COLUMNS,
            description = {"是否按照表的 schema 来截断 char 或者 varchar 列。默认为 false。\n"
                    + "因为外表会存在表的 schema 中 char 或者 varchar 列的最大长度和底层 parquet 或者 orc 文件中的 schema 不一致"
                    + "的情况。此时开启改选项，会按照表的 schema 中的最大长度进行截断。",
                    "Whether to truncate char or varchar columns according to the table's schema. "
                            + "The default is false.\n"
                    + "Because the maximum length of the char or varchar column in the schema of the table"
                            + " is inconsistent with the schema in the underlying parquet or orc file."
                    + " At this time, if the option is turned on, it will be truncated according to the maximum length"
                            + " in the schema of the table."},
            needForward = true)
    public boolean truncateCharOrVarcharColumns = false;

    @VariableMgr.VarAttr(name = ENABLE_MEMTABLE_ON_SINK_NODE, needForward = true)
    public boolean enableMemtableOnSinkNode = true;

    @VariableMgr.VarAttr(name = LOAD_STREAM_PER_NODE)
    public int loadStreamPerNode = 2;

    @VariableMgr.VarAttr(name = GROUP_COMMIT, needForward = true)
    public String groupCommit = "off_mode";

    @VariableMgr.VarAttr(name = ENABLE_PREPARED_STMT_AUDIT_LOG, needForward = true)
    public boolean enablePreparedStmtAuditLog = false;

    @VariableMgr.VarAttr(name = INVERTED_INDEX_CONJUNCTION_OPT_THRESHOLD,
            description = {"在match_all中求取多个倒排索引的交集时,如果最大的倒排索引中的总数是最小倒排索引中的总数的整数倍,"
                    + "则使用跳表来优化交集操作。",
                    "When intersecting multiple inverted indexes in match_all,"
                    + " if the maximum total count of the largest inverted index"
                    + " is a multiple of the minimum total count of the smallest inverted index,"
                    + " use a skiplist to optimize the intersection."})
    public int invertedIndexConjunctionOptThreshold = 1000;

    @VariableMgr.VarAttr(name = INVERTED_INDEX_MAX_EXPANSIONS,
            description = {"这个参数用来限制查询时扩展的词项（terms）的数量，以此来控制查询的性能",
                    "This parameter is used to limit the number of term expansions during a query,"
                    + " thereby controlling query performance"})
    public int invertedIndexMaxExpansions = 50;

    @VariableMgr.VarAttr(name = INVERTED_INDEX_SKIP_THRESHOLD,
            description = {"在倒排索引中如果预估命中量占比总量超过百分比阈值，则跳过索引直接进行匹配。",
                    "In the inverted index,"
                            + " if the estimated hit ratio exceeds the percentage threshold of the total amount, "
                            + " then skip the index and proceed directly to matching."})
    public int invertedIndexSkipThreshold = 50;

    @VariableMgr.VarAttr(name = INVERTED_INDEX_COMPATIBLE_READ,
            description = {"兼容读取倒排索引，用于在x86和arm集群之间读取旧版本索引文件。",
                    "Compatible read for inverted index between x86 and arm, "
                            + "used to read old version index file from x86 in arm cluster"
                            + "or read old version index file from arm in x86 cluster"})
    public boolean invertedIndexCompatibleRead = false;

    @VariableMgr.VarAttr(name = SQL_DIALECT, needForward = true, checker = "checkSqlDialect",
            description = {"解析sql使用的方言", "The dialect used to parse sql."})
    public String sqlDialect = "doris";

    @VariableMgr.VarAttr(name = SERDE_DIALECT, needForward = true, checker = "checkSerdeDialect",
            description = {"返回给 MySQL 客户端时各数据类型的输出格式方言",
                    "The output format dialect of each data type returned to the MySQL client."},
            options = {"doris", "presto", "trino"})
    public String serdeDialect = "doris";

    @VariableMgr.VarAttr(name = ENABLE_UNIQUE_KEY_PARTIAL_UPDATE, needForward = true)
    public boolean enableUniqueKeyPartialUpdate = false;

    @VariableMgr.VarAttr(name = PARTIAL_UPDATE_NEW_KEY_BEHAVIOR, needForward = true, description = {
            "用于设置部分列更新中对于新插入的行的行为",
            "Used to set the behavior for newly inserted rows in partial update."
            }, checker = "checkPartialUpdateNewKeyBehavior", options = {"APPEND", "ERROR"})
    public String partialUpdateNewKeyPolicy = "APPEND";

    @VariableMgr.VarAttr(name = TEST_QUERY_CACHE_HIT, description = {
            "用于测试查询缓存是否命中，如果未命中指定类型的缓存，则会报错",
            "Used to test whether the query cache is hit. "
                    + "If the specified type of cache is not hit, an error will be reported."},
            options = {"none", "sql_cache", "partition_cache"})
    public String testQueryCacheHit = "none";

    @VariableMgr.VarAttr(name = ENABLE_AUTO_ANALYZE,
            description = {"该参数控制是否开启自动收集", "Set false to disable auto analyze"},
            flag = VariableMgr.GLOBAL)
    public volatile boolean enableAutoAnalyze = true;

    @VariableMgr.VarAttr(name = FORCE_SAMPLE_ANALYZE, needForward = true,
            description = {"是否将 full analyze 自动转换成 sample analyze", "Set true to force sample analyze"},
            flag = VariableMgr.GLOBAL)
    public boolean forceSampleAnalyze = Config.force_sample_analyze;

    @VariableMgr.VarAttr(name = ENABLE_AUTO_ANALYZE_INTERNAL_CATALOG,
            description = {"临时参数，收否自动收集所有内表", "Temp variable， enable to auto collect all OlapTable."},
            flag = VariableMgr.GLOBAL)
    public boolean enableAutoAnalyzeInternalCatalog = true;

    @VariableMgr.VarAttr(name = ENABLE_PARTITION_ANALYZE,
            description = {"临时参数，收否收集分区级别统计信息", "Temp variable， enable to collect partition level statistics."},
            flag = VariableMgr.GLOBAL)
    public boolean enablePartitionAnalyze = false;

    @VariableMgr.VarAttr(name = AUTO_ANALYZE_TABLE_WIDTH_THRESHOLD,
            description = {"参与自动收集的最大表宽度，列数多于这个参数的表不参与自动收集",
                "Maximum table width to enable auto analyze, "
                    + "table with more columns than this value will not be auto analyzed."},
            flag = VariableMgr.GLOBAL)
    public int autoAnalyzeTableWidthThreshold = 300;

    @VariableMgr.VarAttr(name = AUTO_ANALYZE_START_TIME, needForward = true, checker = "checkAnalyzeTimeFormat",
            description = {"该参数定义自动ANALYZE例程的开始时间",
                    "This parameter defines the start time for the automatic ANALYZE routine."},
            flag = VariableMgr.GLOBAL)
    public String autoAnalyzeStartTime = "00:00:00";

    @VariableMgr.VarAttr(name = AUTO_ANALYZE_END_TIME, needForward = true, checker = "checkAnalyzeTimeFormat",
            description = {"该参数定义自动ANALYZE例程的结束时间",
                    "This parameter defines the end time for the automatic ANALYZE routine."},
            flag = VariableMgr.GLOBAL)
    public String autoAnalyzeEndTime = "23:59:59";

    @VariableMgr.VarAttr(name = IGNORE_RUNTIME_FILTER_IDS,
            description = {"在IGNORE_RUNTIME_FILTER_IDS列表中的runtime filter将不会被生成",
                    "the runtime filter id in IGNORE_RUNTIME_FILTER_IDS list will not be generated"})

    public String ignoreRuntimeFilterIds = "";

    @VariableMgr.VarAttr(name = STATS_INSERT_MERGE_ITEM_COUNT, flag = VariableMgr.GLOBAL, description = {
            "控制统计信息相关INSERT攒批数量", "Controls the batch size for stats INSERT merging."
    }
    )
    public int statsInsertMergeItemCount = 200;

    @VariableMgr.VarAttr(name = HUGE_TABLE_DEFAULT_SAMPLE_ROWS, flag = VariableMgr.GLOBAL, description = {
            "定义开启开启大表自动sample后，对大表的采样比例",
            "This defines the number of sample percent for large tables when automatic sampling for"
                    + "large tables is enabled"

    })
    public long hugeTableDefaultSampleRows = 4194304;


    @VariableMgr.VarAttr(name = HUGE_TABLE_LOWER_BOUND_SIZE_IN_BYTES, flag = VariableMgr.GLOBAL,
            description = {
                    "大小超过该值的表将会自动通过采样收集统计信息",
                    "This defines the lower size bound for large tables. "
                            + "When enable_auto_sample is enabled, tables"
                            + "larger than this value will automatically collect "
                            + "statistics through sampling"})
    public long hugeTableLowerBoundSizeInBytes = 0;

    @VariableMgr.VarAttr(name = HUGE_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS, flag = VariableMgr.GLOBAL,
            description = {"控制对大表的自动ANALYZE的最小时间间隔，"
                    + "在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes的表仅ANALYZE一次",
                    "This controls the minimum time interval for automatic ANALYZE on large tables."
                            + "Within this interval,"
                            + "tables larger than huge_table_lower_bound_size_in_bytes are analyzed only once."})
    public long hugeTableAutoAnalyzeIntervalInMillis = TimeUnit.HOURS.toMillis(0);

    @VariableMgr.VarAttr(name = EXTERNAL_TABLE_AUTO_ANALYZE_INTERVAL_IN_MILLIS, flag = VariableMgr.GLOBAL,
            description = {"控制对外表的自动ANALYZE的最小时间间隔，在该时间间隔内的外表仅ANALYZE一次",
                    "This controls the minimum time interval for automatic ANALYZE on external tables."
                        + "Within this interval, external tables are analyzed only once."})
    public long externalTableAutoAnalyzeIntervalInMillis = TimeUnit.HOURS.toMillis(24);

    @VariableMgr.VarAttr(name = TABLE_STATS_HEALTH_THRESHOLD, flag = VariableMgr.GLOBAL,
            description = {"取值在0-100之间，当自上次统计信息收集操作之后"
                    + "数据更新量达到 (100 - table_stats_health_threshold)% ，认为该表的统计信息已过时",
                    "The value should be between 0 and 100. When the data update quantity "
                            + "exceeds (100 - table_stats_health_threshold)% since the last "
                            + "statistics collection operation, the statistics for this table are"
                            + "considered outdated."})
    public int tableStatsHealthThreshold = 90;

    @VariableMgr.VarAttr(name = PARTITION_SAMPLE_COUNT, flag = VariableMgr.GLOBAL,
            description = {
                    "大分区表采样的分区数上限",
                    "The upper limit of the number of partitions for sampling large partitioned tables.\n"})
    public int partitionSampleCount = 30;

    @VariableMgr.VarAttr(name = PARTITION_SAMPLE_ROW_COUNT, flag = VariableMgr.GLOBAL,
            description = {
                    "大分区表采样的行数上限",
                    "The upper limit of the number of rows for sampling large partitioned tables.\n"})
    public long partitionSampleRowCount = 3_000_000_000L;

    @VariableMgr.VarAttr(name = FETCH_HIVE_ROW_COUNT_SYNC,
            fuzzy = true,
            description = {"同步获取Hive外表行数", "Fetch Hive external table row count synchronously"})
    public boolean fetchHiveRowCountSync = true;

    @VariableMgr.VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE, needForward = true,
            description = {"是否开启基于结构信息的物化视图透明改写",
                    "Whether to enable materialized view rewriting based on struct info"})
    public boolean enableMaterializedViewRewrite = true;

    @VariableMgr.VarAttr(name = ALLOW_MODIFY_MATERIALIZED_VIEW_DATA, needForward = true,
            description = {"是否允许修改物化视图的数据",
                    "Is it allowed to modify the data of the materialized view"})
    public boolean allowModifyMaterializedViewData = false;

    @VariableMgr.VarAttr(name = ENABLE_MATERIALIZED_VIEW_REWRITE_WHEN_BASE_TABLE_UNAWARENESS,
            needForward = true,
            description = {"查询时，当物化视图存在无法实时感知数据的外表时，是否开启基于结构信息的物化视图透明改写",
                    ""})
    public boolean enableMaterializedViewRewriteWhenBaseTableUnawareness = false;
    @VariableMgr.VarAttr(name = MATERIALIZED_VIEW_REWRITE_SUCCESS_CANDIDATE_NUM, needForward = true,
            description = {"异步物化视图透明改写成功的结果集合，允许参与到CBO候选的最大数量",
                    "The max candidate num which participate in CBO when using asynchronous materialized views"})
    public int materializedViewRewriteSuccessCandidateNum = 3;

    @VariableMgr.VarAttr(name = ENABLE_DML_MATERIALIZED_VIEW_REWRITE, needForward = true,
            description = {"DML 时, 是否开启基于结构信息的物化视图透明改写",
                    "Whether to enable materialized view rewriting based on struct info"})
    public boolean enableDmlMaterializedViewRewrite = true;

    @VariableMgr.VarAttr(name = ENABLE_DML_MATERIALIZED_VIEW_REWRITE_WHEN_BASE_TABLE_UNAWARENESS,
            needForward = true,
            description = {"DML 时，当物化视图存在无法实时感知数据的外表时，是否开启基于结构信息的物化视图透明改写",
                    ""})
    public boolean enableDmlMaterializedViewRewriteWhenBaseTableUnawareness = false;

    @VariableMgr.VarAttr(name = MATERIALIZED_VIEW_RELATION_MAPPING_MAX_COUNT, needForward = true,
            description = {"透明改写过程中，relation mapping最大允许数量，如果超过，进行截取",
                    "During transparent rewriting, relation mapping specifies the maximum allowed number. "
                            + "If the number exceeds the allowed number, the number is intercepted"})
    public int materializedViewRelationMappingMaxCount = 8;

    @VariableMgr.VarAttr(name = ENABLE_MATERIALIZED_VIEW_UNION_REWRITE, needForward = true,
            description = {"当物化视图不足以提供查询的全部数据时，是否允许基表和物化视图 union 来响应查询",
                    "When the materialized view is not enough to provide all the data for the query, "
                            + "whether to allow the union of the base table and the materialized view to "
                            + "respond to the query"}, varType = VariableAnnotation.REMOVED)
    public boolean enableMaterializedViewUnionRewrite = true;

    @VariableMgr.VarAttr(name = ENABLE_MATERIALIZED_VIEW_NEST_REWRITE, needForward = true,
            description = {"是否允许嵌套物化视图改写",
                    "Whether enable materialized view nest rewrite"})
    public boolean enableMaterializedViewNestRewrite = false;

    @VariableMgr.VarAttr(name = ENABLE_SYNC_MV_COST_BASED_REWRITE, needForward = true,
            description = {"是否允许基于代价改写同步物化视图",
                    "Whether enable cost based rewrite for sync mv"}, varType = VariableAnnotation.REMOVED)
    public boolean enableSyncMvCostBasedRewrite = true;

    @VariableMgr.VarAttr(name = MATERIALIZED_VIEW_REWRITE_DURATION_THRESHOLD_MS, needForward = true,
            description = {"物化视图透明改写允许的最长耗时，超过此时长不再进行透明改写",
                    "The maximum duration allowed for transparent rewriting of materialized views; "
                            + "if this duration is exceeded, transparent rewriting will no longer be performed."})
    public long materializedViewRewriteDurationThresholdMs = 1000L;

    @VariableMgr.VarAttr(name = CREATE_TABLE_PARTITION_MAX_NUM, needForward = true,
            description = {"建表时创建分区的最大数量",
                    "The maximum number of partitions created during table creation"})
    public int createTablePartitionMaxNum = 10000;


    @VariableMgr.VarAttr(name = HIVE_PARQUET_USE_COLUMN_NAMES,
            description = {"默认情况下按名称访问 Parquet 列。将此属性设置为“false”可按 Hive 表定义中的序号位置访问列。",
                    "Access Parquet columns by name by default. Set this property to `false` to access columns "
                            + "by their ordinal position in the Hive table definition."})
    public boolean hiveParquetUseColumnNames = true;

    @VariableMgr.VarAttr(name = HIVE_ORC_USE_COLUMN_NAMES,
            description = {"默认情况下按名称访问 Orc 列。将此属性设置为“false”可按 Hive 表定义中的序号位置访问列。",
                    "Access Parquet columns by name by default. Set this property to `false` to access columns "
                            + "by their ordinal position in the Hive table definition."})
    public boolean hiveOrcUseColumnNames = true;

    @VariableMgr.VarAttr(name = KEEP_CARRIAGE_RETURN,
            description = {"在同时处理\r和\r\n作为CSV的行分隔符时，是否保留\r",
                    "When processing both \\n and \\r\\n as CSV line separators, should \\r be retained?"})
    public boolean keepCarriageReturn = false;

    @VariableMgr.VarAttr(name = EXCHANGE_MULTI_BLOCKS_BYTE_SIZE,
            description = {"Enable exchange to send multiple blocks in one RPC. Default is 256KB. A negative"
                    + " value disables multi-block exchange."})
    public int exchangeMultiBlocksByteSize = 256 * 1024;

    @VariableMgr.VarAttr(name = FORCE_JNI_SCANNER,
            fuzzy = true,
            description = {"强制使用jni方式读取外表", "Force the use of jni mode to read external table"})
    private boolean forceJniScanner = false;

    @VariableMgr.VarAttr(name = ENABLE_COUNT_PUSH_DOWN_FOR_EXTERNAL_TABLE,
            fuzzy = true,
            description = {"对外表启用 count(*) 下推优化", "enable count(*) pushdown optimization for external table"})
    private boolean enableCountPushDownForExternalTable = true;

    @VariableMgr.VarAttr(name = MINIMUM_OPERATOR_MEMORY_REQUIRED_KB, needForward = true,
            description = {"一个算子运行需要的最小的内存大小",
                    "The minimum memory required to be used by an operator, if not meet, the operator will not run"})
    public int minimumOperatorMemoryRequiredKB = 1000;

    public static final String IGNORE_RUNTIME_FILTER_IDS = "ignore_runtime_filter_ids";

    public static final String ENABLE_EXTERNAL_TABLE_BATCH_MODE = "enable_external_table_batch_mode";
    @VariableMgr.VarAttr(
            name = ENABLE_EXTERNAL_TABLE_BATCH_MODE,
            fuzzy = true,
            description = {"使能外表的batch mode功能", "Enable the batch mode function of the external table."},
            needForward = true)
    public boolean enableExternalTableBatchMode = true;

    @VariableMgr.VarAttr(name = SKEW_REWRITE_AGG_BUCKET_NUM, needForward = true,
            description = {"bucketNum参数控制count(distinct)倾斜优化的数据分布。决定不同值在worker间的分配方式，"
                    + "值越大越能处理极端倾斜但增加shuffle开销，值越小网络开销越低但可能无法完全解决倾斜。",
                    "The bucketNum parameter controls data distribution for skew optimization "
                            + "in count(distinct) queries. Determines how distinct values "
                            + "are distributed across workers to avoid data skew. "
                            + "Larger values better handle extreme skew but increase shuffle overhead. "
                            + "Smaller values reduce network traffic but may not fully resolve skew. "
            }, checker = "checkSkewRewriteAggBucketNum")
    public int skewRewriteAggBucketNum = 1024;

    public void setSkewRewriteAggBucketNum(int num) {
        this.skewRewriteAggBucketNum = num;
    }

    @VariableMgr.VarAttr(name = ENABLE_STRICT_CAST,
            description = {"cast使用严格模式", "Use strict mode for cast"})
    public boolean enableStrictCast = false;

    public Set<Integer> getIgnoredRuntimeFilterIds() {
        Set<Integer> ids = Sets.newLinkedHashSet();
        if (ignoreRuntimeFilterIds.isEmpty()) {
            return ImmutableSet.of();
        }
        for (String v : ignoreRuntimeFilterIds.split(",[\\s]*")) {
            int res = -1;
            if (!v.isEmpty()) {
                boolean isNumber = true;
                for (int i = 0; i < v.length(); ++i) {
                    char c = v.charAt(i);
                    if (c < '0' || c > '9') {
                        isNumber = false;
                        break;
                    }
                }
                if (isNumber) {
                    try {
                        res = Integer.parseInt(v);
                    } catch (Throwable t) {
                        // ignore
                    }
                }

            }
            ids.add(res);
        }
        return ids;
    }

    public void setIgnoreRuntimeFilterIds(String ignoreRuntimeFilterIds) {
        this.ignoreRuntimeFilterIds = ignoreRuntimeFilterIds;
    }

    public static final String IGNORE_SHAPE_NODE = "ignore_shape_nodes";

    public static final String DETAIL_SHAPE_NODES = "detail_shape_nodes";

    public static final String ENABLE_SEGMENT_CACHE = "enable_segment_cache";

    public Set<String> getIgnoreShapePlanNodes() {
        return Arrays.stream(ignoreShapePlanNodes.split(",[\\s]*")).collect(ImmutableSet.toImmutableSet());
    }

    public void setIgnoreShapePlanNodes(String ignoreShapePlanNodes) {
        this.ignoreShapePlanNodes = ignoreShapePlanNodes;
    }

    @VariableMgr.VarAttr(name = IGNORE_SHAPE_NODE,
            description = {"'explain shape plan' 命令中忽略的PlanNode 类型",
                    "the plan node type which is ignored in 'explain shape plan' command"})
    public String ignoreShapePlanNodes = "";

    @VariableMgr.VarAttr(name = DETAIL_SHAPE_NODES, needForward = true, setter = "setDetailShapePlanNodes",
            description = {"'explain shape plan' 命令中显示详细信息的PlanNode 类型",
                    "the plan node type show detail in 'explain shape plan' command"})
    public String detailShapePlanNodes = "";

    private Set<String> detailShapePlanNodesSet = ImmutableSet.of();

    public Set<String> getDetailShapePlanNodesSet() {
        return detailShapePlanNodesSet;
    }

    public void setDetailShapePlanNodes(String detailShapePlanNodes) {
        this.detailShapePlanNodesSet = Arrays.stream(detailShapePlanNodes.split(",[\\s]*"))
                .collect(ImmutableSet.toImmutableSet());
        this.detailShapePlanNodes = detailShapePlanNodes;
    }

    @VariableMgr.VarAttr(name = ENABLE_DECIMAL256, needForward = true, description = { "控制是否在计算过程中使用Decimal256类型",
            "Set to true to enable Decimal256 type" }, affectQueryResult = true)
    public boolean enableDecimal256 = false;

    @VariableMgr.VarAttr(name = FALLBACK_OTHER_REPLICA_WHEN_FIXED_CORRUPT, needForward = true,
            description = { "当开启use_fix_replica时遇到故障，是否漂移到其他健康的副本",
                "use other health replica when the use_fix_replica meet error" })
    public boolean fallbackOtherReplicaWhenFixedCorrupt = false;

    public static final String FE_DEBUG = "fe_debug";
    @VariableMgr.VarAttr(name = FE_DEBUG, needForward = true, fuzzy = true,
            description = {"when set true, FE will throw exceptions instead swallow them. This is used for test",
                    "when set true, FE will throw exceptions instead swallow them. This is used for test"})
    public boolean feDebug = false;

    @VariableMgr.VarAttr(name = SHOW_ALL_FE_CONNECTION,
            description = {"when it's true show processlist statement list all fe's connection",
                    "当变量为true时，show processlist命令展示所有fe的连接"})
    public boolean showAllFeConnection = false;

    @VariableMgr.VarAttr(name = MAX_MSG_SIZE_OF_RESULT_RECEIVER,
            description = {"Max message size during result deserialization, change this if you meet error"
                    + " like \"MaxMessageSize reached\"",
                    "用于控制结果反序列化时 thrift 字段的最大值，当遇到类似\"MaxMessageSize reached\"这样的错误时可以考虑修改该参数"})
    public int maxMsgSizeOfResultReceiver = TConfiguration.DEFAULT_MAX_MESSAGE_SIZE;


    // CLOUD_VARIABLES_BEGIN
    @VariableMgr.VarAttr(name = CLOUD_CLUSTER)
    public String cloudCluster = "";
    @VariableMgr.VarAttr(name = DISABLE_EMPTY_PARTITION_PRUNE)
    public boolean disableEmptyPartitionPrune = false;
    @VariableMgr.VarAttr(name = CLOUD_PARTITION_VERSION_CACHE_TTL_MS)
    public static long cloudPartitionVersionCacheTtlMs = 0;
    // CLOUD_VARIABLES_END

    // fetch remote schema rpc timeout
    @VariableMgr.VarAttr(name = FETCH_REMOTE_SCHEMA_TIMEOUT_SECONDS, fuzzy = true)
    public long fetchRemoteSchemaTimeoutSeconds = 120;
    // max tablet count for fetch remote schema
    @VariableMgr.VarAttr(name = MAX_FETCH_REMOTE_TABLET_COUNT, fuzzy = true)
    public int maxFetchRemoteTabletCount = 512;

    @VariableMgr.VarAttr(
            name = "enable_compress_materialize",
            description = {"控制是否启用compress materialize。",
                    "enable compress-materialize. "},
            needForward = true, fuzzy = false,
            varType = VariableAnnotation.EXPERIMENTAL
    )
    public boolean enableCompressMaterialize = false;

    @VariableMgr.VarAttr(
            name = DATA_QUEUE_MAX_BLOCKS,
            description = {"DataQueue 中每个子队列允许最大的 block 个数",
                    "Max blocks in DataQueue."},
            needForward = true, fuzzy = true)
    public long dataQueueMaxBlocks = 1;

    // for spill to disk
    @VariableMgr.VarAttr(
            name = ENABLE_SPILL,
            description = {"控制是否启用查询算子落盘。默认为 false。",
                    "Controls whether to enable spill to disk for query. "
                            + "The default value is false."},
            needForward = true, fuzzy = true)
    public boolean enableSpill = false;

    @VariableMgr.VarAttr(
            name = ENABLE_FORCE_SPILL,
            description = {"控制是否开启强制落盘（即使在内存足够的情况），默认为 false。",
                    "Controls whether enable force spill."
            },
            needForward = true, fuzzy = false
    )
    public boolean enableForceSpill = false;

    @VariableMgr.VarAttr(
            name = ENABLE_RESERVE_MEMORY,
            description = {"控制是否启用分配内存前先reverve memory的功能。默认为 true。",
                    "Controls whether to enable reserve memory before allocating memory. "
                            + "The default value is true."},
            needForward = true, fuzzy = true)
    public boolean enableReserveMemory = true;

    @VariableMgr.VarAttr(name = SPILL_MIN_REVOCABLE_MEM, fuzzy = true)
    public long spillMinRevocableMem = 32 * 1024 * 1024;

    // spill_sort_mem_limit controls the memory usage during merge sort phase of spill sort.
    // During merge sort phase, mutiple sorted blocks will be read into memory and do merge sort,
    // the count of blocks should be controlled or else will cause OOM, it's calculated as
    // std::max(spill_sort_mem_limit / spill_sort_batch_bytes, 2)
    @VariableMgr.VarAttr(name = SPILL_SORT_MEM_LIMIT)
    public long spillSortMemLimit = 134217728; // 128M

    @VariableMgr.VarAttr(name = SPILL_SORT_BATCH_BYTES)
    public long spillSortBatchBytes = 8388608; // 8M

    @VariableMgr.VarAttr(name = SPILL_AGGREGATION_PARTITION_COUNT, fuzzy = true)
    public int spillAggregationPartitionCount = 32;

    @VariableMgr.VarAttr(name = LOW_MEMORY_MODE_BUFFER_LIMIT, fuzzy = false)
    public long lowMemoryModeBufferLimit = 33554432;

    // The memory limit of streaming agg when spilling is enabled
    // NOTE: streaming agg operator will not spill to disk.
    @VariableMgr.VarAttr(name = SPILL_STREAMING_AGG_MEM_LIMIT, fuzzy = false)
    public long spillStreamingAggMemLimit = 268435456; //256MB

    @VariableMgr.VarAttr(name = SPILL_HASH_JOIN_PARTITION_COUNT, fuzzy = true)
    public int spillHashJoinPartitionCount = 32;

    @VariableMgr.VarAttr(name = SPILL_REVOCABLE_MEMORY_HIGH_WATERMARK_PERCENT, fuzzy = true)
    public int spillRevocableMemoryHighWatermarkPercent = -1;

    @VariableMgr.VarAttr(
            name = DUMP_HEAP_PROFILE_WHEN_MEM_LIMIT_EXCEEDED,
            description = {"查询因为内存不足被Cancel时，是否Dump heap profile到日志文件。默认为 false。",
                    "Whether to dump heap profile to log file when query is canceled becuase of memory not enough. "
                            + "The default value is false."},
            needForward = true)
    public boolean dumpHeapProfileWhenMemLimitExceeded = false;

    @VariableMgr.VarAttr(name = USE_MAX_LENGTH_OF_VARCHAR_IN_CTAS, needForward = true, description = {
            "在CTAS中，如果 CHAR / VARCHAR 列不来自于源表，是否是将这一列的长度设置为 MAX，即65533。默认为 true。",
            "In CTAS (Create Table As Select), if CHAR/VARCHAR columns do not originate from the source table,"
                    + " whether to set the length of such a column to MAX, which is 65533. The default is true."
    })
    public boolean useMaxLengthOfVarcharInCtas = true;

    // Whether enable segment cache. Segment cache only works when FE's query options sets enableSegmentCache true
    // along with BE's config `disable_segment_cache` false
    @VariableMgr.VarAttr(name = ENABLE_SEGMENT_CACHE, needForward = true)
    public boolean enableSegmentCache = true;

    /**
     * When enabling shard scroll, FE will plan scan ranges by shards of ES indices.
     * Otherwise, FE will plan a single query to ES.
     */
    @VariableMgr.VarAttr(name = ENABLE_ES_PARALLEL_SCROLL, description = {
        "ES catalog 是否开启 shard 级别并发的 scroll 请求，默认开启。",
        "Whether to enable shard-level parallel scroll requests for ES catalog, enabled by default."
    })
    public boolean enableESParallelScroll = true;

    @VariableMgr.VarAttr(name = ENABLE_MATCH_WITHOUT_INVERTED_INDEX, description = {
        "开启无索引match查询功能，建议正式环境保持开启",
        "Enable no-index match query functionality."
                + " it is recommended to keep this enabled in the production environment."
    })
    public boolean enableMatchWithoutInvertedIndex = true;

    @VariableMgr.VarAttr(name = ENABLE_FALLBACK_ON_MISSING_INVERTED_INDEX, description = {
        "开启后在没有找到索引的情况下直接查询报错，建议正式环境保持开启",
        "After enabling, it will directly query and report an error if no index is found."
                + " It is recommended to keep this enabled in the production environment."
    })
    public boolean enableFallbackOnMissingInvertedIndex = true;

    @VariableMgr.VarAttr(name = ENABLE_INVERTED_INDEX_SEARCHER_CACHE, description = {
        "开启后会缓存倒排索引searcher",
        "Enabling this will cache the inverted index searcher."
    })
    public boolean enableInvertedIndexSearcherCache = true;

    @VariableMgr.VarAttr(name = ENABLE_INVERTED_INDEX_QUERY_CACHE, description = {
        "开启后会缓存倒排索引查询结果",
        "Enabling this will cache the results of inverted index queries."
    })
    public boolean enableInvertedIndexQueryCache = true;

    @VariableMgr.VarAttr(name = IN_LIST_VALUE_COUNT_THRESHOLD, description = {
        "in条件value数量大于这个threshold后将不会走fast_execute",
        "When the number of values in the IN condition exceeds this threshold,"
                + " fast_execute will not be used."
    }, affectQueryResult = true)
    public int inListValueCountThreshold = 10;

    @VariableMgr.VarAttr(name = ENABLE_ADAPTIVE_PIPELINE_TASK_SERIAL_READ_ON_LIMIT, needForward = true, description = {
        "开启后将会允许自动调整 pipeline task 的并发数。当 scan 节点没有过滤条件，且 limit 参数小于"
            + "adaptive_pipeline_task_serial_read_on_limit 中指定的行数时，scanner 的并行度将会被设置为 1",
        "When enabled, the pipeline task concurrency will be adjusted automatically. When the scan node has no filter "
            + "conditions and the limit parameter is less than the number of rows specified in "
            + "adaptive_pipeline_task_serial_read_on_limit, the parallelism of the scan will be set to 1."
    })
    public boolean enableAdaptivePipelineTaskSerialReadOnLimit = true;

    @VariableMgr.VarAttr(name = ADAPTIVE_PIPELINE_TASK_SERIAL_READ_ON_LIMIT, needForward = true, description = {
        "当 enable_adaptive_pipeline_task_serial_read_on_limit 开启时，scanner 的并行度将会被设置为 1 的行数阈值",
            "When enable_adaptive_pipeline_task_serial_read_on_limit is enabled, "
            + "the number of rows at which the parallelism of the scan will be set to 1."
    })
    public int adaptivePipelineTaskSerialReadOnLimit = 10000;

    @VariableMgr.VarAttr(name = ENABLE_PHRASE_QUERY_SEQUENYIAL_OPT, needForward = true, description = {
        "开启顺序短语查询对连词的优化",
        "enable optimization for conjunctions in sequential phrase queries"
    })
    public boolean enablePhraseQuerySequentialOpt = true;

    @VariableMgr.VarAttr(name = REQUIRE_SEQUENCE_IN_INSERT, needForward = true, description = {
            "该变量用于控制，使用了sequence列的unique key表，insert into操作是否要求必须提供每一行的sequence列的值",
            "This variable controls whether the INSERT INTO operation on unique key tables with a sequence"
                    + " column requires a sequence column to be provided for each row"
    })
    public boolean requireSequenceInInsert = true;

    @VariableMgr.VarAttr(name = ENABLE_COOLDOWN_REPLICA_AFFINITY, needForward = true)
    public boolean enableCooldownReplicaAffinity = true;

    @VariableMgr.VarAttr(name = ENABLE_AUTO_CREATE_WHEN_OVERWRITE, needForward = true, description = {
        "开启后对自动分区表的 insert overwrite 操作会对没有找到分区的插入数据按自动分区规则创建分区，默认关闭",
        "The insert overwrite operation on an auto-partitioned table will create partitions for inserted data"
                + " for which no partition is found according to the auto-partitioning rules, which is turned off"
                + " by default."
    })
    public boolean enableAutoCreateWhenOverwrite = false;

    @VariableMgr.VarAttr(name = ENABLE_TEXT_VALIDATE_UTF8, needForward = true, description = {
            "对于 text 类型的文件读取，是否开启utf8编码检查。非utf8字符会显示成乱码。",
            "For text type file reading, whether to enable utf8 encoding check."
                    + "non-utf8 characters will be displayed as garbled characters."
    })
    public boolean enableTextValidateUtf8 = true;

    @VariableMgr.VarAttr(name = SKIP_CHECKING_ACID_VERSION_FILE, needForward = true, description = {
            "跳过检查 transactional hive 版本文件 '_orc_acid_version.'",
            "Skip checking transactional hive version file '_orc_acid_version.'"
    })
    public boolean skipCheckingAcidVersionFile = false;

    @VariableMgr.VarAttr(name = ENABLE_SQL_CONVERTOR_FEATURES, needForward = true,
            checker = "checkSqlConvertorFeatures",
            description = {
                    "开启 SQL 转换器的指定功能。多个功能使用逗号分隔",
                    "enable SQL convertor features. Multiple features are separated by commas"
            })
    public String enableSqlConvertorFeatures = "";

    // The default value is true,
    // which throughs reducing rpc call from follower node to meta service to improve query performance
    // for getting version is memory operation in master node,
    // but it will slightly increase the pressure on the FE master.
    @VariableMgr.VarAttr(name = ENABLE_SCHEMA_SCAN_FROM_MASTER_FE, description = {
            "在follower节点查询时, 是否允许从master节点扫描information_schema.tables的结果",
            "Whether to allow scanning information_schema.tables from the master node"
    })
    public boolean enableSchemaScanFromMasterFe = true;

    @VariableMgr.VarAttr(name = SHOW_COLUMN_COMMENT_IN_DESCRIBE, needForward = true,
            description = {
                    "是否在 DESCRIBE TABLE 语句中显示列注释",
                    "whether to show column comments in DESCRIBE TABLE statement"
            })
    public boolean showColumnCommentInDescribe = false;

    @VariableMgr.VarAttr(name = SQL_CONVERTOR_CONFIG, needForward = true,
            description = {
                    "SQL 转换器的相关配置，使用 Json 格式。以 {} 为根元素。",
                    "SQL convertor config, use Json format. The root element is {}"
            })
    public String sqlConvertorConfig = "{}";

    @VariableMgr.VarAttr(name = PREFER_UDF_OVER_BUILTIN, needForward = true,
            description = {
                    "是否优先查找 UDF 而不是内置函数",
                    "Whether to prefer UDF over builtin functions"
            })
    public boolean preferUdfOverBuiltin = false;

    @VariableMgr.VarAttr(name = SKEW_REWRITE_JOIN_SALT_EXPLODE_FACTOR, description = {
            "join 加盐优化的扩展因子, 对指定的倾斜值，join倾斜侧生成0到 ExplodeFactor - 1的随机值，"
                    + "join扩展侧复制为 ExplodeFactor 个副本，使hash shuffle之后计算负载均匀分布。"
                    + "可以配置为0-65535中的数字: 0代表根据集群中be的数量和cpu核数自适应，1-65535中的数量代表扩展倍数",
            "ExplodeFactor: The expansion factor for join skew optimization. "
                    + "For specified skewed values, it generates random values between 0 and ExplodeFactor-1 "
                    + "on the skewed side, while replicating the expanded side into ExplodeFactor copies,"
                    + "ensuring even load distribution after hash shuffling. "
                    + "Configurable range: 0-65535 (0=auto-adapt based on BEs and CPU cores;"
                    + "1-65535=manual expansion multiplier)"
    }, checker = "checkSkewRewriteJoinSaltExplodeFactor")
    public int skewRewriteJoinSaltExplodeFactor = 0;

    public void setEnableEsParallelScroll(boolean enableESParallelScroll) {
        this.enableESParallelScroll = enableESParallelScroll;
    }

    public boolean isEnableESParallelScroll() {
        return enableESParallelScroll;
    }

    @VariableMgr.VarAttr(name = ENABLE_ADD_INDEX_FOR_NEW_DATA, needForward = true, description = {
            "是否启用仅对新数据生效的索引添加模式，开启时新建索引只对后续写入的数据生效，关闭时对全部数据重建索引",
            "Whether to enable add index mode that only affects new data, "
                    + "when enabled new indexes only affect subsequently written data, "
                    + "when disabled rebuild indexes for all data"
    })
    public boolean enableAddIndexForNewData = false;

    // If this fe is in fuzzy mode, then will use initFuzzyModeVariables to generate some variables,
    // not the default value set in the code.
    @SuppressWarnings("checkstyle:Indentation")
    public void initFuzzyModeVariables() {
        Random random = new SecureRandom();
        this.feDebug = true;
        this.parallelPipelineTaskNum = random.nextInt(8);
        this.parallelPrepareThreshold = random.nextInt(32) + 1;
        this.enableCommonExprPushdown = random.nextBoolean();
        this.enableLocalExchange = random.nextBoolean();
        this.enableSharedExchangeSinkBuffer = random.nextBoolean();
        this.useSerialExchange = random.nextBoolean();
        this.enableCommonExpPushDownForInvertedIndex = random.nextBoolean();
        this.disableStreamPreaggregations = random.nextBoolean();
        this.enableShareHashTableForBroadcastJoin = random.nextBoolean();

        // 4KB = 4 * 1024 bytes
        int minBytes = 4 * 1024;
        // 10MB = 10 * 1024 * 1024 bytes
        int maxBytes = 10 * 1024 * 1024;
        this.exchangeMultiBlocksByteSize = minBytes + (int) (random.nextDouble() * (maxBytes - minBytes));
        int randomInt = random.nextInt(4);
        if (randomInt % 2 == 0) {
            this.rewriteOrToInPredicateThreshold = 100000;
            this.enableFunctionPushdown = false;
            this.enableSyncRuntimeFilterSize = true;
        } else {
            this.rewriteOrToInPredicateThreshold = 2;
            this.enableFunctionPushdown = true;
            this.enableSyncRuntimeFilterSize = false;
        }

        switch (random.nextInt(4)) {
            case 0:
                this.runtimeFilterType = TRuntimeFilterType.IN.getValue();
                break;
            case 1:
                this.runtimeFilterType = TRuntimeFilterType.BLOOM.getValue();
                break;
            case 2:
                this.runtimeFilterType = TRuntimeFilterType.IN_OR_BLOOM.getValue();
                break;
            case 3:
                break;
            default:
                break;
        }

        if (random.nextBoolean()) {
            this.runtimeFilterType |= TRuntimeFilterType.MIN_MAX.getValue();
        }

        switch (random.nextInt(3)) {
            case 0:
                this.fragmentTransmissionCompressionCodec = "snappy";
                this.runtimeFilterWaitTimeMs = 10;
                break;
            case 1:
                this.fragmentTransmissionCompressionCodec = "lz4";
                break;
            default:
                this.fragmentTransmissionCompressionCodec = "none";
        }

        this.enableParallelScan = random.nextInt(2) == 0;
        this.enableRuntimeFilterPrune = (randomInt % 10) == 0;
        this.enableRuntimeFilterPartitionPrune = (randomInt % 2) == 0;

        switch (randomInt) {
            case 0:
                this.parallelScanMaxScannersCount = 32;
                this.parallelScanMinRowsPerScanner = 64;
                this.runtimeFilterMaxInNum = 10;
                break;
            case 1:
                this.parallelScanMaxScannersCount = 16;
                this.parallelScanMinRowsPerScanner = 128;
                this.runtimeFilterMaxInNum = 0;
                break;
            case 2:
                this.parallelScanMaxScannersCount = 8;
                this.parallelScanMinRowsPerScanner = 256;
                break;
            case 3:
            default:
                break;
        }

        if (Config.fuzzy_test_type.equals("p0")) {
            if (random.nextInt(2) == 1) {
                this.batchSize = 4064;
                this.enableFoldConstantByBe = true;
            } else {
                this.batchSize = 1024;
                this.enableFoldConstantByBe = false;
            }

        }
        this.runtimeFilterWaitInfinitely = random.nextBoolean();

        // set random 1, 10, 100, 1000, 10000
        this.topnOptLimitThreshold = (int) Math.pow(10, random.nextInt(5));

        // for spill to disk
        switch (randomInt % 4) {
            case 0:
                this.spillMinRevocableMem = 0;
                break;
            case 1:
                this.spillMinRevocableMem = 1;
                break;
            case 2:
                this.spillMinRevocableMem = 1024 * 1024;
                break;
            case 3:
            default:
                this.spillMinRevocableMem = 100L * 1024 * 1024;
                break;
        }

        randomInt = random.nextInt(99);
        this.enableSpill = randomInt % 4 != 0;
        this.enableForceSpill = randomInt % 3 == 0;
        this.enableReserveMemory = randomInt % 5 != 0;
        setFuzzyForCatalog(random);
    }

    private void setFuzzyForCatalog(Random random) {
        if (!Config.fuzzy_test_type.equals("external")) {
            return;
        }
        // parquet
        this.enableParquetFilterByMinMax = random.nextBoolean();
        this.enableParquetLazyMat = random.nextBoolean();

        // orc
        this.enableOrcLazyMat = random.nextBoolean();
        this.orcTinyStripeThresholdBytes = Util.getRandomLong(0, 8L * 1024L * 1024L);
        this.orcOnceMaxReadBytes = Util.getRandomLong(0, 8L * 1024L * 1024L, 16L * 1024L * 1024L);
        this.orcMaxMergeDistanceBytes = Util.getRandomLong(0, 1L * 1024L * 1024L, 10L * 1024L * 1024L);

        // jni
        this.forceJniScanner = random.nextBoolean();

        // statistics
        this.fetchHiveRowCountSync = random.nextBoolean();

        // hive
        this.hiveTextCompression = Util.getRandomString(
                "gzip", "defalte", "bzip2", "zstd", "lz4", "lzo", "snappy", "plain");

        // batch mode
        this.enableExternalTableBatchMode = random.nextBoolean();
        if (this.enableExternalTableBatchMode) {
            this.numPartitionsInBatchMode = Util.getRandomInt(0, 1024, Integer.MAX_VALUE);
            this.numFilesInBatchMode = Util.getRandomInt(0, 1024, Integer.MAX_VALUE);
        }

        // common
        this.enableCountPushDownForExternalTable = random.nextBoolean();
        this.runtimeFilterPruneForExternal = random.nextBoolean();
    }

    public String printFuzzyVariables() {
        if (!Config.use_fuzzy_session_variable) {
            return "";
        }
        List<String> res = Lists.newArrayList();
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null || !attr.fuzzy()) {
                continue;
            }
            field.setAccessible(true);
            try {
                Object val = field.get(this);
                res.add(attr.name() + "=" + val.toString());
            } catch (IllegalAccessException e) {
                LOG.warn("failed to get fuzzy session variable {}", attr.name(), e);
            }
        }
        return Joiner.on(",").join(res);
    }

    /**
     * syntax:
     * all -> use all event
     * all except event_1, event_2, ..., event_n -> use all events excluding the event_1~n
     * event_1, event_2, ..., event_n -> use event_1~n
     */
    @VariableMgr.VarAttr(name = NEREIDS_TRACE_EVENT_MODE, checker = "checkNereidsTraceEventMode")
    public String nereidsTraceEventMode = "all";

    private Set<Class<? extends Event>> parsedNereidsEventMode = EventSwitchParser.parse(Lists.newArrayList("all"));

    public boolean isInDebugMode() {
        return showHiddenColumns || skipDeleteBitmap || skipDeletePredicate || skipDeleteSign || skipStorageEngineMerge;
    }

    public String printDebugModeVariables() {
        List<String> res = Lists.newArrayList();
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }
            boolean shouldPrint = false;
            for (String debugVariable : DEBUG_VARIABLES) {
                if (attr.name().equalsIgnoreCase(debugVariable)) {
                    shouldPrint = true;
                    break;
                }
            }
            if (shouldPrint) {
                field.setAccessible(true);
                try {
                    Object val = field.get(this);
                    res.add(attr.name() + "=" + val.toString());
                } catch (IllegalAccessException e) {
                    LOG.warn("failed to get debug mode session variable {}", attr.name(), e);
                }
            }
        }
        return Joiner.on(",").join(res);
    }

    public void setEnableNereidsTrace(boolean enableNereidsTrace) {
        this.enableNereidsTrace = enableNereidsTrace;
    }

    public void setNereidsTraceEventMode(String nereidsTraceEventMode) {
        checkNereidsTraceEventMode(nereidsTraceEventMode);
        this.nereidsTraceEventMode = nereidsTraceEventMode;
    }

    public void checkNereidsTraceEventMode(String nereidsTraceEventMode) {
        List<String> strings = EventSwitchParser.checkEventModeStringAndSplit(nereidsTraceEventMode);
        if (strings != null) {
            parsedNereidsEventMode = EventSwitchParser.parse(strings);
        }
        if (parsedNereidsEventMode == null) {
            throw new UnsupportedOperationException("nereids_trace_event_mode syntax error, please check");
        }
    }

    public Set<Class<? extends Event>> getParsedNereidsEventMode() {
        return parsedNereidsEventMode;
    }

    public String getBlockEncryptionMode() {
        return blockEncryptionMode;
    }

    public void setBlockEncryptionMode(String blockEncryptionMode) {
        this.blockEncryptionMode = blockEncryptionMode;
    }

    public void setRewriteOrToInPredicateThreshold(int threshold) {
        this.rewriteOrToInPredicateThreshold = threshold;
    }

    public int getRewriteOrToInPredicateThreshold() {
        return rewriteOrToInPredicateThreshold;
    }

    public long getMaxExecMemByte() {
        return maxExecMemByte;
    }

    public long getMaxScanQueueExecMemByte() {
        return maxScanQueueMemByte;
    }

    public int getNumScannerThreads() {
        return numScannerThreads;
    }

    public int getQueryTimeoutS() {
        return queryTimeoutS;
    }

    public void setEnableTwoPhaseReadOpt(boolean enable) {
        enableTwoPhaseReadOpt = enable;
    }

    public int getMaxExecutionTimeMS() {
        return maxExecutionTimeMS;
    }

    public int getInsertTimeoutS() {
        return insertTimeoutS;
    }


    public void setInsertTimeoutS(int insertTimeoutS) {
        this.insertTimeoutS = insertTimeoutS;
    }

    public boolean enableProfile() {
        return enableProfile;
    }

    public int getAutoProfileThresholdMs() {
        return this.autoProfileThresholdMs;
    }

    public boolean enableSingleDistinctColumnOpt() {
        return enableSingleDistinctColumnOpt;
    }

    public int getWaitTimeoutS() {
        return waitTimeoutS;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    public boolean isEnableJoinReorderBasedCost() {
        return enableJoinReorderBasedCost;
    }

    public boolean enableMultiClusterSyncLoad() {
        return cloudEnableMultiClusterSyncLoad;
    }

    public void setEnableMultiClusterSyncLoad(boolean cloudEnableMultiClusterSyncLoad) {
        this.cloudEnableMultiClusterSyncLoad = cloudEnableMultiClusterSyncLoad;
    }

    public boolean isTxReadonly() {
        return txReadonly;
    }

    public boolean isTransactionReadonly() {
        return transactionReadonly;
    }

    public String getTransactionIsolation() {
        return transactionIsolation;
    }

    public String getTxIsolation() {
        return txIsolation;
    }

    public String getCharsetClient() {
        return charsetClient;
    }

    public String getCharsetConnection() {
        return charsetConnection;
    }

    public String getCharsetResults() {
        return charsetResults;
    }

    public String getCharsetServer() {
        return charsetServer;
    }

    public String getCollationConnection() {
        return collationConnection;
    }

    public String getCollationDatabase() {
        return collationDatabase;
    }

    public String getCollationServer() {
        return collationServer;
    }

    public boolean isSqlAutoIsNull() {
        return sqlAutoIsNull;
    }

    public long getSqlSelectLimit() {
        if (sqlSelectLimit < 0 || sqlSelectLimit >= Long.MAX_VALUE) {
            return -1;
        }
        return sqlSelectLimit;
    }

    public long getDefaultOrderByLimit() {
        return defaultOrderByLimit;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public int getQueryCacheType() {
        return queryCacheType;
    }

    public int getInteractiveTimeout() {
        return interactiveTimeout;
    }

    public int getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public int getNetReadTimeout() {
        return netReadTimeout;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public int getSqlSafeUpdates() {
        return sqlSafeUpdates;
    }

    public int getNetBufferLength() {
        return netBufferLength;
    }

    public boolean getHaveQueryCache() {
        return haveQueryCache;
    }

    /**
     * setMaxExecMemByte.
     **/
    public void setMaxExecMemByte(long maxExecMemByte) {
        if (maxExecMemByte < MIN_EXEC_MEM_LIMIT) {
            this.maxExecMemByte = MIN_EXEC_MEM_LIMIT;
        } else {
            this.maxExecMemByte = maxExecMemByte;
        }
    }

    public void setMaxScanQueueMemByte(long scanQueueMemByte) {
        this.maxScanQueueMemByte = scanQueueMemByte;
    }

    public void setNumScannerThreads(int numScannerThreads) {
        this.numScannerThreads = numScannerThreads;
    }

    public boolean isSqlQuoteShowCreate() {
        return sqlQuoteShowCreate;
    }

    public void setSqlQuoteShowCreate(boolean sqlQuoteShowCreate) {
        this.sqlQuoteShowCreate = sqlQuoteShowCreate;
    }

    public void setQueryTimeoutS(int queryTimeoutS) {
        if (queryTimeoutS <= 0) {
            LOG.warn("Setting invalid query timeout", new RuntimeException(""));
        }
        this.queryTimeoutS = queryTimeoutS;
    }

    // This method will be called by VariableMgr.replayGlobalVariableV2
    // We dont want any potential exception is thrown during replay oplog
    // so we do not check its validation. Here potential excaption
    // will become real in cases where user set global query timeout 0 before
    // upgrading to this version.
    public void setQueryTimeoutS(String queryTimeoutS) {
        int newQueryTimeoutS = Integer.valueOf(queryTimeoutS);
        if (newQueryTimeoutS <= 0) {
            LOG.warn("Invalid query timeout: {}", newQueryTimeoutS, new RuntimeException(""));
        }
        this.queryTimeoutS = newQueryTimeoutS;
    }

    public void setAnalyzeTimeoutS(int analyzeTimeoutS) {
        this.analyzeTimeoutS = analyzeTimeoutS;
    }

    public void setMaxExecutionTimeMS(int maxExecutionTimeMS) {
        this.maxExecutionTimeMS = maxExecutionTimeMS;
        this.queryTimeoutS = this.maxExecutionTimeMS / 1000;
        if (queryTimeoutS <= 0) {
            LOG.warn("Invalid query timeout: {}", queryTimeoutS, new RuntimeException(""));
        }
    }

    public void setMaxExecutionTimeMS(String maxExecutionTimeMS) {
        this.maxExecutionTimeMS = Integer.valueOf(maxExecutionTimeMS);
        this.queryTimeoutS = this.maxExecutionTimeMS / 1000;
        if (queryTimeoutS <= 0) {
            LOG.warn("Invalid query timeout: {}", queryTimeoutS, new RuntimeException(""));
        }
    }

    public void setPipelineTaskNum(String value) throws Exception {
        int val = checkFieldValue(PARALLEL_PIPELINE_TASK_NUM, 0, value);
        this.parallelPipelineTaskNum = val;
    }

    public void setFragmentInstanceNum(String value) {}

    public void setOrcTinyStripeThresholdBytes(String value) throws Exception {
        long val = checkFieldLongValue(ORC_TINY_STRIPE_THRESHOLD_BYTES, 0, value);
        this.orcTinyStripeThresholdBytes = val;
    }

    public void setOrcOnceMaxReadBytes(String value)  throws Exception {
        long val = checkFieldLongValue(ORC_ONCE_MAX_READ_BYTES, 0, value);
        this.orcOnceMaxReadBytes = val;
    }

    public void setOrcMaxMergeDistanceBytes(String value)  throws Exception {
        long val = checkFieldLongValue(ORC_MAX_MERGE_DISTANCE_BYTES, 0, value);
        this.orcMaxMergeDistanceBytes = val;
    }

    public void setFullSortMaxBufferedBytes(String value) throws Exception {
        long val = Long.parseLong(value);
        if (val < 16L * 1024L * 1024L || val > 1024L * 1024L * 1024L) {
            throw new Exception(
                    FULL_SORT_MAX_BUFFERED_BYTES + " value should between 16MB and 1GB , you set value is: " + value);
        }
        this.fullSortMaxBufferedBytes = val;
    }

    private long checkFieldLongValue(String variableName, long minValue, String value) throws Exception {
        long val = Long.parseLong(value);
        if (val < minValue) {
            throw new Exception(
                    variableName + " value should greater than or equal " + String.valueOf(minValue)
                            + ", you set value is: " + value);
        }
        return val;
    }


    private int checkFieldValue(String variableName, int minValue, String value) throws Exception {
        int val = Integer.valueOf(value);
        if (val < minValue) {
            throw new Exception(
                    variableName + " value should greater than or equal " + String.valueOf(minValue)
                            + ", you set value is: " + value);
        }
        return val;
    }

    public String getWorkloadGroup() {
        return workloadGroup;
    }

    public void setWorkloadGroup(String workloadGroup) {
        this.workloadGroup = workloadGroup;
    }

    public boolean getBypassWorkloadGroup() {
        return this.bypassWorkloadGroup;
    }

    public int getMaxColumnReaderNum() {
        return this.maxColumnReaderNum;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public boolean isDisableFileCache() {
        return Config.isCloudMode() ? disableFileCache : false;
    }

    public void setDisableFileCache(boolean disableFileCache) {
        this.disableFileCache = disableFileCache;
    }

    public boolean isDisableColocatePlan() {
        return disableColocatePlan;
    }

    public boolean isEnableBucketShuffleJoin() {
        return enableBucketShuffleJoin;
    }

    public boolean isEnableOdbcTransaction() {
        return enableOdbcTransaction;
    }

    public String getPreferJoinMethod() {
        return preferJoinMethod;
    }

    public void setPreferJoinMethod(String preferJoinMethod) {
        this.preferJoinMethod = preferJoinMethod;
    }

    public boolean isEnableFoldConstantByBe() {
        return enableFoldConstantByBe;
    }

    public boolean isDebugSkipFoldConstant() {
        return debugSkipFoldConstant;
    }

    public boolean isEnableRewriteElementAtToSlot() {
        return enableRewriteElementAtToSlot;
    }

    public void setEnableRewriteElementAtToSlot(boolean rewriteElementAtToSlot) {
        enableRewriteElementAtToSlot = rewriteElementAtToSlot;
    }

    public void setEnableFoldConstantByBe(boolean foldConstantByBe) {
        this.enableFoldConstantByBe = foldConstantByBe;
    }

    public void setDebugSkipFoldConstant(boolean debugSkipFoldConstant) {
        this.debugSkipFoldConstant = debugSkipFoldConstant;
    }

    public int getParallelExecInstanceNum() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getEnv() != null && connectContext.getEnv().getAuth() != null) {
            int userParallelExecInstanceNum = connectContext.getEnv().getAuth()
                    .getParallelFragmentExecInstanceNum(connectContext.getQualifiedUser());
            if (userParallelExecInstanceNum > 0) {
                return userParallelExecInstanceNum;
            }
        }
        if (parallelPipelineTaskNum == 0) {
            int size = Env.getCurrentSystemInfo().getMinPipelineExecutorSize();
            int autoInstance = (size + 1) / 2;
            return Math.min(autoInstance, maxInstanceNum);
        } else {
            return parallelPipelineTaskNum;
        }
    }

    public int getExchangeInstanceParallel() {
        return exchangeInstanceParallel;
    }

    public boolean getEnableInsertStrict() {
        return enableInsertStrict;
    }

    public void setEnableInsertStrict(boolean enableInsertStrict) {
        this.enableInsertStrict = enableInsertStrict;
    }

    public double getInsertMaxFilterRatio() {
        return insertMaxFilterRatio;
    }

    public void setInsertMaxFilterRatio(double maxFilterRatio) {
        this.insertMaxFilterRatio = maxFilterRatio;
    }

    public boolean isEnableSqlCache() {
        return enableSqlCache;
    }

    public void setEnableSqlCache(boolean enableSqlCache) {
        this.enableSqlCache = enableSqlCache;
    }

    public boolean getEnableQueryCache() {
        return enableQueryCache;
    }

    public void setEnableQueryCache(boolean enableQueryCache) {
        this.enableQueryCache = enableQueryCache;
    }

    public boolean isQueryCacheForceRefresh() {
        return queryCacheForceRefresh;
    }

    public void setQueryCacheForceRefresh(boolean queryCacheForceRefresh) {
        this.queryCacheForceRefresh = queryCacheForceRefresh;
    }

    public long getQueryCacheEntryMaxBytes() {
        return queryCacheEntryMaxBytes;
    }

    public void setQueryCacheEntryMaxBytes(long queryCacheEntryMaxBytes) {
        this.queryCacheEntryMaxBytes = queryCacheEntryMaxBytes;
    }

    public long getQueryCacheEntryMaxRows() {
        return queryCacheEntryMaxRows;
    }

    public void setQueryCacheEntryMaxRows(long queryCacheEntryMaxRows) {
        this.queryCacheEntryMaxRows = queryCacheEntryMaxRows;
    }

    // Serialize to thrift object
    public boolean getForwardToMaster() {
        return forwardToMaster;
    }

    public boolean isUseV2Rollup() {
        return useV2Rollup;
    }

    // for unit test
    public void setUseV2Rollup(boolean useV2Rollup) {
        this.useV2Rollup = useV2Rollup;
    }

    public boolean isRewriteCountDistinct() {
        return rewriteCountDistinct;
    }

    public void setRewriteCountDistinct(boolean rewriteCountDistinct) {
        this.rewriteCountDistinct = rewriteCountDistinct;
    }

    public String getEventScheduler() {
        return eventScheduler;
    }

    public void setEventScheduler(String eventScheduler) {
        this.eventScheduler = eventScheduler;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getMaxPushdownConditionsPerColumn() {
        return maxPushdownConditionsPerColumn;
    }

    public void setMaxPushdownConditionsPerColumn(int maxPushdownConditionsPerColumn) {
        this.maxPushdownConditionsPerColumn = maxPushdownConditionsPerColumn;
    }

    public double getBroadcastRightTableScaleFactor() {
        return broadcastRightTableScaleFactor;
    }

    public void setBroadcastRightTableScaleFactor(double broadcastRightTableScaleFactor) {
        this.broadcastRightTableScaleFactor = broadcastRightTableScaleFactor;
    }

    public double getLeftSemiOrAntiProbeFactor() {
        return leftSemiOrAntiProbeFactor;
    }

    public void setLeftSemiOrAntiProbeFactor(double leftSemiOrAntiProbeFactor) {
        this.leftSemiOrAntiProbeFactor = leftSemiOrAntiProbeFactor;
    }

    public double getBroadcastRowCountLimit() {
        return broadcastRowCountLimit;
    }

    public void setBroadcastRowCountLimit(double broadcastRowCountLimit) {
        this.broadcastRowCountLimit = broadcastRowCountLimit;
    }

    public double getBroadcastHashtableMemLimitPercentage() {
        return broadcastHashtableMemLimitPercentage;
    }

    public void setBroadcastHashtableMemLimitPercentage(double broadcastHashtableMemLimitPercentage) {
        this.broadcastHashtableMemLimitPercentage = broadcastHashtableMemLimitPercentage;
    }

    public boolean showHiddenColumns() {
        return showHiddenColumns;
    }

    public void setShowHiddenColumns(boolean showHiddenColumns) {
        this.showHiddenColumns = showHiddenColumns;
    }

    public boolean skipStorageEngineMerge() {
        return skipStorageEngineMerge;
    }

    public boolean skipDeleteSign() {
        return skipDeleteSign;
    }

    public boolean isAllowPartitionColumnNullable() {
        return allowPartitionColumnNullable;
    }

    public String getRuntimeFilterMode() {
        return runtimeFilterMode;
    }

    public void setRuntimeFilterMode(String runtimeFilterMode) {
        this.runtimeFilterMode = runtimeFilterMode;
    }

    public int getRuntimeBloomFilterSize() {
        return runtimeBloomFilterSize;
    }

    public void setRuntimeBloomFilterSize(int runtimeBloomFilterSize) {
        this.runtimeBloomFilterSize = runtimeBloomFilterSize;
    }

    public int getRuntimeBloomFilterMinSize() {
        return runtimeBloomFilterMinSize;
    }

    public void setRuntimeBloomFilterMinSize(int runtimeBloomFilterMinSize) {
        this.runtimeBloomFilterMinSize = runtimeBloomFilterMinSize;
    }

    public int getRuntimeBloomFilterMaxSize() {
        return runtimeBloomFilterMaxSize;
    }

    public void setRuntimeBloomFilterMaxSize(int runtimeBloomFilterMaxSize) {
        this.runtimeBloomFilterMaxSize = runtimeBloomFilterMaxSize;
    }

    public int getRuntimeFilterWaitTimeMs() {
        return runtimeFilterWaitTimeMs;
    }

    public void setRuntimeFilterWaitTimeMs(int runtimeFilterWaitTimeMs) {
        this.runtimeFilterWaitTimeMs = runtimeFilterWaitTimeMs;
    }

    public int getRuntimeFiltersMaxNum() {
        return runtimeFiltersMaxNum;
    }

    public void setRuntimeFiltersMaxNum(int runtimeFiltersMaxNum) {
        this.runtimeFiltersMaxNum = runtimeFiltersMaxNum;
    }

    public int getRuntimeFilterType() {
        return runtimeFilterType;
    }

    public boolean allowedRuntimeFilterType(TRuntimeFilterType type) {
        return RuntimeFilterTypeHelper.allowedRuntimeFilterType(runtimeFilterType, type);
    }

    public void setRuntimeFilterType(int runtimeFilterType) {
        this.runtimeFilterType = runtimeFilterType;
    }

    public int getRuntimeFilterMaxInNum() {
        return runtimeFilterMaxInNum;
    }

    public void setRuntimeFilterMaxInNum(int runtimeFilterMaxInNum) {
        this.runtimeFilterMaxInNum = runtimeFilterMaxInNum;
    }

    public void setEnableLocalShuffle(boolean enableLocalShuffle) {
        this.enableLocalShuffle = enableLocalShuffle;
    }

    public boolean enablePushDownNoGroupAgg() {
        return enablePushDownNoGroupAgg;
    }

    public boolean getEnableFunctionPushdown() {
        return this.enableFunctionPushdown;
    }

    public boolean getForbidUnknownColStats() {
        return forbidUnknownColStats;
    }

    public void setForbidUnknownColStats(boolean forbid) {
        forbidUnknownColStats = forbid;
    }

    public boolean getEnableLocalExchange() {
        return enableLocalExchange;
    }

    public boolean getEnableCboStatistics() {
        return enableCboStatistics;
    }

    public long getFileSplitSize() {
        return fileSplitSize;
    }

    public void setFileSplitSize(long fileSplitSize) {
        this.fileSplitSize = fileSplitSize;
    }

    public int getNumPartitionsInBatchMode() {
        return numPartitionsInBatchMode;
    }

    public int getNumFilesInBatchMode() {
        return numFilesInBatchMode;
    }

    public long getFetchSplitsMaxWaitTime() {
        return fetchSplitsMaxWaitTime;
    }

    public void setFetchSplitsMaxWaitTime(long fetchSplitsMaxWaitTime) {
        this.fetchSplitsMaxWaitTime = fetchSplitsMaxWaitTime;
    }

    public boolean isEnableOrcLazyMat() {
        return enableOrcLazyMat;
    }

    public void setEnableOrcLazyMat(boolean enableOrcLazyMat) {
        this.enableOrcLazyMat = enableOrcLazyMat;
    }

    public boolean isEnableOrcFilterByMinMax() {
        return enableOrcFilterByMinMax;
    }

    public void setEnableOrcFilterByMinMax(boolean enableOrcFilterByMinMax) {
        this.enableOrcFilterByMinMax = enableOrcFilterByMinMax;
    }

    public boolean isCheckOrcInitSargsSuccess() {
        return checkOrcInitSargsSuccess;
    }

    public void setCheckOrcInitSargsSuccess(boolean checkOrcInitSargsSuccess) {
        this.checkOrcInitSargsSuccess = checkOrcInitSargsSuccess;
    }

    public String getSqlDialect() {
        return sqlDialect;
    }

    public String[] getSqlConvertorFeatures() {
        return enableSqlConvertorFeatures.split(",");
    }

    public String getSqlConvertorConfig() {
        return sqlConvertorConfig;
    }

    public Dialect getSqlParseDialect() {
        return Dialect.getByName(sqlDialect);
    }

    public void setSqlDialect(String sqlDialect) {
        this.sqlDialect = sqlDialect == null ? null : sqlDialect.toLowerCase();
    }

    /**
     * getInsertVisibleTimeoutMs.
     **/
    public long getInsertVisibleTimeoutMs() {
        if (insertVisibleTimeoutMs < MIN_INSERT_VISIBLE_TIMEOUT_MS) {
            return MIN_INSERT_VISIBLE_TIMEOUT_MS;
        } else {
            return insertVisibleTimeoutMs;
        }
    }

    /**
     * setInsertVisibleTimeoutMs.
     **/
    public void setInsertVisibleTimeoutMs(long insertVisibleTimeoutMs) {
        if (insertVisibleTimeoutMs < MIN_INSERT_VISIBLE_TIMEOUT_MS) {
            this.insertVisibleTimeoutMs = MIN_INSERT_VISIBLE_TIMEOUT_MS;
        } else {
            this.insertVisibleTimeoutMs = insertVisibleTimeoutMs;
        }
    }

    public boolean getIsSingleSetVar() {
        return isSingleSetVar;
    }

    public void setIsSingleSetVar(boolean issinglesetvar) {
        this.isSingleSetVar = issinglesetvar;
    }

    public Map<SessionVariableField, String> getSessionOriginValue() {
        return sessionOriginValue;
    }

    public void addSessionOriginValue(SessionVariableField key, String value) {
        if (sessionOriginValue.containsKey(key)) {
            // If we already set the origin value, just skip the reset.
            return;
        }
        sessionOriginValue.put(key, value);
    }

    public void clearSessionOriginValue() {
        sessionOriginValue.clear();
    }

    public boolean isDeleteWithoutPartition() {
        return deleteWithoutPartition;
    }

    public boolean isExtractWideRangeExpr() {
        return extractWideRangeExpr;
    }

    public boolean isGroupByAndHavingUseAliasFirst() {
        return groupByAndHavingUseAliasFirst;
    }

    public int getCpuResourceLimit() {
        return cpuResourceLimit;
    }

    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    public boolean isEnableParallelOutfile() {
        return enableParallelOutfile;
    }

    public boolean isDisableJoinReorder() {
        return disableJoinReorder;
    }

    public boolean isEnableBushyTree() {
        return enableBushyTree;
    }

    public void setEnableBushyTree(boolean enableBushyTree) {
        this.enableBushyTree = enableBushyTree;
    }

    public boolean isEnablePartitionTopN() {
        return enablePartitionTopN;
    }

    public void setEnablePartitionTopN(boolean enablePartitionTopN) {
        this.enablePartitionTopN = enablePartitionTopN;
    }

    public double getGlobalPartitionTopNThreshold() {
        return globalPartitionTopNThreshold;
    }

    public void setGlobalPartitionTopnThreshold(int threshold) {
        this.globalPartitionTopNThreshold = threshold;
    }

    public boolean isEnableFoldNondeterministicFn() {
        return enableFoldNondeterministicFn;
    }

    public void setEnableFoldNondeterministicFn(boolean enableFoldNondeterministicFn) {
        this.enableFoldNondeterministicFn = enableFoldNondeterministicFn;
    }

    public boolean isReturnObjectDataAsBinary() {
        return returnObjectDataAsBinary;
    }

    public void setReturnObjectDataAsBinary(boolean returnObjectDataAsBinary) {
        this.returnObjectDataAsBinary = returnObjectDataAsBinary;
    }

    public boolean isEnableInferPredicate() {
        return enableInferPredicate;
    }

    public void setEnableInferPredicate(boolean enableInferPredicate) {
        this.enableInferPredicate = enableInferPredicate;
    }

    public boolean isEnableProjection() {
        return enableProjection;
    }

    public boolean isEnableShortCircuitQuery() {
        return enableShortCircuitQuery;
    }

    public boolean checkOverflowForDecimal() {
        return checkOverflowForDecimal;
    }

    public boolean isTrimTailingSpacesForExternalTableQuery() {
        return trimTailingSpacesForExternalTableQuery;
    }

    public void setTrimTailingSpacesForExternalTableQuery(boolean trimTailingSpacesForExternalTableQuery) {
        this.trimTailingSpacesForExternalTableQuery = trimTailingSpacesForExternalTableQuery;
    }

    public void setEnableJoinReorderBasedCost(boolean enableJoinReorderBasedCost) {
        this.enableJoinReorderBasedCost = enableJoinReorderBasedCost;
    }

    public void setDisableJoinReorder(boolean disableJoinReorder) {
        this.disableJoinReorder = disableJoinReorder;
    }

    public boolean isEnablePushDownMinMaxOnUnique() {
        return enablePushDownMinMaxOnUnique;
    }

    public void setEnablePushDownMinMaxOnUnique(boolean enablePushDownMinMaxOnUnique) {
        this.enablePushDownMinMaxOnUnique = enablePushDownMinMaxOnUnique;
    }

    public boolean isEnablePushDownStringMinMax() {
        return enablePushDownStringMinMax;
    }

    /** canUseNereidsDistributePlanner */
    public static boolean canUseNereidsDistributePlanner() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return true;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        StatementContext statementContext = connectContext.getStatementContext();
        if (statementContext != null) {
            StatementBase parsedStatement = statementContext.getParsedStatement();
            if (!(parsedStatement instanceof LogicalPlanAdapter)) {
                return false;
            }
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStatement).getLogicalPlan();
            // TODO: support other sink
            if (!(logicalPlan instanceof UnboundResultSink
                    || logicalPlan instanceof LogicalFileSink
                    || logicalPlan instanceof InsertIntoTableCommand)) {
                return false;
            }
        }
        return sessionVariable.enableNereidsDistributePlanner;
    }

    public boolean isEnableNereidsDistributePlanner() {
        return enableNereidsDistributePlanner;
    }

    public void setEnableNereidsDistributePlanner(boolean enableNereidsDistributePlanner) {
        this.enableNereidsDistributePlanner = enableNereidsDistributePlanner;
    }

    public int getNthOptimizedPlan() {
        return nthOptimizedPlan;
    }

    public Set<String> getDisableNereidsRuleNames() {
        String checkPrivilege = RuleType.CHECK_PRIVILEGES.name();
        String checkRowPolicy = RuleType.CHECK_ROW_POLICY.name();
        return Arrays.stream(disableNereidsRules.split(","))
                .map(rule -> rule.trim().toUpperCase(Locale.ROOT))
                .filter(rule -> !rule.isEmpty()
                        && !StringUtils.equalsIgnoreCase(rule, checkPrivilege)
                        && !StringUtils.equalsIgnoreCase(rule, checkRowPolicy))
                .collect(ImmutableSet.toImmutableSet());
    }

    public BitSet getDisableNereidsRules() {
        BitSet bitSet = new BitSet();
        for (String ruleName : disableNereidsRules.split(",")) {
            ruleName = ruleName.trim().toUpperCase(Locale.ROOT);
            if (ruleName.isEmpty()) {
                continue;
            }
            RuleType ruleType = RuleType.valueOf(ruleName);
            if (ruleType == RuleType.CHECK_PRIVILEGES || ruleType == RuleType.CHECK_ROW_POLICY) {
                continue;
            }
            bitSet.set(ruleType.type());
        }
        return bitSet;
    }

    public Set<Integer> getEnableNereidsRules() {
        return Arrays.stream(enableNereidsRules.split(",[\\s]*"))
                .filter(rule -> !rule.isEmpty())
                .map(rule -> rule.toUpperCase(Locale.ROOT))
                .map(rule -> RuleType.valueOf(rule).type())
                .collect(ImmutableSet.toImmutableSet());
    }

    public BitSet getDisableNereidsExpressionRules() {
        return disableNereidsExpressionRuleSet;
    }

    public void setDisableNereidsRules(String disableNereidsRules) {
        this.disableNereidsRules = disableNereidsRules;
    }

    public void setEnableParallelResultSink(boolean enableParallelResultSink) {
        this.enableParallelResultSink = enableParallelResultSink;
    }

    public void setDisableNereidsExpressionRules(String disableNereidsExpressionRules) {
        BitSet bitSet = new BitSet();
        for (String ruleName : disableNereidsExpressionRules.split(",")) {
            ruleName = ruleName.trim().toUpperCase(Locale.ROOT);
            if (ruleName.isEmpty()) {
                continue;
            }
            ExpressionRuleType ruleType = ExpressionRuleType.valueOf(ruleName);
            bitSet.set(ruleType.type());
        }
        this.disableNereidsExpressionRuleSet = bitSet;
        this.disableNereidsExpressionRules = disableNereidsExpressionRules;
    }

    public double getNereidsCboPenaltyFactor() {
        return nereidsCboPenaltyFactor;
    }

    public void setNereidsCboPenaltyFactor(double penaltyFactor) {
        this.nereidsCboPenaltyFactor = penaltyFactor;
    }

    public boolean isEnableNereidsTrace() {
        return enableNereidsTrace;
    }

    public void setEnableExprTrace(boolean enableExprTrace) {
        this.enableExprTrace = enableExprTrace;
    }

    public boolean isEnableExprTrace() {
        return enableExprTrace;
    }

    public boolean isEnableSingleReplicaInsert() {
        return enableSingleReplicaInsert;
    }

    public void setEnableSingleReplicaInsert(boolean enableSingleReplicaInsert) {
        this.enableSingleReplicaInsert = enableSingleReplicaInsert;
    }

    public boolean isEnableFastAnalyzeInsertIntoValues() {
        return enableFastAnalyzeInsertIntoValues;
    }

    public void setEnableFastAnalyzeInsertIntoValues(boolean enableFastAnalyzeInsertIntoValues) {
        this.enableFastAnalyzeInsertIntoValues = enableFastAnalyzeInsertIntoValues;
    }

    public boolean isEnableMemtableOnSinkNode() {
        return enableMemtableOnSinkNode;
    }

    public void setEnableMemtableOnSinkNode(boolean enableMemtableOnSinkNode) {
        this.enableMemtableOnSinkNode = enableMemtableOnSinkNode;
    }

    public boolean isEnableRuntimeFilterPrune() {
        return enableRuntimeFilterPrune;
    }

    public void setEnableRuntimeFilterPrune(boolean enableRuntimeFilterPrune) {
        this.enableRuntimeFilterPrune = enableRuntimeFilterPrune;
    }

    public boolean isEnableRuntimeFilterPartitionPrune() {
        return enableRuntimeFilterPartitionPrune;
    }

    public void setEnableRuntimeFilterPartitionPrune(boolean enableRuntimeFilterPartitionPrune) {
        this.enableRuntimeFilterPartitionPrune = enableRuntimeFilterPartitionPrune;
    }

    public void setFragmentTransmissionCompressionCodec(String codec) {
        this.fragmentTransmissionCompressionCodec = codec;
    }

    public boolean isEnableUnicodeNameSupport() {
        return enableUnicodeNameSupport;
    }

    public void setEnableUnicodeNameSupport(boolean enableUnicodeNameSupport) {
        this.enableUnicodeNameSupport = enableUnicodeNameSupport;
    }

    public boolean isKeepCarriageReturn() {
        return keepCarriageReturn;
    }

    public void setKeepCarriageReturn(boolean keepCarriageReturn) {
        this.keepCarriageReturn = keepCarriageReturn;
    }

    public boolean isReadHiveJsonInOneColumn() {
        return readHiveJsonInOneColumn;
    }

    public void setReadHiveJsonInOneColumn(boolean readHiveJsonInOneColumn) {
        this.readHiveJsonInOneColumn = readHiveJsonInOneColumn;
    }

    public boolean isDropTableIfCtasFailed() {
        return dropTableIfCtasFailed;
    }

    public void checkQueryTimeoutValid(String newQueryTimeout) {
        int value = Integer.valueOf(newQueryTimeout);
        if (value <= 0) {
            UnsupportedOperationException exception =
                    new UnsupportedOperationException(
                        "query_timeout can not be set to " + newQueryTimeout + ", it must be greater than 0");
            LOG.warn("Check query_timeout failed", exception);
            throw exception;
        }
    }

    public void checkMaxExecutionTimeMSValid(String newValue) {
        int value = Integer.valueOf(newValue);
        if (value < 1000) {
            UnsupportedOperationException exception =
                    new UnsupportedOperationException(
                        "max_execution_time can not be set to " + newValue
                        + ", it must be greater or equal to 1000, the time unit is millisecond");
            LOG.warn("Check max_execution_time failed", exception);
            throw exception;
        }
    }

    public long convertBoolToLong(Boolean val) {
        return val ? 1 : 0;
    }

    public boolean isEnableFileCache() {
        return enableFileCache;
    }

    public void setEnableFileCache(boolean enableFileCache) {
        this.enableFileCache = enableFileCache;
    }

    public String getFileCacheBasePath() {
        return fileCacheBasePath;
    }

    public void setFileCacheBasePath(String basePath) {
        this.fileCacheBasePath = basePath;
    }

    public boolean isEnableInvertedIndexQuery() {
        return enableInvertedIndexQuery;
    }

    public void setEnableInvertedIndexQuery(boolean enableInvertedIndexQuery) {
        this.enableInvertedIndexQuery = enableInvertedIndexQuery;
    }


    public boolean isEnableCommonExprPushdownForInvertedIndex() {
        return enableCommonExpPushDownForInvertedIndex;
    }


    public void setEnableCommonExprPushdownForInvertedIndex(boolean enableCommonExpPushDownForInvertedIndex) {
        this.enableCommonExpPushDownForInvertedIndex = enableCommonExpPushDownForInvertedIndex;
    }

    public boolean isEnablePushDownCountOnIndex() {
        return enablePushDownCountOnIndex;
    }

    public void setEnablePushDownCountOnIndex(boolean enablePushDownCountOnIndex) {
        this.enablePushDownCountOnIndex = enablePushDownCountOnIndex;
    }

    public boolean isEnableNoNeedReadDataOpt() {
        return enableNoNeedReadDataOpt;
    }

    public void setEnableNoNeedReadDataOpt(boolean enableNoNeedReadDataOpt) {
        this.enableNoNeedReadDataOpt = enableNoNeedReadDataOpt;
    }

    public int getMaxTableCountUseCascadesJoinReorder() {
        return this.maxTableCountUseCascadesJoinReorder;
    }

    public void setMaxTableCountUseCascadesJoinReorder(int maxTableCountUseCascadesJoinReorder) {
        this.maxTableCountUseCascadesJoinReorder =
                maxTableCountUseCascadesJoinReorder < MIN_JOIN_REORDER_TABLE_COUNT
                        ? MIN_JOIN_REORDER_TABLE_COUNT
                        : maxTableCountUseCascadesJoinReorder;
    }

    public boolean isShowUserDefaultRole() {
        return showUserDefaultRole;
    }

    public int getExternalTableAnalyzePartNum() {
        return externalTableAnalyzePartNum;
    }

    public boolean isTruncateCharOrVarcharColumns() {
        return truncateCharOrVarcharColumns;
    }

    public void setTruncateCharOrVarcharColumns(boolean truncateCharOrVarcharColumns) {
        this.truncateCharOrVarcharColumns = truncateCharOrVarcharColumns;
    }

    public boolean isEnableUniqueKeyPartialUpdate() {
        return enableUniqueKeyPartialUpdate;
    }

    public void setEnableUniqueKeyPartialUpdate(boolean enableUniqueKeyPartialUpdate) {
        this.enableUniqueKeyPartialUpdate = enableUniqueKeyPartialUpdate;
    }

    public TPartialUpdateNewRowPolicy getPartialUpdateNewRowPolicy() {
        return parsePartialUpdateNewKeyBehavior(partialUpdateNewKeyPolicy);
    }

    public int getLoadStreamPerNode() {
        return loadStreamPerNode;
    }

    public void setLoadStreamPerNode(int loadStreamPerNode) {
        this.loadStreamPerNode = loadStreamPerNode;
    }

    public void setEnableSegmentCache(boolean value) {
        this.enableSegmentCache = value;
    }

    public boolean isEnableSegmentCache() {
        return this.enableSegmentCache;
    }

    public void setRequireSequenceInInsert(boolean value) {
        this.requireSequenceInInsert = value;
    }

    public boolean isRequireSequenceInInsert() {
        return this.requireSequenceInInsert;
    }

    /**
     * Serialize to thrift object.
     * Used for rest api.
     */
    public TQueryOptions toThrift() {
        TQueryOptions tResult = new TQueryOptions();
        tResult.setMemLimit(maxExecMemByte);
        tResult.setLocalExchangeFreeBlocksLimit(localExchangeFreeBlocksLimit);
        tResult.setScanQueueMemLimit(maxScanQueueMemByte);
        tResult.setNumScannerThreads(numScannerThreads);
        tResult.setMaxColumnReaderNum(maxColumnReaderNum);
        tResult.setParallelPrepareThreshold(parallelPrepareThreshold);

        tResult.setQueryTimeout(queryTimeoutS);
        tResult.setEnableProfile(enableProfile);
        tResult.setRpcVerboseProfileMaxInstanceCount(rpcVerboseProfileMaxInstanceCount);
        if (enableProfile) {
            // If enable profile == true, then also set report success to true
            // be need report success to start report thread. But it is very tricky
            // we should modify BE in the future.
            tResult.setIsReportSuccess(true);
        }
        tResult.setBeExecVersion(Config.be_exec_version);
        tResult.setEnableLocalShuffle(enableLocalShuffle);
        tResult.setParallelInstance(getParallelExecInstanceNum());
        tResult.setReturnObjectDataAsBinary(returnObjectDataAsBinary);
        tResult.setTrimTailingSpacesForExternalTableQuery(trimTailingSpacesForExternalTableQuery);
        tResult.setEnableShareHashTableForBroadcastJoin(enableShareHashTableForBroadcastJoin);

        tResult.setBatchSize(batchSize);
        tResult.setDisableStreamPreaggregations(disableStreamPreaggregations);
        tResult.setEnableDistinctStreamingAggregation(enableDistinctStreamingAggregation);
        tResult.setPartitionTopnMaxPartitions(partitionTopNMaxPartitions);
        tResult.setPartitionTopnPrePartitionRows(partitionTopNPerPartitionRows);

        if (maxScanKeyNum > 0) {
            tResult.setMaxScanKeyNum(maxScanKeyNum);
        } else {
            // for old version default value
            tResult.setMaxScanKeyNum(48);
        }
        if (maxPushdownConditionsPerColumn > 0) {
            tResult.setMaxPushdownConditionsPerColumn(maxPushdownConditionsPerColumn);
        } else {
            // for old version default value
            tResult.setMaxPushdownConditionsPerColumn(1024);
        }

        tResult.setRuntimeFilterWaitTimeMs(runtimeFilterWaitTimeMs);
        tResult.setRuntimeFilterMaxInNum(runtimeFilterMaxInNum);
        tResult.setRuntimeBloomFilterMinSize(runtimeBloomFilterMinSize);
        tResult.setRuntimeBloomFilterMaxSize(runtimeBloomFilterMaxSize);
        tResult.setRuntimeFilterWaitInfinitely(runtimeFilterWaitInfinitely);

        if (cpuResourceLimit > 0) {
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuResourceLimit);
            tResult.setResourceLimit(resourceLimit);
        }

        tResult.setEnableFunctionPushdown(enableFunctionPushdown);
        tResult.setEnableCommonExprPushdown(enableCommonExprPushdown);
        tResult.setCheckOverflowForDecimal(checkOverflowForDecimal);
        tResult.setFragmentTransmissionCompressionCodec(fragmentTransmissionCompressionCodec.trim().toLowerCase());
        tResult.setEnableLocalExchange(enableLocalExchange);

        tResult.setSkipStorageEngineMerge(skipStorageEngineMerge);

        tResult.setSkipDeletePredicate(skipDeletePredicate);

        tResult.setSkipDeleteBitmap(skipDeleteBitmap);

        tResult.setEnableFileCache(enableFileCache);

        tResult.setEnablePageCache(enablePageCache);

        tResult.setFileCacheBasePath(fileCacheBasePath);

        tResult.setEnableInvertedIndexQuery(enableInvertedIndexQuery);
        tResult.setEnableCommonExprPushdownForInvertedIndex(enableCommonExpPushDownForInvertedIndex);
        tResult.setEnableNoNeedReadDataOpt(enableNoNeedReadDataOpt);

        if (dryRunQuery) {
            tResult.setDryRunQuery(true);
        }

        tResult.setEnableParquetLazyMat(enableParquetLazyMat);
        tResult.setEnableOrcLazyMat(enableOrcLazyMat);
        tResult.setEnableParquetFilterByMinMax(enableParquetFilterByMinMax);
        tResult.setEnableOrcFilterByMinMax(enableOrcFilterByMinMax);
        tResult.setCheckOrcInitSargsSuccess(checkOrcInitSargsSuccess);

        tResult.setTruncateCharOrVarcharColumns(truncateCharOrVarcharColumns);
        tResult.setEnableMemtableOnSinkNode(enableMemtableOnSinkNode);

        tResult.setInvertedIndexConjunctionOptThreshold(invertedIndexConjunctionOptThreshold);
        tResult.setInvertedIndexMaxExpansions(invertedIndexMaxExpansions);

        tResult.setEnableDecimal256(getEnableDecimal256());

        tResult.setSkipMissingVersion(skipMissingVersion);

        tResult.setInvertedIndexSkipThreshold(invertedIndexSkipThreshold);

        tResult.setInvertedIndexCompatibleRead(invertedIndexCompatibleRead);

        tResult.setEnableParallelScan(enableParallelScan);
        tResult.setParallelScanMaxScannersCount(parallelScanMaxScannersCount);
        tResult.setParallelScanMinRowsPerScanner(parallelScanMinRowsPerScanner);
        tResult.setSkipBadTablet(skipBadTablet);
        tResult.setDisableFileCache(disableFileCache);

        // for spill
        tResult.setEnableSpill(enableSpill);
        tResult.setEnableForceSpill(enableForceSpill);
        tResult.setEnableReserveMemory(enableReserveMemory);
        tResult.setMinRevocableMem(spillMinRevocableMem);
        tResult.setSpillSortMemLimit(spillSortMemLimit);
        tResult.setSpillSortBatchBytes(spillSortBatchBytes);
        tResult.setSpillAggregationPartitionCount(spillAggregationPartitionCount);
        tResult.setSpillStreamingAggMemLimit(spillStreamingAggMemLimit);
        tResult.setSpillHashJoinPartitionCount(spillHashJoinPartitionCount);
        tResult.setRevocableMemoryHighWatermarkPercent(spillRevocableMemoryHighWatermarkPercent);
        tResult.setDumpHeapProfileWhenMemLimitExceeded(dumpHeapProfileWhenMemLimitExceeded);

        tResult.setDataQueueMaxBlocks(dataQueueMaxBlocks);
        tResult.setLowMemoryModeBufferLimit(lowMemoryModeBufferLimit);

        tResult.setEnableSharedExchangeSinkBuffer(enableSharedExchangeSinkBuffer);
        tResult.setEnableParallelResultSink(enableParallelResultSink);
        tResult.setEnableParallelOutfile(enableParallelOutfile);
        tResult.setEnableShortCircuitQueryAccessColumnStore(enableShortCircuitQueryAcessColumnStore);
        tResult.setReadCsvEmptyLineAsNull(readCsvEmptyLineAsNull);
        tResult.setSerdeDialect(getSerdeDialect());

        tResult.setEnableMatchWithoutInvertedIndex(enableMatchWithoutInvertedIndex);
        tResult.setEnableFallbackOnMissingInvertedIndex(enableFallbackOnMissingInvertedIndex);
        tResult.setEnableInvertedIndexSearcherCache(enableInvertedIndexSearcherCache);
        tResult.setEnableInvertedIndexQueryCache(enableInvertedIndexQueryCache);
        tResult.setHiveOrcUseColumnNames(hiveOrcUseColumnNames);
        tResult.setHiveParquetUseColumnNames(hiveParquetUseColumnNames);
        tResult.setQuerySlotCount(wgQuerySlotCount);

        tResult.setKeepCarriageReturn(keepCarriageReturn);

        tResult.setEnableSegmentCache(enableSegmentCache);

        tResult.setEnableAdaptivePipelineTaskSerialReadOnLimit(enableAdaptivePipelineTaskSerialReadOnLimit);
        tResult.setAdaptivePipelineTaskSerialReadOnLimit(adaptivePipelineTaskSerialReadOnLimit);
        tResult.setInListValueCountThreshold(inListValueCountThreshold);
        tResult.setEnablePhraseQuerySequentialOpt(enablePhraseQuerySequentialOpt);
        tResult.setEnableAutoCreateWhenOverwrite(enableAutoCreateWhenOverwrite);

        tResult.setOrcTinyStripeThresholdBytes(orcTinyStripeThresholdBytes);
        tResult.setOrcMaxMergeDistanceBytes(orcMaxMergeDistanceBytes);
        tResult.setOrcOnceMaxReadBytes(orcOnceMaxReadBytes);
        tResult.setIgnoreRuntimeFilterError(ignoreRuntimeFilterError);
        tResult.setEnableFixedLenToUint32V2(enableFixedLenToUint32V2);
        tResult.setProfileLevel(getProfileLevel());
        tResult.setEnableRuntimeFilterPartitionPrune(enableRuntimeFilterPartitionPrune);

        tResult.setMinimumOperatorMemoryRequiredKb(minimumOperatorMemoryRequiredKB);
        tResult.setExchangeMultiBlocksByteSize(exchangeMultiBlocksByteSize);
        tResult.setEnableStrictCast(enableStrictCast);
        return tResult;
    }

    public JSONObject toJson() throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        root.put(attr.name(), (Boolean) field.get(this));
                        break;
                    case "int":
                        root.put(attr.name(), (Integer) field.get(this));
                        break;
                    case "long":
                        root.put(attr.name(), (Long) field.get(this));
                        break;
                    case "float":
                        root.put(attr.name(), (Float) field.get(this));
                        break;
                    case "double":
                        root.put(attr.name(), (Double) field.get(this));
                        break;
                    case "String":
                        root.put(attr.name(), (String) field.get(this));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
        return root;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JSONObject root = toJson();
        Text.writeString(out, root.toString());
    }


    public void readFields(DataInput in) throws IOException {
        String json = Text.readString(in);
        readFromJson(json);
    }

    public void readFromJson(String json) throws IOException {
        JSONObject root = (JSONObject) JSONValue.parse(json);
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }
                if (!root.containsKey(attr.name())) {
                    continue;
                }

                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "int":
                        // root.get(attr.name()) always return Long type, so need to convert it.
                        field.set(this, Integer.valueOf(root.get(attr.name()).toString()));
                        break;
                    case "long":
                        field.set(this, (Long) root.get(attr.name()));
                        break;
                    case "float":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "double":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "String":
                        field.set(this, root.get(attr.name()));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to read session variable: " + e.getMessage());
        }
    }

    /**
     * Get all variables which need to forward along with statement.
     **/
    public Map<String, String> getForwardVariables() {
        HashMap<String, String> map = new HashMap<String, String>();
        try {
            Field[] fields = SessionVariable.class.getDeclaredFields();
            for (Field f : fields) {
                VarAttr varAttr = f.getAnnotation(VarAttr.class);
                if (varAttr == null || !(varAttr.needForward() || varAttr.affectQueryResult())) {
                    continue;
                }
                map.put(varAttr.name(), String.valueOf(f.get(this)));
            }
        } catch (IllegalAccessException e) {
            LOG.error("failed to get forward variables", e);
        }
        return map;
    }

    /**
     * Set forwardedSessionVariables for variables.
     **/
    public void setForwardedSessionVariables(Map<String, String> variables) {
        try {
            Field[] fields = SessionVariable.class.getDeclaredFields();
            for (Field f : fields) {
                f.setAccessible(true);
                VarAttr varAttr = f.getAnnotation(VarAttr.class);
                if (varAttr == null || !(varAttr.needForward() || varAttr.affectQueryResult())) {
                    continue;
                }
                String val = variables.get(varAttr.name());
                if (val == null) {
                    continue;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("set forward variable: {} = {}", varAttr.name(), val);
                }

                // set config field
                switch (f.getType().getSimpleName()) {
                    case "short":
                        f.setShort(this, Short.parseShort(val));
                        break;
                    case "int":
                        f.setInt(this, Integer.parseInt(val));
                        break;
                    case "long":
                        f.setLong(this, Long.parseLong(val));
                        break;
                    case "double":
                        f.setDouble(this, Double.parseDouble(val));
                        break;
                    case "boolean":
                        f.setBoolean(this, Boolean.parseBoolean(val));
                        break;
                    case "String":
                        f.set(this, val);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown field type: " + f.getType().getSimpleName());
                }
            }
        } catch (IllegalAccessException e) {
            LOG.error("failed to set forward variables", e);
        }
    }

    /**
     * Set forwardedSessionVariables for queryOptions.
     **/
    public void setForwardedSessionVariables(TQueryOptions queryOptions) {
        if (queryOptions.isSetMemLimit()) {
            setMaxExecMemByte(queryOptions.getMemLimit());
        }
        if (queryOptions.isSetQueryTimeout()) {
            setQueryTimeoutS(queryOptions.getQueryTimeout());
        }
        if (queryOptions.isSetInsertTimeout()) {
            setInsertTimeoutS(queryOptions.getInsertTimeout());
        }
        if (queryOptions.isSetAnalyzeTimeout()) {
            setAnalyzeTimeoutS(queryOptions.getAnalyzeTimeout());
        }
    }

    /**
     * The sessionContext is as follows:
     * "k1:v1;k2:v2;..."
     * eg: set session_context="trace_id:123456"
     * Here we want to get value with key named "trace_id",
     * Return empty string is not found.
     *
     * @return
     */
    public String getTraceId() {
        if (Strings.isNullOrEmpty(sessionContext)) {
            return "";
        }
        String[] parts = sessionContext.split(";");
        for (String part : parts) {
            String[] innerParts = part.split(":");
            if (innerParts.length != 2) {
                continue;
            }
            if (innerParts[0].equals("trace_id")) {
                return innerParts[1];
            }
        }
        return "";
    }

    public boolean isTraceNereids() {
        return traceNereids;
    }

    public void setTraceNereids(boolean traceNereids) {
        this.traceNereids = traceNereids;
    }

    public boolean isPlayNereidsDump() {
        return planNereidsDump;
    }

    public void setPlanNereidsDump(boolean planNereidsDump) {
        this.planNereidsDump = planNereidsDump;
    }

    public boolean isDumpNereidsMemo() {
        return dumpNereidsMemo;
    }

    public void setDumpNereidsMemo(boolean dumpNereidsMemo) {
        this.dumpNereidsMemo = dumpNereidsMemo;
    }

    public boolean isEnableStrictConsistencyDml() {
        return this.enableStrictConsistencyDml;
    }

    public void setEnableStrictConsistencyDml(boolean value) {
        this.enableStrictConsistencyDml = value;
    }

    /**
     *
     * @return true iff set success
     */
    public boolean setVarOnce(String varName, String value) {
        try {
            setIsSingleSetVar(true);
            VariableMgr.setVar(this, new SetVar(varName, new StringLiteral(value)));
            return true;
        } catch (DdlException e) {
            LOG.warn("set onece {} = {} failed", varName, value);
            return false;
        }
    }

    // return number of variables by given variable annotation
    public int getVariableNumByVariableAnnotation(VariableAnnotation type) {
        int num = 0;
        Field[] fields = SessionVariable.class.getDeclaredFields();
        for (Field f : fields) {
            VarAttr varAttr = f.getAnnotation(VarAttr.class);
            if (varAttr == null) {
                continue;
            }
            if (varAttr.varType() == type) {
                ++num;
            }
        }
        return num;
    }

    public boolean getEnableSharedScan() {
        return enableSharedScan;
    }

    public void setEnableSharedScan(boolean value) {
        enableSharedScan = value;
    }

    public boolean getEnableParallelScan() {
        return enableParallelScan;
    }

    public boolean enableParallelResultSink() {
        return enableParallelResultSink;
    }

    public void setParallelResultSink(Boolean enableParallelResultSink) {
        this.enableParallelResultSink   = enableParallelResultSink;
    }

    public String hiveTextCompression() {
        if (hiveTextCompression.equals("uncompressed")) {
            // This is for compatibility.
            return "plain";
        }
        return hiveTextCompression;
    }

    public void setHiveTextCompression(String hiveTextCompression) {
        this.hiveTextCompression = hiveTextCompression;
    }

    public boolean enableSyncRuntimeFilterSize() {
        return enableSyncRuntimeFilterSize;
    }

    public static boolean enableAggState() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return true;
        }
        return connectContext.getSessionVariable().enableAggState;
    }

    public static boolean getEnableDecimal256() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return false;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        return connectContext.getState().isNereids() && sessionVariable.isEnableDecimal256();
    }

    public boolean isEnableDecimal256() {
        return enableDecimal256;
    }

    public void checkAnalyzeTimeFormat(String time) {
        try {
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            timeFormatter.parse(time);
        } catch (DateTimeParseException e) {
            LOG.warn("Parse analyze start/end time format fail", e);
            throw new UnsupportedOperationException("Expect format: HH:mm:ss");
        }
    }

    public void checkGenerateStatsFactor(String generateStatsFactor) {
        int value = Integer.valueOf(generateStatsFactor);
        if (value <= 0) {
            UnsupportedOperationException exception =
                    new UnsupportedOperationException("Generate stats factor " + value + " should greater than 0");
            LOG.warn("Check generate stats factor failed", exception);
            throw exception;
        }
    }

    public TPartialUpdateNewRowPolicy parsePartialUpdateNewKeyBehavior(String behavior) {
        if (behavior == null) {
            return null;
        } else if (behavior.equalsIgnoreCase("APPEND")) {
            return TPartialUpdateNewRowPolicy.APPEND;
        } else if (behavior.equalsIgnoreCase("ERROR")) {
            return TPartialUpdateNewRowPolicy.ERROR;
        }
        return null;
    }

    public void checkPartialUpdateNewKeyBehavior(String partialUpdateNewKeyBehavior) {
        TPartialUpdateNewRowPolicy policy = parsePartialUpdateNewKeyBehavior(partialUpdateNewKeyBehavior);
        if (policy == null) {
            UnsupportedOperationException exception =
                    new UnsupportedOperationException(PARTIAL_UPDATE_NEW_KEY_BEHAVIOR
                            + " should be one of {'APPEND', 'ERROR'}, but found "
                                    + partialUpdateNewKeyBehavior);
            LOG.warn("Check " + PARTIAL_UPDATE_NEW_KEY_BEHAVIOR + " failed", exception);
            throw exception;
        }
    }

    public void setGenerateStatsFactor(int factor) {
        this.generateStatsFactor = factor;
        if (factor <= 0) {
            LOG.warn("Invalid generate stats factor: {}", factor, new RuntimeException(""));
        }
    }

    public void setGenerateStatsFactor(String factor) {
        this.generateStatsFactor = Integer.valueOf(factor);
        if (generateStatsFactor <= 0) {
            LOG.warn("Invalid generate stats factor: {}", generateStatsFactor, new RuntimeException(""));
        }
    }

    public boolean getEnableDescribeExtendVariantColumn() {
        return enableDescribeExtendVariantColumn;
    }

    public void setEnableDescribeExtendVariantColumn(boolean enableDescribeExtendVariantColumn) {
        this.enableDescribeExtendVariantColumn = enableDescribeExtendVariantColumn;
    }

    public static boolean enableDescribeExtendVariantColumn() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            return false;
        }
        return connectContext.getSessionVariable().enableDescribeExtendVariantColumn;
    }

    public int getProfileLevel() {
        return this.profileLevel;
    }

    public void checkSqlDialect(String sqlDialect) {
        if (StringUtils.isEmpty(sqlDialect)) {
            LOG.warn("sqlDialect value is empty");
            throw new UnsupportedOperationException("sqlDialect value is empty");
        }
        if (Arrays.stream(Dialect.values())
                .noneMatch(dialect -> dialect.getDialectName().equalsIgnoreCase(sqlDialect))) {
            LOG.warn("sqlDialect value is invalid, the invalid value is {}", sqlDialect);
            throw new UnsupportedOperationException("sqlDialect value is invalid, the invalid value is " + sqlDialect);
        }
    }

    public void checkSerdeDialect(String serdeDialect) {
        if (StringUtils.isEmpty(serdeDialect)) {
            LOG.warn("serdeDialect value is empty");
            throw new UnsupportedOperationException("serdeDialect value is empty");
        }

        if (!serdeDialect.equalsIgnoreCase("doris")
                && !serdeDialect.equalsIgnoreCase("presto")
                && !serdeDialect.equalsIgnoreCase("trino")
                && !serdeDialect.equalsIgnoreCase("hive")) {
            LOG.warn("serde dialect value is invalid, the invalid value is {}", serdeDialect);
            throw new UnsupportedOperationException(
                    "sqlDialect value is invalid, the invalid value is " + serdeDialect);
        }
    }

    public void checkBatchSize(String batchSize) {
        Long batchSizeValue = Long.valueOf(batchSize);
        if (batchSizeValue < 1 || batchSizeValue > 65535) {
            throw new InvalidParameterException("batch_size should be between 1 and 65535)");
        }
    }

    public void checkSkewRewriteAggBucketNum(String bucketNumStr) {
        try {
            long bucketNum = Long.parseLong(bucketNumStr);
            if (bucketNum <= 0 || bucketNum >= 65536) {
                throw new InvalidParameterException(
                        SKEW_REWRITE_AGG_BUCKET_NUM + " should be between 1 and 65535");
            }
        } catch (NumberFormatException e) {
            throw new InvalidParameterException(
                    SKEW_REWRITE_AGG_BUCKET_NUM + " must be a valid number between 1 and 65535");
        }
    }

    public void checkSkewRewriteJoinSaltExplodeFactor(String factorStr) {
        try {
            int factor = Integer.parseInt(factorStr);
            if (factor < 0 || factor >= 65536) {
                throw new InvalidParameterException(
                        SKEW_REWRITE_JOIN_SALT_EXPLODE_FACTOR + " should be between 0 and 65535");
            }
        } catch (NumberFormatException e) {
            throw new InvalidParameterException(
                    SKEW_REWRITE_JOIN_SALT_EXPLODE_FACTOR + " must be a valid number between 0 and 65535");
        }
    }

    public boolean isEnableInsertGroupCommit() {
        return Config.wait_internal_group_commit_finish
                || GroupCommitBlockSink.parseGroupCommit(groupCommit) == TGroupCommitMode.ASYNC_MODE
                || GroupCommitBlockSink.parseGroupCommit(groupCommit) == TGroupCommitMode.SYNC_MODE;
    }

    public String getGroupCommit() {
        if (Config.wait_internal_group_commit_finish) {
            return "sync_mode";
        }
        return groupCommit;
    }

    public boolean isEnablePreparedStmtAuditLog() {
        return enablePreparedStmtAuditLog;
    }

    public boolean isEnableMaterializedViewRewrite() {
        return enableMaterializedViewRewrite;
    }

    public void setEnableMaterializedViewRewrite(boolean enableMaterializedViewRewrite) {
        this.enableMaterializedViewRewrite = enableMaterializedViewRewrite;
    }

    public boolean isEnableDmlMaterializedViewRewrite() {
        return enableDmlMaterializedViewRewrite;
    }

    public boolean isEnableDmlMaterializedViewRewriteWhenBaseTableUnawareness() {
        return enableDmlMaterializedViewRewriteWhenBaseTableUnawareness;
    }


    public boolean isAllowModifyMaterializedViewData() {
        return allowModifyMaterializedViewData;
    }

    public boolean isEnableMaterializedViewRewriteWhenBaseTableUnawareness() {
        return enableMaterializedViewRewriteWhenBaseTableUnawareness;
    }

    public int getMaterializedViewRewriteSuccessCandidateNum() {
        return materializedViewRewriteSuccessCandidateNum;
    }

    public boolean isEnableMaterializedViewUnionRewrite() {
        return enableMaterializedViewUnionRewrite;
    }

    public boolean isEnableMaterializedViewNestRewrite() {
        return enableMaterializedViewNestRewrite;
    }

    public int getMaterializedViewRelationMappingMaxCount() {
        return materializedViewRelationMappingMaxCount;
    }

    public int getCreateTablePartitionMaxNum() {
        return createTablePartitionMaxNum;
    }

    public boolean isIgnoreStorageDataDistribution() {
        return ignoreStorageDataDistribution && enableLocalShuffle && enableNereidsPlanner;
    }

    public void setIgnoreStorageDataDistribution(boolean ignoreStorageDataDistribution) {
        this.ignoreStorageDataDistribution = ignoreStorageDataDistribution;
    }

    // CLOUD_VARIABLES_BEGIN
    public String getCloudCluster() {
        return cloudCluster;
    }

    public String setCloudCluster(String cloudCluster) {
        return this.cloudCluster = cloudCluster;
    }

    public boolean getDisableEmptyPartitionPrune() {
        return disableEmptyPartitionPrune;
    }

    public void setDisableEmptyPartitionPrune(boolean val) {
        disableEmptyPartitionPrune = val;
    }
    // CLOUD_VARIABLES_END

    public boolean isForceJniScanner() {
        return forceJniScanner;
    }

    public String getIgnoreSplitType() {
        return ignoreSplitType;
    }

    public void checkIgnoreSplitType(String value) {
        try {
            IgnoreSplitType.valueOf(value);
        } catch (Exception e) {
            throw new UnsupportedOperationException("We only support `NONE`, `IGNORE_JNI` and `IGNORE_NATIVE`");
        }
    }

    public boolean getUseConsistentHashForExternalScan() {
        return useConsistentHashForExternalScan;
    }

    public void setForceJniScanner(boolean force) {
        forceJniScanner = force;
    }

    public boolean isEnableCountPushDownForExternalTable() {
        return enableCountPushDownForExternalTable;
    }

    public boolean isForceToLocalShuffle() {
        return enableLocalShuffle && forceToLocalShuffle && enableNereidsPlanner;
    }

    public void setForceToLocalShuffle(boolean forceToLocalShuffle) {
        this.forceToLocalShuffle = forceToLocalShuffle;
    }

    public boolean getShowAllFeConnection() {
        return this.showAllFeConnection;
    }

    public int getMaxMsgSizeOfResultReceiver() {
        return this.maxMsgSizeOfResultReceiver;
    }

    public boolean isEnableAutoCreateWhenOverwrite() {
        return this.enableAutoCreateWhenOverwrite;
    }

    public TSerdeDialect getSerdeDialect() {
        switch (serdeDialect) {
            case "doris":
                return TSerdeDialect.DORIS;
            case "presto":
            case "trino":
                return TSerdeDialect.PRESTO;
            case "hive":
                return TSerdeDialect.HIVE;
            default:
                throw new IllegalArgumentException("Unknown serde dialect: " + serdeDialect);
        }
    }

    public boolean isEnableCooldownReplicaAffinity() {
        return enableCooldownReplicaAffinity;
    }

    public boolean isUseSerialExchange() {
        return useSerialExchange && getEnableLocalExchange();
    }

    public void setDisableInvertedIndexV1ForVaraint(boolean disableInvertedIndexV1ForVaraint) {
        this.disableInvertedIndexV1ForVaraint = disableInvertedIndexV1ForVaraint;
    }

    public boolean getDisableInvertedIndexV1ForVaraint() {
        return disableInvertedIndexV1ForVaraint;
    }

    public void setDisableVariantFlattenNested(boolean disableVariantFlattenNested) {
        this.disableVariantFlattenNested = disableVariantFlattenNested;
    }

    public boolean getDisableVariantFlattenNested() {
        return disableVariantFlattenNested;
    }

    public void setProfileLevel(String profileLevel) {
        int profileLevelTmp = Integer.valueOf(profileLevel);
        if (profileLevelTmp < 1 || profileLevelTmp > 3) {
            LOG.warn("Profile level shuold be in the range of 1-3.");
        } else {
            this.profileLevel = profileLevelTmp;
        }

    }

    public void checkProfileLevel(String profileLevel) {
        int value = Integer.valueOf(profileLevel);
        if (value < 1 || value > 3) {
            UnsupportedOperationException exception =
                    new UnsupportedOperationException("Profile level can not be set to " + profileLevel
                            + ", it must be in the range of 1-3");
            LOG.warn("Check profile_level failed", exception);
            throw exception;
        }
    }

    public void checkSqlConvertorFeatures(String features) {
        if (Strings.isNullOrEmpty(features)) {
            return;
        }
        String[] featureArray = features.split(",");
        for (String feature : featureArray) {
            if (!feature.equalsIgnoreCase("ctas")
                    && !feature.equalsIgnoreCase("delete_all_comment")) {
                throw new UnsupportedOperationException("Unknown sql convertor feature: " + feature
                        + ", current support: ctas, delete_all_comment");
            }
        }
    }

    public boolean getEnableLocalMergeSort() {
        return enableLocalMergeSort;
    }

    public boolean getEnableExternalTableBatchMode() {
        return enableExternalTableBatchMode;
    }

    public boolean showSplitProfileInfo() {
        return enableProfile() && getProfileLevel() > 1;
    }

    public boolean isEnableAddIndexForNewData() {
        return enableAddIndexForNewData;
    }

    public void setEnableAddIndexForNewData(boolean enableAddIndexForNewData) {
        this.enableAddIndexForNewData = enableAddIndexForNewData;
    }

    public static boolean enableStrictCast() {
        if (ConnectContext.get() != null) {
            return ConnectContext.get().getSessionVariable().enableStrictCast;
        } else {
            return Boolean.parseBoolean(VariableMgr.getDefaultValue("ENABLE_STRICT_CAST"));
        }
    }
}
