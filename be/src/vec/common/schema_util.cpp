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

#include "vec/common/schema_util.h"

#include <assert.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "udf/udf.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/json/json_parser.h"
#include "vec/json/parse2column.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized::schema_util {
#include "common/compile_check_begin.h"

size_t get_number_of_dimensions(const IDataType& type) {
    if (const auto* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        return type_array->get_number_of_dimensions();
    }
    return 0;
}
size_t get_number_of_dimensions(const IColumn& column) {
    if (const auto* column_array = check_and_get_column<ColumnArray>(column)) {
        return column_array->get_number_of_dimensions();
    }
    return 0;
}

DataTypePtr get_base_type_of_array(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
    }
    return last_array ? last_array->get_nested_type() : type;
}

Array create_empty_array_field(size_t num_dimensions) {
    DCHECK(num_dimensions > 0);
    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Field::create_field<TYPE_ARRAY>(Array()));
        current_array = &current_array->back().get<Array&>();
    }
    return array;
}

size_t get_size_of_interger(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_TINYINT:
        return sizeof(int8_t);
    case PrimitiveType::TYPE_SMALLINT:
        return sizeof(int16_t);
    case PrimitiveType::TYPE_INT:
        return sizeof(int32_t);
    case PrimitiveType::TYPE_BIGINT:
        return sizeof(int64_t);
    case PrimitiveType::TYPE_LARGEINT:
        return sizeof(int128_t);
    case PrimitiveType::TYPE_BOOLEAN:
        return sizeof(uint8_t);
    default:
        throw Exception(Status::FatalError("Unknown integer type: {}", type_to_string(type)));
        return 0;
    }
}

bool is_conversion_required_between_integers(const PrimitiveType& lhs, const PrimitiveType& rhs) {
    bool is_native_int = is_int_or_bool(lhs) && is_int_or_bool(rhs);
    return !is_native_int || get_size_of_interger(lhs) > get_size_of_interger(rhs);
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {arg, {nullptr, type, type->get_name()}};

    // To prevent from null info lost, we should not call function since the function framework will wrap
    // nullable to Variant instead of the root of Variant
    // correct output: Nullable(Array(int)) -> Nullable(Variant(Nullable(Array(int))))
    // incorrect output: Nullable(Array(int)) -> Nullable(Variant(Array(int)))
    if (type->get_primitive_type() == TYPE_VARIANT) {
        // If source column is variant, so the nullable info is different from dst column
        if (arg.type->get_primitive_type() == TYPE_VARIANT) {
            *result = type->is_nullable() ? make_nullable(arg.column) : remove_nullable(arg.column);
            return Status::OK();
        }
        // set variant root column/type to from column/type
        auto variant = ColumnVariant::create(true /*always nullable*/);
        CHECK(arg.column->is_nullable());
        variant->create_root(arg.type, arg.column->assume_mutable());
        ColumnPtr nullable = ColumnNullable::create(
                variant->get_ptr(),
                check_and_get_column<ColumnNullable>(arg.column.get())->get_null_map_column_ptr());
        *result = type->is_nullable() ? nullable : variant->get_ptr();
        return Status::OK();
    }

    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, type);
    if (!function) {
        return Status::InternalError("Not found cast function {} to {}", arg.type->get_name(),
                                     type->get_name());
    }
    Block tmp_block {arguments};
    uint32_t result_column = cast_set<uint32_t>(tmp_block.columns());
    auto ctx = FunctionContext::create_context(nullptr, {}, {});

    if (arg.type->get_primitive_type() == INVALID_TYPE) {
        // cast from nothing to any type should result in nulls
        *result = type->create_column_const_with_default_value(arg.column->size())
                          ->convert_to_full_column_if_const();
        return Status::OK();
    }

    // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
    // each line in original string column.
    ctx->set_string_as_jsonb_string(true);
    ctx->set_jsonb_string_as_string(true);
    tmp_block.insert({nullptr, type, arg.name});
    RETURN_IF_ERROR(
            function->execute(ctx.get(), tmp_block, {0}, result_column, arg.column->size()));
    *result = tmp_block.get_by_position(result_column).column->convert_to_full_column_if_const();
    VLOG_DEBUG << fmt::format("{} before convert {}, after convert {}", arg.name,
                              arg.column->get_name(), (*result)->get_name());
    return Status::OK();
}

void get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                        TabletColumn& column, const ExtraInfo& ext_info) {
    column.set_name(name);
    column.set_type(data_type->get_storage_field_type());
    if (ext_info.unique_id >= 0) {
        column.set_unique_id(ext_info.unique_id);
    }
    if (ext_info.parent_unique_id >= 0) {
        column.set_parent_unique_id(ext_info.parent_unique_id);
    }
    if (!ext_info.path_info.empty()) {
        column.set_path_info(ext_info.path_info);
    }
    if (data_type->is_nullable()) {
        const auto& real_type = static_cast<const DataTypeNullable&>(*data_type);
        column.set_is_nullable(true);
        get_column_by_type(real_type.get_nested_type(), name, column, {});
        return;
    }
    if (data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
        TabletColumn child;
        get_column_by_type(assert_cast<const DataTypeArray*>(data_type.get())->get_nested_type(),
                           "", child, {});
        column.set_length(TabletColumn::get_field_length_by_type(TPrimitiveType::ARRAY, 0));
        column.add_sub_column(child);
        return;
    }
    // size is not fixed when type is string or json
    if (is_string_type(data_type->get_primitive_type()) ||
        data_type->get_primitive_type() == TYPE_JSONB) {
        column.set_length(INT_MAX);
        return;
    }
    if (is_int_or_bool(data_type->get_primitive_type()) ||
        is_string_type(data_type->get_primitive_type()) ||
        is_float_or_double(data_type->get_primitive_type())) {
        column.set_length(cast_set<int32_t>(data_type->get_size_of_value_in_memory()));
        return;
    }
    // TODO handle more types like struct/date/datetime/decimal...
    throw Exception(Status::FatalError("__builtin_unreachable"));
}

TabletColumn get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                                const ExtraInfo& ext_info) {
    TabletColumn result;
    get_column_by_type(data_type, name, result, ext_info);
    return result;
}

// check if two paths which same prefix have different structure
static bool has_different_structure_in_same_path(const PathInData::Parts& lhs,
                                                 const PathInData::Parts& rhs) {
    if (lhs.size() != rhs.size()) {
        return false; // different size means different structure
    }
    // Since we group by path string, lhs and rhs must have the same size and keys
    // We only need to check if they have different nested structure
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (lhs[i] != rhs[i]) {
            VLOG_DEBUG << fmt::format(
                    "Check different structure: {} vs {}, lhs[i].is_nested: {}, rhs[i].is_nested: "
                    "{}",
                    lhs[i].key, rhs[i].key, lhs[i].is_nested, rhs[i].is_nested);
            return true;
        }
    }
    return false;
}

Status check_variant_has_no_ambiguous_paths(const PathsInData& tuple_paths) {
    // Group paths by their string representation to reduce comparisons
    std::unordered_map<std::string, std::vector<size_t>> path_groups;

    for (size_t i = 0; i < tuple_paths.size(); ++i) {
        // same path should have same structure, so we group them by path
        path_groups[tuple_paths[i].get_path()].push_back(i);
        // print part of tuple_paths[i]
        VLOG_DEBUG << "tuple_paths[i]: " << tuple_paths[i].get_path();
    }

    // Only compare paths within the same group
    for (const auto& [path_str, indices] : path_groups) {
        if (indices.size() <= 1) {
            continue; // No conflicts possible
        }

        // Compare all pairs within this group
        for (size_t i = 0; i < indices.size(); ++i) {
            for (size_t j = 0; j < i; ++j) {
                if (has_different_structure_in_same_path(tuple_paths[indices[i]].get_parts(),
                                                         tuple_paths[indices[j]].get_parts())) {
                    return Status::DataQualityError(
                            "Ambiguous paths: {} vs {} with different nested part {} vs {}",
                            tuple_paths[indices[i]].get_path(), tuple_paths[indices[j]].get_path(),
                            tuple_paths[indices[i]].has_nested_part(),
                            tuple_paths[indices[j]].has_nested_part());
                }
            }
        }
    }
    return Status::OK();
}

Status update_least_schema_internal(const std::map<PathInData, DataTypes>& subcolumns_types,
                                    TabletSchemaSPtr& common_schema, bool update_sparse_column,
                                    int32_t variant_col_unique_id,
                                    std::set<PathInData>* path_set = nullptr) {
    PathsInData tuple_paths;
    DataTypes tuple_types;
    CHECK(common_schema.use_count() == 1);
    // Get the least common type for all paths.
    for (const auto& [key, subtypes] : subcolumns_types) {
        assert(!subtypes.empty());
        if (key.get_path() == ColumnVariant::COLUMN_NAME_DUMMY) {
            continue;
        }
        size_t first_dim = get_number_of_dimensions(*subtypes[0]);
        tuple_paths.emplace_back(key);
        for (size_t i = 1; i < subtypes.size(); ++i) {
            if (first_dim != get_number_of_dimensions(*subtypes[i])) {
                tuple_types.emplace_back(make_nullable(std::make_shared<DataTypeJsonb>()));
                LOG(INFO) << fmt::format(
                        "Uncompatible types of subcolumn '{}': {} and {}, cast to JSONB",
                        key.get_path(), subtypes[0]->get_name(), subtypes[i]->get_name());
                break;
            }
        }
        if (tuple_paths.size() == tuple_types.size()) {
            continue;
        }
        DataTypePtr common_type;
        get_least_supertype_jsonb(subtypes, &common_type);
        if (!common_type->is_nullable()) {
            common_type = make_nullable(common_type);
        }
        tuple_types.emplace_back(common_type);
    }
    CHECK_EQ(tuple_paths.size(), tuple_types.size());

    // Append all common type columns of this variant
    for (int i = 0; i < tuple_paths.size(); ++i) {
        TabletColumn common_column;
        // const std::string& column_name = variant_col_name + "." + tuple_paths[i].get_path();
        get_column_by_type(tuple_types[i], tuple_paths[i].get_path(), common_column,
                           ExtraInfo {.unique_id = -1,
                                      .parent_unique_id = variant_col_unique_id,
                                      .path_info = tuple_paths[i]});
        if (update_sparse_column) {
            common_schema->mutable_column_by_uid(variant_col_unique_id)
                    .append_sparse_column(common_column);
        } else {
            common_schema->append_column(common_column);
        }
        if (path_set != nullptr) {
            path_set->insert(tuple_paths[i]);
        }
    }
    return Status::OK();
}

Status update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  std::set<PathInData>* path_set) {
    // Types of subcolumns by path from all tuples.
    std::map<PathInData, DataTypes> subcolumns_types;

    // Collect all paths first to enable batch checking
    std::vector<PathInData> all_paths;

    for (const TabletSchemaSPtr& schema : schemas) {
        for (const TabletColumnPtr& col : schema->columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id) {
                subcolumns_types[*col->path_info_ptr()].push_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
                all_paths.push_back(*col->path_info_ptr());
            }
        }
    }

    // Batch check for conflicts
    RETURN_IF_ERROR(check_variant_has_no_ambiguous_paths(all_paths));

    for (const TabletSchemaSPtr& schema : schemas) {
        if (schema->field_index(variant_col_unique_id) == -1) {
            // maybe dropped
            continue;
        }
        for (const TabletColumnPtr& col :
             schema->column_by_uid(variant_col_unique_id).sparse_columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id &&
                // this column have been found in origin columns
                subcolumns_types.find(*col->path_info_ptr()) != subcolumns_types.end()) {
                subcolumns_types[*col->path_info_ptr()].push_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
            }
        }
    }
    return update_least_schema_internal(subcolumns_types, common_schema, false,
                                        variant_col_unique_id, path_set);
}

Status update_least_sparse_column(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  const std::set<PathInData>& path_set) {
    // Types of subcolumns by path from all tuples.
    std::map<PathInData, DataTypes> subcolumns_types;
    for (const TabletSchemaSPtr& schema : schemas) {
        if (schema->field_index(variant_col_unique_id) == -1) {
            // maybe dropped
            continue;
        }
        for (const TabletColumnPtr& col :
             schema->column_by_uid(variant_col_unique_id).sparse_columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id &&
                path_set.find(*col->path_info_ptr()) == path_set.end()) {
                subcolumns_types[*col->path_info_ptr()].push_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
            }
        }
    }
    return update_least_schema_internal(subcolumns_types, common_schema, true,
                                        variant_col_unique_id);
}

void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr& target_schema) {
    DCHECK(target.is_extracted_column());
    target.set_aggregation_method(source.aggregation());

    // 1. bloom filter
    if (target.type() != FieldType::OLAP_FIELD_TYPE_TINYINT &&
        target.type() != FieldType::OLAP_FIELD_TYPE_ARRAY &&
        target.type() != FieldType::OLAP_FIELD_TYPE_DOUBLE &&
        target.type() != FieldType::OLAP_FIELD_TYPE_FLOAT) {
        // above types are not supported in bf
        target.set_is_bf_column(source.is_bf_column());
    }

    // 2. inverted index
    const auto* source_index_meta = target_schema->inverted_index(source.unique_id());
    if (source_index_meta != nullptr) {
        // add index meta
        TabletIndex index_info = *source_index_meta;
        index_info.set_escaped_escaped_index_suffix_path(target.path_info_ptr()->get_path());
        const auto* target_index_meta = target_schema->inverted_index(
                target.parent_unique_id(), target.path_info_ptr()->get_path());
        if (target_index_meta != nullptr) {
            // already exist
            target_schema->update_index(target, IndexType::INVERTED, std::move(index_info));
        } else {
            target_schema->append_index(std::move(index_info));
        }
    }

    // 3. TODO: gnragm bf index
}

void inherit_column_attributes(TabletSchemaSPtr& schema) {
    // Add index meta if extracted column is missing index meta
    for (size_t i = 0; i < schema->num_columns(); ++i) {
        TabletColumn& col = schema->mutable_column(i);
        if (!col.is_extracted_column()) {
            continue;
        }
        if (schema->field_index(col.parent_unique_id()) == -1) {
            // parent column is missing, maybe dropped
            continue;
        }
        inherit_column_attributes(schema->column_by_uid(col.parent_unique_id()), col, schema);
    }
}

Status get_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                               const TabletSchemaSPtr& base_schema, TabletSchemaSPtr& output_schema,
                               bool check_schema_size) {
    std::vector<int32_t> variant_column_unique_id;
    // Construct a schema excluding the extracted columns and gather unique identifiers for variants.
    // Ensure that the output schema also excludes these extracted columns. This approach prevents
    // duplicated paths following the update_least_common_schema process.
    auto build_schema_without_extracted_columns = [&](const TabletSchemaSPtr& base_schema) {
        output_schema = std::make_shared<TabletSchema>();
        // not copy columns but only shadow copy other attributes
        output_schema->shawdow_copy_without_columns(*base_schema);
        // Get all columns without extracted columns and collect variant col unique id
        for (const TabletColumnPtr& col : base_schema->columns()) {
            if (col->is_variant_type()) {
                variant_column_unique_id.push_back(col->unique_id());
            }
            if (!col->is_extracted_column()) {
                output_schema->append_column(*col);
            }
        }
    };
    if (base_schema == nullptr) {
        // Pick tablet schema with max schema version
        auto max_version_schema =
                *std::max_element(schemas.cbegin(), schemas.cend(),
                                  [](const TabletSchemaSPtr a, const TabletSchemaSPtr b) {
                                      return a->schema_version() < b->schema_version();
                                  });
        CHECK(max_version_schema);
        build_schema_without_extracted_columns(max_version_schema);
    } else {
        // use input base_schema schema as base schema
        build_schema_without_extracted_columns(base_schema);
    }

    // schema consists of two parts, static part of columns, variant columns (include extracted columns and sparse columns)
    //     static     extracted       sparse
    // | --------- | ----------- | ------------|
    // If a sparse column in one schema's is found in another schema's extracted columns
    // move it out of the sparse column and merge it into the extracted column.
    //                     static                extracted                         sparse
    //                 | --------- | ----------- ------------ ------- ---------| ------------|
    //    schema 1:       k (int)     v:a (float)       v:c (string)               v:b (int)
    //    schema 2:       k (int)     v:a (float)       v:b (bigint)               v:d (string)
    //    schema 3:       k (int)     v:a (double)      v:b (smallint)
    //    result :        k (int)     v:a (double)  v:b (bigint) v:c (string)      v:d (string)
    for (int32_t unique_id : variant_column_unique_id) {
        std::set<PathInData> path_set;
        // 1. cast extracted column to common type
        // path set is used to record the paths of those sparse columns that have been merged into the extracted columns, eg: v:b
        RETURN_IF_ERROR(update_least_common_schema(schemas, output_schema, unique_id, &path_set));
        // 2. cast sparse column to common type, exclude the columns from the path set
        RETURN_IF_ERROR(update_least_sparse_column(schemas, output_schema, unique_id, path_set));
    }

    inherit_column_attributes(output_schema);
    if (check_schema_size &&
        output_schema->columns().size() > config::variant_max_merged_tablet_schema_size) {
        return Status::DataQualityError("Reached max column size limit {}",
                                        config::variant_max_merged_tablet_schema_size);
    }

    return Status::OK();
}

Status _parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                              const ParseConfig& config) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto column_ref = block.get_by_position(variant_pos[i]).column;
        bool is_nullable = column_ref->is_nullable();
        const auto& column = remove_nullable(column_ref);
        const auto& var = assert_cast<const ColumnVariant&>(*column.get());
        var.assume_mutable_ref().finalize();

        MutableColumnPtr variant_column;
        if (!var.is_scalar_variant()) {
            variant_column = var.assume_mutable();
            // already parsed
            continue;
        }
        ColumnPtr scalar_root_column;
        if (var.get_root_type()->get_primitive_type() == TYPE_JSONB) {
            // TODO more efficient way to parse jsonb type, currently we just convert jsonb to
            // json str and parse them into variant
            RETURN_IF_ERROR(cast_column({var.get_root(), var.get_root_type(), ""},
                                        var.get_root()->is_nullable()
                                                ? make_nullable(std::make_shared<DataTypeString>())
                                                : std::make_shared<DataTypeString>(),
                                        &scalar_root_column));
            if (scalar_root_column->is_nullable()) {
                scalar_root_column = assert_cast<const ColumnNullable*>(scalar_root_column.get())
                                             ->get_nested_column_ptr();
            }
        } else {
            const auto& root = *var.get_root();
            scalar_root_column =
                    root.is_nullable()
                            ? assert_cast<const ColumnNullable&>(root).get_nested_column_ptr()
                            : var.get_root();
        }

        if (scalar_root_column->is_column_string()) {
            variant_column = ColumnVariant::create(true);
            parse_json_to_variant(*variant_column.get(),
                                  assert_cast<const ColumnString&>(*scalar_root_column), config);
        } else {
            // Root maybe other types rather than string like ColumnVariant(Int32).
            // In this case, we should finlize the root and cast to JSON type
            auto expected_root_type =
                    make_nullable(std::make_shared<ColumnVariant::MostCommonType>());
            const_cast<ColumnVariant&>(var).ensure_root_node_type(expected_root_type);
            variant_column = var.assume_mutable();
        }

        // Wrap variant with nullmap if it is nullable
        ColumnPtr result = variant_column->get_ptr();
        if (is_nullable) {
            const auto& null_map =
                    assert_cast<const ColumnNullable&>(*column_ref).get_null_map_column_ptr();
            result = ColumnNullable::create(result, null_map);
        }
        block.get_by_position(variant_pos[i]).column = result;
    }
    return Status::OK();
}

Status parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                             const ParseConfig& config) {
    // Parse each variant column from raw string column
    RETURN_IF_CATCH_EXCEPTION({
        return vectorized::schema_util::_parse_variant_columns(block, variant_pos, config);
    });
}

Status encode_variant_sparse_subcolumns(ColumnVariant& column) {
    // Make sure the root node is jsonb storage type
    auto expected_root_type = make_nullable(std::make_shared<ColumnVariant::MostCommonType>());
    column.ensure_root_node_type(expected_root_type);
    RETURN_IF_ERROR(column.merge_sparse_to_root_column());
    return Status::OK();
}

// sort by paths in lexicographical order
vectorized::ColumnVariant::Subcolumns get_sorted_subcolumns(
        const vectorized::ColumnVariant::Subcolumns& subcolumns) {
    // sort by paths in lexicographical order
    vectorized::ColumnVariant::Subcolumns sorted = subcolumns;
    std::sort(sorted.begin(), sorted.end(), [](const auto& lhsItem, const auto& rhsItem) {
        return lhsItem->path < rhsItem->path;
    });
    return sorted;
}

// ---------------------------

std::string dump_column(DataTypePtr type, const ColumnPtr& col) {
    Block tmp;
    tmp.insert(ColumnWithTypeAndName {col, type, col->get_name()});
    return tmp.dump_data(0, tmp.rows());
}

// ---------------------------
Status extract(ColumnPtr source, const PathInData& path, MutableColumnPtr& dst) {
    auto type_string = std::make_shared<DataTypeString>();
    std::string jsonpath = path.to_jsonpath();
    bool is_nullable = source->is_nullable();
    auto json_type = is_nullable ? make_nullable(std::make_shared<DataTypeJsonb>())
                                 : std::make_shared<DataTypeJsonb>();
    ColumnsWithTypeAndName arguments {
            {source, json_type, ""},
            {type_string->create_column_const(
                     1, Field::create_field<TYPE_STRING>(String(jsonpath.data(), jsonpath.size()))),
             type_string, ""}};
    auto function =
            SimpleFunctionFactory::instance().get_function("jsonb_extract", arguments, json_type);
    if (!function) {
        return Status::InternalError("Not found function jsonb_extract");
    }
    Block tmp_block {arguments};
    vectorized::ColumnNumbers argnum;
    argnum.emplace_back(0);
    argnum.emplace_back(1);
    uint32_t result_column = cast_set<uint32_t>(tmp_block.columns());
    tmp_block.insert({nullptr, json_type, ""});
    RETURN_IF_ERROR(function->execute(nullptr, tmp_block, argnum, result_column, source->size()));
    dst = tmp_block.get_by_position(result_column)
                  .column->convert_to_full_column_if_const()
                  ->assume_mutable();
    return Status::OK();
}

bool has_schema_index_diff(const TabletSchema* new_schema, const TabletSchema* old_schema,
                           int32_t new_col_idx, int32_t old_col_idx) {
    const auto& column_new = new_schema->column(new_col_idx);
    const auto& column_old = old_schema->column(old_col_idx);

    if (column_new.is_bf_column() != column_old.is_bf_column() ||
        column_new.has_bitmap_index() != column_old.has_bitmap_index()) {
        return true;
    }

    bool new_schema_has_inverted_index = new_schema->inverted_index(column_new);
    bool old_schema_has_inverted_index = old_schema->inverted_index(column_old);

    return new_schema_has_inverted_index != old_schema_has_inverted_index;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized::schema_util
