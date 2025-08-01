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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical file scan for external catalog.
 */
public class LogicalFileScan extends LogicalCatalogRelation {
    protected final SelectedPartitions selectedPartitions;
    protected final Optional<TableSample> tableSample;
    protected final Optional<TableSnapshot> tableSnapshot;
    protected final Optional<TableScanParams> scanParams;

    public LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
                           Collection<Slot> operativeSlots,
                           Optional<TableSample> tableSample, Optional<TableSnapshot> tableSnapshot,
                           Optional<TableScanParams> scanParams) {
        this(id, table, qualifier,
                table.initSelectedPartitions(MvccUtil.getSnapshotFromContext(table)),
                operativeSlots, ImmutableList.of(),
                tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty());
    }

    /**
     * Constructor for LogicalFileScan.
     */
    protected LogicalFileScan(RelationId id, ExternalTable table, List<String> qualifier,
            SelectedPartitions selectedPartitions, Collection<Slot> operativeSlots,
            List<NamedExpression> virtualColumns, Optional<TableSample> tableSample,
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties) {
        super(id, PlanType.LOGICAL_FILE_SCAN, table, qualifier, operativeSlots, virtualColumns,
                groupExpression, logicalProperties);
        this.selectedPartitions = selectedPartitions;
        this.tableSample = tableSample;
        this.tableSnapshot = tableSnapshot;
        this.scanParams = scanParams;
    }

    public SelectedPartitions getSelectedPartitions() {
        return selectedPartitions;
    }

    public Optional<TableSample> getTableSample() {
        return tableSample;
    }

    public Optional<TableSnapshot> getTableSnapshot() {
        return tableSnapshot;
    }

    public Optional<TableScanParams> getScanParams() {
        return scanParams;
    }

    @Override
    public ExternalTable getTable() {
        Preconditions.checkArgument(table instanceof ExternalTable,
                "LogicalFileScan's table must be ExternalTable, but table is " + table.getClass().getSimpleName());
        return (ExternalTable) table;
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalFileScan",
                "qualified", qualifiedName(),
                "output", getOutput(),
                "operativeCols", operativeSlots,
                "stats", statistics
        );
    }

    @Override
    public LogicalFileScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, logicalProperties);
    }

    public LogicalFileScan withSelectedPartitions(SelectedPartitions selectedPartitions) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.of(getLogicalProperties()));
    }

    @Override
    public LogicalFileScan withRelationId(RelationId relationId) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, Optional.empty(), Optional.empty());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFileScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(selectedPartitions, ((LogicalFileScan) o).selectedPartitions);
    }

    /**
     * SelectedPartitions contains the selected partitions and the total partition number.
     * Mainly for hive table partition pruning.
     */
    public static class SelectedPartitions {
        // NOT_PRUNED means the Nereids planner does not handle the partition pruning.
        // This can be treated as the initial value of SelectedPartitions.
        // Or used to indicate that the partition pruning is not processed.
        public static SelectedPartitions NOT_PRUNED = new SelectedPartitions(0, ImmutableMap.of(), false);
        /**
         * total partition number
         */
        public final long totalPartitionNum;
        /**
         * partition name -> partition item
         */
        public final Map<String, PartitionItem> selectedPartitions;
        /**
         * true means the result is after partition pruning
         * false means the partition pruning is not processed.
         */
        public final boolean isPruned;

        /**
         * Constructor for SelectedPartitions.
         */
        public SelectedPartitions(long totalPartitionNum, Map<String, PartitionItem> selectedPartitions,
                boolean isPruned) {
            this.totalPartitionNum = totalPartitionNum;
            this.selectedPartitions = ImmutableMap.copyOf(Objects.requireNonNull(selectedPartitions,
                    "selectedPartitions is null"));
            this.isPruned = isPruned;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SelectedPartitions that = (SelectedPartitions) o;
            return isPruned == that.isPruned && Objects.equals(
                    selectedPartitions.keySet(), that.selectedPartitions.keySet());
        }

        @Override
        public int hashCode() {
            return Objects.hash(selectedPartitions, isPruned);
        }
    }

    @Override
    public LogicalFileScan withOperativeSlots(Collection<Slot> operativeSlots) {
        return new LogicalFileScan(relationId, (ExternalTable) table, qualifier,
                selectedPartitions, operativeSlots, virtualColumns, tableSample, tableSnapshot,
                scanParams, groupExpression, Optional.of(getLogicalProperties()));
    }

    @Override
    public List<Slot> getOperativeSlots() {
        return operativeSlots;
    }
}
