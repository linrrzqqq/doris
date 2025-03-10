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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Status;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTabletStat;
import org.apache.doris.thrift.TTabletStatResult;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * TabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class TabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    private final ExecutorService executor = ThreadPoolManager.newDaemonFixedThreadPool(
            Config.tablet_stat_mgr_threads_num > 0
                    ? Config.tablet_stat_mgr_threads_num
                    : Runtime.getRuntime().availableProcessors(),
            1024, "tablet-stat-mgr", true);

    private MarkedCountDownLatch<Long, Backend> updateTabletStatsLatch = null;

    public TabletStatMgr() {
        super("tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends;
        try {
            backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        } catch (AnalysisException e) {
            LOG.warn("can't get backends info", e);
            return;
        }
        long start = System.currentTimeMillis();
        // no need to get tablet stat if backend is not alive
        List<Backend> aliveBackends = backends.values().stream().filter(Backend::isAlive)
                .collect(Collectors.toList());
        updateTabletStatsLatch = new MarkedCountDownLatch<>(aliveBackends.size());
        aliveBackends.forEach(backend -> {
            updateTabletStatsLatch.addMark(backend.getId(), backend);
            executor.submit(() -> {
                BackendService.Client client = null;
                TNetworkAddress address = null;
                boolean ok = false;
                try {
                    address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                    client = ClientPool.backendPool.borrowObject(address);
                    TTabletStatResult result = client.getTabletStat();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get tablet stat from backend: {}, num: {}", backend.getId(),
                                result.getTabletsStatsSize());
                    }
                    updateTabletStat(backend.getId(), result);
                    updateTabletStatsLatch.markedCountDown(backend.getId(), backend);
                    ok = true;
                } catch (Throwable e) {
                    updateTabletStatsLatch.markedCountDownWithStatus(backend.getId(), backend, Status.CANCELLED);
                    LOG.warn("task exec error. backend[{}]", backend.getId(), e);
                }

                try {
                    if (ok) {
                        ClientPool.backendPool.returnObject(address, client);
                    } else {
                        ClientPool.backendPool.invalidateObject(address, client);
                    }
                } catch (Throwable e) {
                    LOG.warn("client pool recyle error. backend[{}]", backend.getId(), e);
                }
            });
        });
        waitForTabletStatUpdate();

        if (LOG.isDebugEnabled()) {
            LOG.debug("finished to get tablet stat of all backends. cost: {} ms",
                    (System.currentTimeMillis() - start));
        }

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                // Will process OlapTable and MTMV
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;

                Long tableDataSize = 0L;
                Long tableTotalReplicaDataSize = 0L;

                Long tableTotalLocalIndexSize = 0L;
                Long tableTotalLocalSegmentSize = 0L;
                Long tableTotalRemoteIndexSize = 0L;
                Long tableTotalRemoteSegmentSize = 0L;

                Long tableRemoteDataSize = 0L;

                Long tableReplicaCount = 0L;

                Long tableRowCount = 0L;

                if (!table.readLockIfExist()) {
                    continue;
                }
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long version = partition.getVisibleVersion();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            boolean indexReported = true;
                            for (Tablet tablet : index.getTablets()) {

                                Long tabletDataSize = 0L;
                                Long tabletRemoteDataSize = 0L;

                                Long tabletRowCount = Long.MAX_VALUE;

                                boolean tabletReported = false;
                                for (Replica replica : tablet.getReplicas()) {
                                    LOG.debug("Table {} replica {} current version {}, report version {}",
                                            olapTable.getName(), replica.getId(),
                                            replica.getVersion(), replica.getLastReportVersion());
                                    // Replica with less row count is more accurate than the others
                                    // when replicas' version are identical. Because less row count
                                    // means this replica does more compaction than the others.
                                    if (replica.checkVersionCatchUp(version, false)
                                            && replica.getRowCount() < tabletRowCount) {
                                        // 1. If replica version and reported replica version are all equal to
                                        // PARTITION_INIT_VERSION, set tabletReported to true, which indicates this
                                        // tablet is empty for sure when previous report.
                                        // 2. If last report version is larger than PARTITION_INIT_VERSION, set
                                        // tabletReported to true as well. That is, we only guarantee all replicas of
                                        // the tablet are reported for the init version.
                                        // e.g. When replica version is 2, but last reported version is 1,
                                        // tabletReported would be false.
                                        if (replica.getVersion() == Partition.PARTITION_INIT_VERSION
                                                && replica.getLastReportVersion() == Partition.PARTITION_INIT_VERSION
                                                || replica.getLastReportVersion() > Partition.PARTITION_INIT_VERSION) {
                                            tabletReported = true;
                                        }
                                        tabletRowCount = replica.getRowCount();
                                    }

                                    if (replica.getDataSize() > tabletDataSize) {
                                        tabletDataSize = replica.getDataSize();
                                    }
                                    tableTotalReplicaDataSize += replica.getDataSize();

                                    if (replica.getRemoteDataSize() > tabletRemoteDataSize) {
                                        tabletRemoteDataSize = replica.getRemoteDataSize();
                                    }
                                    tableReplicaCount++;
                                    tableTotalLocalIndexSize += replica.getLocalInvertedIndexSize();
                                    tableTotalLocalSegmentSize += replica.getLocalSegmentSize();
                                    tableTotalRemoteIndexSize += replica.getRemoteInvertedIndexSize();
                                    tableTotalRemoteSegmentSize += replica.getRemoteSegmentSize();
                                }

                                tableDataSize += tabletDataSize;
                                tableRemoteDataSize += tabletRemoteDataSize;

                                // When all BEs are down, avoid set Long.MAX_VALUE to index and table row count. Use 0.
                                if (tabletRowCount == Long.MAX_VALUE) {
                                    tabletRowCount = 0L;
                                }
                                tableRowCount += tabletRowCount;
                                indexRowCount += tabletRowCount;
                                // Only when all tablets of this index are reported, we set indexReported to true.
                                indexReported = indexReported && tabletReported;
                            } // end for tablets
                            index.setRowCountReported(indexReported);
                            index.setRowCount(indexRowCount);
                            LOG.debug("Table {} index {} all tablets reported[{}], row count {}",
                                    olapTable.getName(), olapTable.getIndexNameById(index.getId()),
                                    indexReported, indexRowCount);
                        } // end for indices
                    } // end for partitions

                    // this is only one thread to update table statistics, readLock is enough
                    olapTable.setStatistics(new OlapTable.Statistics(db.getName(), table.getName(),
                            tableDataSize, tableTotalReplicaDataSize,
                            tableRemoteDataSize, tableReplicaCount, tableRowCount, 0L, 0L,
                            tableTotalLocalIndexSize, tableTotalLocalSegmentSize,
                            tableTotalRemoteIndexSize, tableTotalRemoteSegmentSize));

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("finished to set row num for table: {} in database: {}",
                                 table.getName(), db.getFullName());
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    public void waitForTabletStatUpdate() {
        boolean ok = true;
        try {
            if (!updateTabletStatsLatch.await(600, TimeUnit.SECONDS)) {
                LOG.info("timeout waiting {} update tablet stats tasks finish after {} seconds.",
                        updateTabletStatsLatch.getCount(), 600);
                ok = false;
            }
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException, {}", this, e);
        }
        if (!ok || !updateTabletStatsLatch.getStatus().ok()) {
            List<Long> unfinishedBackendIds = updateTabletStatsLatch.getLeftMarks().stream()
                    .map(Map.Entry::getKey).collect(Collectors.toList());
            Status status = Status.TIMEOUT;
            if (!updateTabletStatsLatch.getStatus().ok()) {
                status = updateTabletStatsLatch.getStatus();
            }
            LOG.warn("Failed to update tablet stats reason: {}, unfinished backends: {}",
                    status.getErrorMsg(), unfinishedBackendIds);
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_UPDATE_TABLET_STAT_FAILED.increase(1L);
            }
        }
    }

    private void updateTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        if (result.isSetTabletStatList()) {
            for (TTabletStat stat : result.getTabletStatList()) {
                if (invertedIndex.getTabletMeta(stat.getTabletId()) != null) {
                    Replica replica = invertedIndex.getReplica(stat.getTabletId(), beId);
                    if (replica != null) {
                        replica.setDataSize(stat.getDataSize());
                        replica.setRemoteDataSize(stat.getRemoteDataSize());
                        replica.setLocalInvertedIndexSize(stat.getLocalIndexSize());
                        replica.setLocalSegmentSize(stat.getLocalSegmentSize());
                        replica.setRemoteInvertedIndexSize(stat.getRemoteIndexSize());
                        replica.setRemoteSegmentSize(stat.getRemoteSegmentSize());
                        replica.setRowCount(stat.getRowCount());
                        replica.setTotalVersionCount(stat.getTotalVersionCount());
                        replica.setVisibleVersionCount(stat.isSetVisibleVersionCount() ? stat.getVisibleVersionCount()
                                : stat.getTotalVersionCount());
                        // Older version BE doesn't set visible version. Set it to max for compatibility.
                        replica.setLastReportVersion(stat.isSetVisibleVersion() ? stat.getVisibleVersion()
                                : Long.MAX_VALUE);
                    }
                }
            }
        } else {
            for (Map.Entry<Long, TTabletStat> entry : result.getTabletsStats().entrySet()) {
                if (invertedIndex.getTabletMeta(entry.getKey()) == null) {
                    // the replica is obsolete, ignore it.
                    continue;
                }
                Replica replica = invertedIndex.getReplica(entry.getKey(), beId);
                if (replica == null) {
                    // replica may be deleted from catalog, ignore it.
                    continue;
                }
                // TODO(cmy) no db lock protected. I think it is ok even we get wrong row num
                replica.setDataSize(entry.getValue().getDataSize());
                replica.setRowCount(entry.getValue().getRowCount());
            }
        }
    }
}
