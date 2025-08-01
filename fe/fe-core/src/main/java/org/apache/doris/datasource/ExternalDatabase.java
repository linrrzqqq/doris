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

package org.apache.doris.datasource;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Base class of external database.
 *
 * @param <T> External table type is ExternalTable or its subclass.
 */
public abstract class ExternalDatabase<T extends ExternalTable>
        implements DatabaseIf<T>, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalDatabase.class);

    protected MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock(true);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "remoteName")
    protected String remoteName;
    @SerializedName(value = "dbProperties")
    protected DatabaseProperty dbProperties = new DatabaseProperty();
    @SerializedName(value = "initialized")
    protected boolean initialized = false;
    // Cache of table name to table id.
    protected Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    @SerializedName(value = "idToTbl")
    protected Map<Long, T> idToTbl = Maps.newConcurrentMap();
    // table name lower case -> table name
    private Map<String, String> lowerCaseToTableName = Maps.newConcurrentMap();
    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;
    protected final InitDatabaseLog.Type dbLogType;
    protected ExternalCatalog extCatalog;

    private MetaCache<T> metaCache;

    private volatile boolean isInitializing = false;

    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param id Database id.
     * @param name Database name.
     * @param remoteName Remote database name.
     */
    public ExternalDatabase(ExternalCatalog extCatalog, long id, String name, String remoteName,
            InitDatabaseLog.Type dbLogType) {
        this.extCatalog = extCatalog;
        this.id = id;
        this.name = name;
        this.remoteName = remoteName;
        this.dbLogType = dbLogType;
    }

    public void setExtCatalog(ExternalCatalog extCatalog) {
        this.extCatalog = extCatalog;
    }

    public void setRemoteName(String remoteName) {
        this.remoteName = remoteName;
    }

    public void setTableExtCatalog(ExternalCatalog extCatalog) {
        for (T table : idToTbl.values()) {
            table.setCatalog(extCatalog);
        }
    }

    public synchronized void resetToUninitialized() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("resetToUninitialized db name {}, id {}, isInitializing: {}, initialized: {}",
                    this.name, this.id, isInitializing, initialized, new Exception());
        }
        this.initialized = false;
        this.lowerCaseToTableName = Maps.newConcurrentMap();
        if (extCatalog.getUseMetaCache().isPresent()) {
            if (extCatalog.getUseMetaCache().get() && metaCache != null) {
                metaCache.invalidateAll();
            } else if (!extCatalog.getUseMetaCache().get()) {
                for (T table : idToTbl.values()) {
                    table.unsetObjectCreated();
                }
            }
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDbCache(getCatalog().getId(), getFullName());
    }

    public boolean isInitialized() {
        return initialized;
    }

    public final void makeSureInitialized() {
        // Must call this method before any operation on the database to avoid deadlock of synchronized block
        extCatalog.makeSureInitialized();
        synchronized (this) {
            if (isInitializing) {
                return;
            }
            isInitializing = true;
            try {
                if (!initialized) {
                    if (extCatalog.getUseMetaCache().get()) {
                        buildMetaCache();
                        setLastUpdateTime(System.currentTimeMillis());
                    } else {
                        if (!Env.getCurrentEnv().isMaster()) {
                            // Forward to master and wait the journal to replay.
                            int waitTimeOut = ConnectContext.get() == null ? 300
                                    : ConnectContext.get().getExecTimeoutS();
                            MasterCatalogExecutor remoteExecutor = new MasterCatalogExecutor(waitTimeOut * 1000);
                            try {
                                remoteExecutor.forward(extCatalog.getId(), id);
                            } catch (Exception e) {
                                Util.logAndThrowRuntimeException(LOG,
                                        String.format("failed to forward init external db %s operation to master",
                                                name), e);
                            }
                            return;
                        }
                        init();
                    }
                    initialized = true;
                }
            } catch (Exception e) {
                LOG.warn("failed to init db {}, id {}, isInitializing: {}, initialized: {}",
                        this.name, this.id, isInitializing, initialized, e);
                initialized = false;
            } finally {
                isInitializing = false;
            }
        }
    }

    public void replayInitDb(InitDatabaseLog log, ExternalCatalog catalog) {
        // If the remote name is missing during upgrade, all tables in the Map will be reinitialized.
        if ((log.getCreateCount() > 0 && (log.getRemoteTableNames() == null || log.getRemoteTableNames().isEmpty()))
                || (log.getRefreshCount() > 0
                && (log.getRefreshRemoteTableNames() == null || log.getRefreshRemoteTableNames().isEmpty()))) {
            tableNameToId = Maps.newConcurrentMap();
            idToTbl = Maps.newConcurrentMap();
            lastUpdateTime = log.getLastUpdateTime();
            initialized = false;
            return;
        }

        Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
        Map<Long, T> tmpIdToTbl = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            Optional<T> table = getTableForReplay(log.getRefreshTableIds().get(i));
            // When upgrade cluster with this pr: https://github.com/apache/doris/pull/27666
            // Maybe there are some create table events will be skipped
            // if the cluster has any hms catalog(s) with hms event listener enabled.
            // So we need add a validation here to avoid table(s) not found, this is just a temporary solution
            // because later we will remove all the logics about InitCatalogLog/InitDatabaseLog.
            if (table.isPresent()) {
                table.get().setRemoteName(log.getRefreshRemoteTableNames().get(i));
                tmpTableNameToId.put(table.get().getName(), table.get().getId());
                tmpIdToTbl.put(table.get().getId(), table.get());

                // Add logic to set the database if missing
                if (table.get().getDb() == null) {
                    table.get().setDb(this);
                }
                LOG.info("Synchronized table (refresh): [Name: {}, ID: {}]", table.get().getName(),
                        table.get().getId());
            }
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            T table = buildTableForInit(log.getRemoteTableNames().get(i), log.getCreateTableNames().get(i),
                            log.getCreateTableIds().get(i), catalog, this, false);
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);

            // Add logic to set the database if missing
            if (table.getDb() == null) {
                table.setDb(this);
            }
            LOG.info("Synchronized table (create): [Name: {}, ID: {}, Remote Name: {}]",
                    table.getName(), table.getId(), log.getRemoteTableNames().get(i));
        }
        // Check whether the remoteName and db Tbl db in tmpIdToTbl is empty
        for (T table : tmpIdToTbl.values()) {
            if (Strings.isNullOrEmpty(table.getRemoteName())
                    || table.getDb() == null) {
                LOG.info("Table [{}] remoteName or database is empty, mark as uninitialized",
                        table.getName());
                tableNameToId = Maps.newConcurrentMap();
                idToTbl = Maps.newConcurrentMap();
                lastUpdateTime = log.getLastUpdateTime();
                initialized = false;
                return;
            }
        }
        tableNameToId = tmpTableNameToId;
        idToTbl = tmpIdToTbl;
        lastUpdateTime = log.getLastUpdateTime();
        initialized = true;
    }

    private void init() {
        InitDatabaseLog initDatabaseLog = new InitDatabaseLog();
        initDatabaseLog.setType(dbLogType);
        initDatabaseLog.setCatalogId(extCatalog.getId());
        initDatabaseLog.setDbId(id);
        List<Pair<String, String>> tableNamePairs = listTableNames();
        if (tableNamePairs != null) {
            Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
            Map<Long, T> tmpIdToTbl = Maps.newHashMap();
            for (Pair<String, String> pair : tableNamePairs) {
                String remoteTableName = pair.first;
                String localTableName = pair.second;
                long tblId;
                if (tableNameToId != null && tableNameToId.containsKey(localTableName)) {
                    tblId = tableNameToId.get(localTableName);
                    tmpTableNameToId.put(localTableName, tblId);
                    T table = idToTbl.get(tblId);
                    // If the remote name is missing during upgrade, all tables in the Map will be reinitialized.
                    if (Strings.isNullOrEmpty(table.getRemoteName())) {
                        table.setRemoteName(remoteTableName);
                    }
                    // If the db is missing, set it.
                    if (table.getDb() == null) {
                        table.setDb(this);
                    }
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addRefreshTable(tblId, remoteTableName);
                } else {
                    tblId = Env.getCurrentEnv().getNextId();
                    tmpTableNameToId.put(localTableName, tblId);
                    T table = buildTableForInit(remoteTableName, localTableName, tblId, extCatalog, this, false);
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addCreateTable(tblId, localTableName, remoteTableName);
                }
            }
            tableNameToId = tmpTableNameToId;
            idToTbl = tmpIdToTbl;
        }

        lastUpdateTime = System.currentTimeMillis();
        initDatabaseLog.setLastUpdateTime(lastUpdateTime);
        Env.getCurrentEnv().getEditLog().logInitExternalDb(initDatabaseLog);
    }

    private void buildMetaCache() {
        if (metaCache == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("buildMetaCache for database: {}:{}", this.name, this.id, new Exception());
            }
            metaCache = Env.getCurrentEnv().getExtMetaCacheMgr().buildMetaCache(
                    name,
                    OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                    OptionalLong.of(Config.external_cache_refresh_time_minutes * 60L),
                    Config.max_meta_object_cache_num,
                    ignored -> listTableNames(),
                    localTableName -> Optional.ofNullable(
                            buildTableForInit(null, localTableName,
                                    Util.genIdByName(extCatalog.getName(), name, localTableName),
                                    extCatalog,
                                    this, true)),
                    (key, value, cause) -> value.ifPresent(ExternalTable::unsetObjectCreated));
        }
    }

    private List<Pair<String, String>> listTableNames() {
        List<Pair<String, String>> tableNames;
        lowerCaseToTableName.clear();
        if (name.equals(InfoSchemaDb.DATABASE_NAME)) {
            tableNames = ExternalInfoSchemaDatabase.listTableNames().stream()
                    .map(tableName -> {
                        lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
                        return Pair.of(tableName, tableName);
                    })
                    .collect(Collectors.toList());
        } else if (name.equals(MysqlDb.DATABASE_NAME)) {
            tableNames = ExternalMysqlDatabase.listTableNames().stream()
                    .map(tableName -> {
                        lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
                        return Pair.of(tableName, tableName);
                    })
                    .collect(Collectors.toList());
        } else {
            tableNames = extCatalog.listTableNames(null, remoteName).stream().map(tableName -> {
                String localTableName = extCatalog.fromRemoteTableName(remoteName, tableName);
                if (this.isStoredTableNamesLowerCase()) {
                    localTableName = localTableName.toLowerCase();
                }
                lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
                return Pair.of(tableName, localTableName);
            }).collect(Collectors.toList());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("after list tables in {}.{}, lowerCaseToTableName: {}",
                    getCatalog().getName(), getFullName(), lowerCaseToTableName);
        }
        // Check for conflicts when stored table names or meta names are case-insensitive
        if (Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
                || this.isStoredTableNamesLowerCase()
                || this.isTableNamesCaseInsensitive()) {
            // Map to track lowercased local names and their corresponding remote names
            Map<String, List<String>> lowerCaseToRemoteNames = Maps.newHashMap();

            // Collect lowercased local names and their remote counterparts
            for (Pair<String, String> pair : tableNames) {
                String lowerCaseLocalName = pair.value().toLowerCase();
                lowerCaseToRemoteNames.computeIfAbsent(lowerCaseLocalName, k -> Lists.newArrayList()).add(pair.key());
            }

            // Identify conflicts: multiple remote names mapping to the same lowercased local name
            List<String> conflicts = lowerCaseToRemoteNames.values().stream()
                    .filter(remoteNames -> remoteNames.size() > 1) // Conflict: more than one remote name
                    .flatMap(List::stream) // Collect all conflicting remote names
                    .collect(Collectors.toList());

            // Throw exception if conflicts are found
            if (!conflicts.isEmpty()) {
                throw new RuntimeException(String.format(
                        ExternalCatalog.FOUND_CONFLICTING + " table names under case-insensitive conditions. "
                                + "Conflicting remote table names: %s in remote database '%s' under catalog '%s'. "
                                + "Please use meta_names_mapping to handle name mapping.",
                        String.join(", ", conflicts), remoteName, extCatalog.getName()));
            }
        }
        return tableNames;
    }

    public T buildTableForInit(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog, ExternalDatabase db, boolean checkExists) {

        // Step 1: Resolve local table name if not provided
        if (localTableName == null && remoteTableName != null) {
            localTableName = extCatalog.fromRemoteTableName(remoteName, remoteTableName);
        }

        // Step 2: Check if the table exists in the system, if the `checkExists` flag is enabled
        if (checkExists && (!FeConstants.runningUnitTest || this instanceof TestExternalDatabase)) {
            try {
                List<String> tblNames = Lists.newArrayList(getTableNamesWithLock());
                if (!tblNames.contains(localTableName)) {
                    // reset the table name list to ensure it is up-to-date
                    resetMetaCacheNames();
                    tblNames = Lists.newArrayList(getTableNamesWithLock());
                    if (!tblNames.contains(localTableName)) {
                        LOG.warn("Table {} does not exist in the remote database {}. Skipping initialization.",
                                localTableName, this.name);
                        return null;
                    }
                }
            } catch (RuntimeException e) {
                // Handle "Found conflicting" exception explicitly
                if (e.getMessage().contains(ExternalCatalog.FOUND_CONFLICTING)) {
                    LOG.error(e.getMessage());
                    throw e; // Rethrow to let the caller handle this critical issue
                } else {
                    // Any errors other than name conflicts, we default to not finding the table
                    LOG.warn("Failed to check existence of table {} in the remote system. Ignoring this table.",
                            localTableName, e);
                    return null;
                }
            } catch (Exception e) {
                // If connection fails, treat the table as non-existent
                LOG.warn("Failed to check existence of table {} in the remote system. Ignoring this table.",
                        localTableName, e);
                return null;
            }
        }

        // Step 3: Resolve remote table name if using meta cache and it is not provided
        if (remoteTableName == null && extCatalog.useMetaCache.get()) {
            if (Boolean.parseBoolean(extCatalog.getLowerCaseMetaNames())
                    || !Strings.isNullOrEmpty(extCatalog.getMetaNamesMapping())
                    || this.isStoredTableNamesLowerCase()) {
                remoteTableName = metaCache.getRemoteName(localTableName);
                if (remoteTableName == null) {
                    LOG.warn("Could not resolve remote table name for local table: {}", localTableName);
                    return null;
                }
            } else {
                remoteTableName = localTableName;
            }
        }

        // Step 4: Build and return the table instance using the resolved names and other parameters
        return buildTableInternal(remoteTableName, localTableName, tblId, catalog, db);
    }

    protected abstract T buildTableInternal(String remoteTableName, String localTableName, long tblId,
            ExternalCatalog catalog, ExternalDatabase db);

    /**
     * This method will try getting table from cache only,
     * If there is no cache, it will return empty.
     * Different from "getTableNullable()", this method will not visit the remote catalog to get table
     * when it does not exist in cache.
     * This is used for replaying the metadata, to avoid exception when trying to get table from remote catalog.
     *
     * @param tableId
     * @return
     */
    public Optional<T> getTableForReplay(long tableId) {
        Preconditions.checkState(extCatalog.getUseMetaCache().isPresent(), extCatalog.getName() + "." + name);
        if (extCatalog.getUseMetaCache().get()) {
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.getMetaObjById(tableId);
        } else {
            return Optional.ofNullable(idToTbl.get(tableId));
        }
    }

    /**
     * Same as "getTableForReplay(long tableId)", use "tryGetMetaObj" to get table from cache only.
     *
     * @param tblName
     * @return
     */
    public Optional<T> getTableForReplay(String tblName) {
        Preconditions.checkState(extCatalog.getUseMetaCache().isPresent(), extCatalog.getName() + "." + name);
        String localName = getLocalTableName(tblName, true);
        if (localName == null) {
            return Optional.empty();
        }
        if (extCatalog.getUseMetaCache().get()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("getTableForReplay from metacache, db: {}.{}, is db init: {}",
                        this.extCatalog.getName(), this.name, isInitialized());
            }
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.tryGetMetaObj(localName);
        } else if (tableNameToId.containsKey(localName)) {
            return Optional.ofNullable(idToTbl.get(tableNameToId.get(localName)));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void readLock() {
        this.rwLock.readLock().lock();
    }

    @Override
    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    @Override
    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    @Override
    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at external db[" + id + "]", e);
            return false;
        }
    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    @Override
    public boolean writeLockIfExist() {
        writeLock();
        return true;
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
    }

    @Override
    public void writeLockOrDdlException() throws DdlException {
        writeLock();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getFullName() {
        return name;
    }

    public String getRemoteName() {
        return Strings.isNullOrEmpty(remoteName) ? name : remoteName;
    }

    @Override
    public DatabaseProperty getDbProperties() {
        return dbProperties;
    }

    @Override
    public boolean isTableExist(String tableName) {
        String remoteTblName = tableName;
        if (this.isTableNamesCaseInsensitive()) {
            remoteTblName = lowerCaseToTableName.get(tableName.toLowerCase());
            if (remoteTblName == null) {
                // Here we need to execute listTableNames() once to fill in lowerCaseToTableName
                // to prevent lowerCaseToTableName from being empty in some cases
                listTableNames();
                remoteTblName = lowerCaseToTableName.get(tableName.toLowerCase());
                if (remoteTblName == null) {
                    return false;
                }
            }
        }
        return extCatalog.tableExist(ConnectContext.get().getSessionContext(), remoteName, remoteTblName);
    }

    // ATTN: this method only returned cached tables.
    @Override
    public List<T> getTables() {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            List<T> tables = Lists.newArrayList();
            Set<String> tblNames = getTableNamesWithLock();
            for (String tblName : tblNames) {
                T tbl = getTableNullable(tblName);
                if (tbl != null) {
                    tables.add(tbl);
                }
            }
            return tables;
        } else {
            return Lists.newArrayList(idToTbl.values());
        }
    }

    @Override
    public List<T> getViews() {
        throw new NotImplementedException("getViews() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        throw new NotImplementedException("getTablesOnIdOrderIfExist() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        throw new NotImplementedException("getTablesOnIdOrderOrThrowException() is not implemented");
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            return Sets.newHashSet(metaCache.listNames());
        } else {
            return Sets.newHashSet(tableNameToId.keySet());
        }
    }

    @Override
    public T getTableNullable(String tableName) {
        makeSureInitialized();
        String finalName = getLocalTableName(tableName, false);
        if (finalName == null) {
            return null;
        }
        if (extCatalog.getUseMetaCache().get()) {
            // must use full qualified name to generate id.
            // otherwise, if 2 databases have the same table name, the id will be the same.
            return metaCache.getMetaObj(finalName,
                    Util.genIdByName(extCatalog.getName(), name, finalName)).orElse(null);
        } else {
            if (!tableNameToId.containsKey(finalName)) {
                return null;
            }
            return idToTbl.get(tableNameToId.get(finalName));
        }
    }

    /**
     * Get the local table name based on the given table name.
     *
     * @param tableName
     * @param isReplay, if true, only check local cache, will not visit remote system
     * @return
     */
    @Nullable
    private String getLocalTableName(String tableName, boolean isReplay) {
        String finalName = tableName;
        if (this.isStoredTableNamesLowerCase()) {
            finalName = tableName.toLowerCase();
        }
        if (this.isTableNamesCaseInsensitive()) {
            finalName = lowerCaseToTableName.get(tableName.toLowerCase());
            if (finalName == null) {
                if (isReplay) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("failed to get final table name from: {}.{}.{}, is replay = true",
                                getCatalog().getName(), getFullName(), tableName);
                    }
                    return null;
                }
                // Here we need to execute listTableNames() once to fill in lowerCaseToTableName
                // to prevent lowerCaseToTableName from being empty in some cases
                listTableNames();
                finalName = lowerCaseToTableName.get(tableName.toLowerCase());
                if (finalName == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("failed to get final table name from: {}.{}.{}",
                                getCatalog().getName(), getFullName(), tableName);
                    }
                    return null;
                }
            }
        }
        if (extCatalog.getLowerCaseMetaNames().equalsIgnoreCase("true")
                && (this.isTableNamesCaseInsensitive())) {
            finalName = tableName.toLowerCase();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get table {} from database: {}.{}, final name is: {}, catalog id: {}",
                    tableName, getCatalog().getName(), getFullName(), finalName, getCatalog().getId());
        }
        return finalName;
    }

    @Override
    public T getNonTempTableNullable(String tableName) {
        throw new NotImplementedException("getNonTempTableNullable() is not implemented");
    }

    @Override
    public T getTableNullable(long tableId) {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            return metaCache.getMetaObjById(tableId).orElse(null);
        } else {
            return idToTbl.get(tableId);
        }
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        tableNameToId = Maps.newConcurrentMap();
        lowerCaseToTableName = Maps.newConcurrentMap();
        Map<Long, T> tmpIdToTbl = Maps.newConcurrentMap();
        for (Object obj : idToTbl.values()) {
            if (obj instanceof LinkedTreeMap) {
                String clazzType = ((LinkedTreeMap<?, ?>) obj).get("clazz").toString();
                switch (clazzType) {
                    case "ExternalInfoSchemaTable":
                        ExternalInfoSchemaTable infoSchemaTable = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(obj),
                                ExternalInfoSchemaTable.class);
                        if (infoSchemaTable.getDb() == null) {
                            infoSchemaTable.setDb(this);
                        }
                        tmpIdToTbl.put(infoSchemaTable.getId(), (T) infoSchemaTable);
                        tableNameToId.put(infoSchemaTable.getName(), infoSchemaTable.getId());
                        lowerCaseToTableName.put(infoSchemaTable.getName().toLowerCase(), infoSchemaTable.getName());
                        break;
                    case "ExternalMysqlTable":
                        ExternalMysqlTable mysqlTable = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(obj),
                                ExternalMysqlTable.class);
                        if (mysqlTable.getDb() == null) {
                            mysqlTable.setDb(this);
                        }
                        tmpIdToTbl.put(mysqlTable.getId(), (T) mysqlTable);
                        tableNameToId.put(mysqlTable.getName(), mysqlTable.getId());
                        lowerCaseToTableName.put(mysqlTable.getName().toLowerCase(), mysqlTable.getName());
                        break;
                    default:
                        break;
                }
            } else {
                Preconditions.checkState(obj instanceof ExternalTable);
                tmpIdToTbl.put(((ExternalTable) obj).getId(), (T) obj);
                tableNameToId.put(((ExternalTable) obj).getName(), ((ExternalTable) obj).getId());
                lowerCaseToTableName.put(((ExternalTable) obj).getName().toLowerCase(),
                        ((ExternalTable) obj).getName());
            }
        }
        // Check whether the remoteName and db Tbl db in tmpIdToTbl is empty
        for (T table : tmpIdToTbl.values()) {
            if (Strings.isNullOrEmpty(table.getRemoteName())
                    || table.getDb() == null) {
                initialized = false;
                break;
            }
        }
        idToTbl = tmpIdToTbl;
        rwLock = new MonitoredReentrantReadWriteLock(true);
    }

    @Override
    public void unregisterTable(String tableName) {
        makeSureInitialized();
        if (LOG.isDebugEnabled()) {
            LOG.debug("unregister table {}.{}", this.name, tableName);
        }
        setLastUpdateTime(System.currentTimeMillis());
        // check if the table exists in cache, it not, does return
        ExternalTable dorisTable = getTableForReplay(tableName).orElse(null);
        if (dorisTable == null) {
            return;
        }
        // clear the cache related to this table.
        if (extCatalog.getUseMetaCache().get()) {
            if (isInitialized()) {
                metaCache.invalidate(dorisTable.getName(),
                        Util.genIdByName(extCatalog.getName(), name, dorisTable.getName()));
                lowerCaseToTableName.remove(dorisTable.getName().toLowerCase());
            }
        } else {
            Long tableId = tableNameToId.remove(dorisTable.getName());
            if (tableId == null) {
                LOG.warn("table [{}] does not exist when drop", dorisTable.getName());
                return;
            }
            idToTbl.remove(tableId);
            lowerCaseToTableName.remove(dorisTable.getName().toLowerCase());
        }

        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(dorisTable);
    }

    @Override
    public CatalogIf getCatalog() {
        return extCatalog;
    }

    // Only used for sync hive metastore event
    @Override
    public boolean registerTable(TableIf tableIf) {
        makeSureInitialized();
        long tableId = tableIf.getId();
        String tableName = tableIf.getName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", tableName);
        }
        if (extCatalog.getUseMetaCache().get()) {
            if (isInitialized()) {
                String localName = extCatalog.fromRemoteTableName(this.remoteName, tableName);
                metaCache.updateCache(tableName, localName, (T) tableIf,
                        Util.genIdByName(extCatalog.getName(), name, localName));
                lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
            }
        } else {
            if (!tableNameToId.containsKey(tableName)) {
                tableNameToId.put(tableName, tableId);
                idToTbl.put(tableId,
                        buildTableForInit(tableName, extCatalog.fromRemoteTableName(this.remoteName, tableName),
                                tableId, extCatalog, this, false));
                lowerCaseToTableName.put(tableName.toLowerCase(), tableName);
            }
        }
        setLastUpdateTime(System.currentTimeMillis());
        return true;
    }

    private boolean isStoredTableNamesLowerCase() {
        // Because we have added a test configuration item,
        // it needs to be judged together with Env.isStoredTableNamesLowerCase()
        return Env.isStoredTableNamesLowerCase() || extCatalog.getOnlyTestLowerCaseTableNames() == 1;
    }

    private boolean isTableNamesCaseInsensitive() {
        // Because we have added a test configuration item,
        // it needs to be judged together with Env.isTableNamesCaseInsensitive()
        return Env.isTableNamesCaseInsensitive() || extCatalog.getOnlyTestLowerCaseTableNames() == 2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalDatabase)) {
            return false;
        }
        ExternalDatabase<?> that = (ExternalDatabase<?>) o;
        return Objects.equal(name, that.name) && Objects.equal(extCatalog,
                that.extCatalog);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, extCatalog);
    }

    @VisibleForTesting
    public void addTableForTest(T tbl) {
        // 1. add for "use_meta_cache = false"
        idToTbl.put(tbl.getId(), tbl);
        tableNameToId.put(tbl.getName(), tbl.getId());
        // 2. add for "use_meta_cache = true"
        buildMetaCache();
        metaCache.addObjForTest(tbl.getId(), tbl.getName(), tbl);
    }

    public void resetMetaCacheNames() {
        if (extCatalog.getUseMetaCache().isPresent() && extCatalog.getUseMetaCache().get() && metaCache != null) {
            metaCache.resetNames();
        } else {
            resetToUninitialized();
        }
    }
}
