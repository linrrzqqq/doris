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

package org.apache.doris.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class UdfCacheCleanupDaemonTest {
    private static final long BACKEND_ID = 980001;

    @BeforeEach
    void setUp() {
        AgentTaskQueue.clearAllTasks();
        Assertions.assertNull(Env.getCurrentSystemInfo().getBackend(BACKEND_ID));
    }

    @AfterEach
    void tearDown() {
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    void removesAllTasksForMissingBackend() {
        AgentTask cleanup = new CleanUDFCacheTask(BACKEND_ID, "", 1);
        AgentTask migration = new StorageMediaMigrationTask(BACKEND_ID, 3, 4, TStorageMedium.SSD);
        AgentTaskQueue.addTask(cleanup);
        AgentTaskQueue.addTask(migration);

        new AgentTaskCleanupDaemon().runAfterCatalogReady();

        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
        Assertions.assertFalse(AgentTaskQueue.getBackendIds().contains(BACKEND_ID));
        Assertions.assertEquals(TStatusCode.ABORTED, cleanup.getErrorCode());
        Assertions.assertEquals(TStatusCode.ABORTED, migration.getErrorCode());
    }

    @Test
    void backendCanRetryCleanupAfterSixMinuteOutage() {
        Backend backend = new Backend(BACKEND_ID, "127.0.0.1", 19050);
        backend.setAlive(false);
        Env.getCurrentSystemInfo().addBackend(backend);
        try {
            CleanUDFCacheTask task = new CleanUDFCacheTask(BACKEND_ID, "", 1);
            task.createTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(6);
            AgentTaskQueue.addTask(task);
            AgentTaskCleanupDaemon daemon = new AgentTaskCleanupDaemon();
            daemon.runAfterCatalogReady();
            backend.setAlive(true);
            daemon.runAfterCatalogReady();
            Assertions.assertEquals(List.of(task), AgentTaskQueue.getDiffTasks(BACKEND_ID, Map.of()));
        } finally {
            Env.getCurrentSystemInfo().replayDropBackend(backend);
        }
    }
}
