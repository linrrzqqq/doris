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
import org.apache.doris.thrift.TCleanUDFCacheReq;
import org.apache.doris.thrift.TTaskType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CleanUDFCacheTask extends AgentTask {
    private String functionSignature;
    private long functionId;

    public CleanUDFCacheTask(long backendId, String signature, long functionId) {
        super(null, backendId, TTaskType.CLEAN_UDF_CACHE, -1, -1, -1, -1, -1,
                functionId, System.currentTimeMillis());
        this.functionSignature = signature;
        this.functionId = functionId;
    }

    public static void submitCleanup(Map<Long, String> functionSignatures) {
        if (functionSignatures.isEmpty()) {
            return;
        }
        List<CleanUDFCacheTask> tasks = new ArrayList<>();
        for (long backendId : Env.getCurrentSystemInfo().getAllBackendIds(false)) {
            for (Map.Entry<Long, String> function : functionSignatures.entrySet()) {
                tasks.add(new CleanUDFCacheTask(backendId, function.getValue(), function.getKey()));
            }
        }
        submitTasks(tasks);
    }

    public static void submitTasks(List<CleanUDFCacheTask> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (CleanUDFCacheTask task : tasks) {
            if (AgentTaskQueue.addTask(task)) {
                batchTask.addTask(task);
            }
        }
        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    public TCleanUDFCacheReq toThrift() {
        TCleanUDFCacheReq req = new TCleanUDFCacheReq();
        req.setFunctionSignature(this.functionSignature);
        req.setFunctionId(this.functionId);
        return req;
    }
}
