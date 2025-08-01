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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper

suite('test_sql_mode_node_mgr', 'multi_cluster,docker,p1') {
    if (!isCloudMode()) {
        return;
    }

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
        new ClusterOptions(),
        new ClusterOptions(),
        new ClusterOptions(),
        new ClusterOptions(),
    ]

    for (options in clusterOptions) {
        options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'cloud_tablet_rebalancer_interval_second=1',
            'cloud_pre_heating_time_limit_sec=5'
        ]
        options.cloudMode = true
        options.sqlModeNodeMgr = true
        options.connectToFollower = true
        options.waitTimeout = 0
        options.feNum = 3
        options.useFollowersMode = true
        options.feConfigs += ["resource_not_ready_sleep_seconds=1",
                "heartbeat_interval_second=1",]
    }

    // Private deployment
    // fe cluster id（docker compose 生成的），be cluster id 为空，ms endpoint 是配置的
    clusterOptions[0].sqlModeNodeMgr = true;
    clusterOptions[0].beClusterId = false;
    clusterOptions[0].beMetaServiceEndpoint = true;

    // fe cluster id（docker compose 生成的），be cluster id 为空，ms endpoint 下发的
    clusterOptions[1].sqlModeNodeMgr = true;
    clusterOptions[1].beClusterId = false;
    clusterOptions[1].beMetaServiceEndpoint = false;

    // fe cluster id（docker compose 生成的），be cluster id （docker compose 生成的），ms endpoint 下发的
    clusterOptions[2].sqlModeNodeMgr = true;
    clusterOptions[2].beClusterId = true;
    clusterOptions[2].beMetaServiceEndpoint = false;

    // fe cluster id（docker compose 生成的），be cluster id （docker compose 生成的），ms endpoint 是配置的
    clusterOptions[3].sqlModeNodeMgr = true;
    clusterOptions[3].beClusterId = true;
    clusterOptions[3].beMetaServiceEndpoint = true;

    // saas
    // fe cluster id（随机生成），be cluster id 是空，ms endpoint 是配置的
    clusterOptions[4].sqlModeNodeMgr = false;
    clusterOptions[4].beClusterId = false;
    clusterOptions[4].beMetaServiceEndpoint = true;
    
    // fe cluster id（随机生成）， be cluster id 是空，ms endpoint 用的是fe 下发的
    clusterOptions[5].sqlModeNodeMgr = false;
    clusterOptions[5].beClusterId = false;
    clusterOptions[5].beMetaServiceEndpoint = false;

    /*
    fe cluster id（随机生成） 不等于 be cluster id（docker compose 配置1234567）
    clusterOptions[].sqlModeNodeMgr = false;
    clusterOptions[].beClusterId = true;
    clusterOptions[].beMetaServiceEndpoint = true;

    fe cluster id（随机生成） 不等于 be cluster id（docker compose 配置1234567）
    clusterOptions[].sqlModeNodeMgr = false;
    clusterOptions[].beClusterId = true;
    clusterOptions[].beMetaServiceEndpoint = false;
    */

    def inject_to_ms_api = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=set&name=${key}&behavior=change_args&value=${value}"
            check check_func
        }
    }

    def clear_ms_inject_api = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=clear"
            check check_func
        }
    }

    def enable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check check_func
        }
    }

    for (options in clusterOptions) {
        docker(options) {
            logger.info("docker started");
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            // inject point, to change MetaServiceImpl_get_cluster_set_config
            inject_to_ms_api.call(msHttpPort, "resource_manager::set_safe_drop_time", URLEncoder.encode('[-1]', "UTF-8")) {
            respCode, body ->
                log.info("inject resource_manager::set_safe_drop_time resp: ${body} ${respCode}".toString()) 
            }

            enable_ms_inject_api.call(msHttpPort) {
                respCode, body ->
                log.info("enable inject resp: ${body} ${respCode}".toString()) 
            }

            def checkFrontendsAndBackends = {
                // Check frontends
                def frontendResult = sql_return_maparray """show frontends;"""
                logger.info("show frontends result {}", frontendResult)
                // Check that we have the expected number of frontends
                assert frontendResult.size() == 3, "Expected 3 frontends, but got ${frontendResult.size()}"

                // Check that all required columns are present
                def requiredColumns = ['Name', 'Host', 'EditLogPort', 'HttpPort', 'QueryPort', 'RpcPort', 'Role', 'IsMaster', 'ClusterId', 'Join', 'Alive', 'ReplayedJournalId', 'LastHeartbeat', 'IsHelper', 'ErrMsg']
                def actualColumns = frontendResult[0].keySet()
                assert actualColumns.containsAll(requiredColumns), "Missing required columns. Expected: ${requiredColumns}, Actual: ${actualColumns}"

                // Check that we have one master and two followers
                def masterCount = frontendResult.count { it['IsMaster'] == 'true' }
                assert masterCount == 1, "Expected 1 master, but got ${masterCount}"

                def followerCount = frontendResult.count { it['IsMaster'] == 'false' }
                assert followerCount == 2, "Expected 2 followers, but got ${followerCount}"

                // Check that all frontends are alive
                def aliveCount = frontendResult.count { it['Alive'] == 'true' }
                assert aliveCount == 3, "Expected all 3 frontends to be alive, but only ${aliveCount} are alive"

                // Check backends
                def backendResult = sql_return_maparray """show backends;"""
                logger.info("show backends result {}", backendResult)
                // Check that we have the expected number of backends
                assert backendResult.size() == 3, "Expected 3 backends, but got ${backendResult.size()}"

                // Check that all required columns are present
                def requiredBackendColumns = ['Host', 'HeartbeatPort', 'BePort', 'HttpPort', 'BrpcPort', 'LastStartTime', 'LastHeartbeat', 'Alive', 'SystemDecommissioned', 'TabletNum', 'DataUsedCapacity', 'AvailCapacity', 'TotalCapacity', 'UsedPct', 'MaxDiskUsedPct', 'Tag', 'ErrMsg', 'Version', 'Status']
                def actualBackendColumns = backendResult[0].keySet()
                assert actualBackendColumns.containsAll(requiredBackendColumns), "Missing required backend columns. Expected: ${requiredBackendColumns}, Actual: ${actualBackendColumns}"

                // Check that all backends are alive
                def aliveBackendCount = backendResult.count { it['Alive'] == 'true' }
                assert aliveBackendCount == 3, "Expected all 3 backends to be alive, but only ${aliveBackendCount} are alive"

                // Check that no backends are decommissioned
                def decommissionedCount = backendResult.count { it['SystemDecommissioned'] == 'true' || it['ClusterDecommissioned'] == 'true' }
                assert decommissionedCount == 0, "Expected no decommissioned backends, but found ${decommissionedCount}"

                // Check that all backends have valid capacities
                backendResult.each { backend ->
                    assert backend['DataUsedCapacity'] != null && backend['AvailCapacity'] != null && backend['TotalCapacity'] != null, "Backend ${backend['BackendId']} has invalid capacity values"
                    assert backend['UsedPct'] != null && backend['MaxDiskUsedPct'] != null, "Backend ${backend['BackendId']} has invalid disk usage percentages"
                }

                logger.info("All backend checks passed successfully")
            }

            // Call the function to check frontends and backends
            checkFrontendsAndBackends()

            def checkClusterStatus = { int expectedFeNum, int expectedBeNum, int existsRows ->
                logger.info("Checking cluster status...")
                
                // Check FE number
                def frontendResult = sql_return_maparray """SHOW FRONTENDS;"""
                // Check that all frontends are alive
                def aliveCount = frontendResult.count { it['Alive'] == 'true' }
                assert aliveCount == expectedFeNum, "Expected all $expectedFeNum frontends to be alive, but only ${aliveCount} are alive"
                assert frontendResult.size() == expectedFeNum, "Expected ${expectedFeNum} frontends, but got ${frontendResult.size()}"
                logger.info("FE number check passed: ${frontendResult.size()} FEs found")

                // Check BE number
                def backendResult = sql_return_maparray """SHOW BACKENDS;"""
                assert backendResult.size() == expectedBeNum, "Expected ${expectedBeNum} backends, but got ${backendResult.size()}"
                logger.info("BE number check passed: ${backendResult.size()} BEs found")

                // Create table if not exists
                sql """
                CREATE TABLE IF NOT EXISTS test_table (
                    id INT,
                    name VARCHAR(50)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES("replication_num" = "3");
                """

                logger.info("Table 'test_table' created or already exists")
                sql """ INSERT INTO test_table VALUES (1, 'test1') """

                def result = sql """ SELECT * FROM test_table ORDER BY id """
                assert result.size() == existsRows + 1, "Expected ${existsRows + 1} rows, but got ${result.size()}"
                logger.info("Read/write check passed: ${result.size()} rows found")

                logger.info("All cluster status checks passed successfully")
            }

            // Call the function to check cluster status
            checkClusterStatus(3, 3, 0)

            // CASE 1 . check restarting fe and be work.
            logger.info("Restarting frontends and backends...")
            cluster.restartFrontends();
            cluster.restartBackends();

            def reconnectFe = {
                sleep(10000)
                logger.info("Reconnecting to a new frontend...")
                def newFe = cluster.getMasterFe()
                if (newFe) {
                    logger.info("New frontend found: ${newFe.host}:${newFe.httpPort}")
                    def url = String.format(
                            "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                            newFe.host, newFe.queryPort)
                    url = context.config.buildUrlWithDb(url, context.dbName)
                    context.connectTo(url, context.config.jdbcUser, context.config.jdbcPassword)
                    logger.info("Successfully reconnected to the new frontend")
                } else {
                    logger.error("No new frontend found to reconnect")
                }
            }

            reconnectFe()

            checkClusterStatus(3, 3, 1)

            // CASE 2. If a be is dropped, query and writing also work.
            // Get the list of backends
            def backends = sql_return_maparray("SHOW BACKENDS")
            logger.info("Current backends: {}", backends)

            // Find a backend to drop
            def backendToDrop = backends[0]
            def backendHost = backendToDrop['Host']
            def backendHeartbeatPort = backendToDrop['HeartbeatPort']

            logger.info("Dropping backend: {}:{}", backendHost, backendHeartbeatPort)

            // Drop the selected backend
            sql """ ALTER SYSTEM DROPP BACKEND "${backendHost}:${backendHeartbeatPort}"; """

            // Wait for the backend to be fully dropped
            def maxWaitSeconds = 300
            def waited = 0
            while (waited < maxWaitSeconds) {
                def currentBackends = sql_return_maparray("SHOW BACKENDS")
                if (currentBackends.size() == 2) {
                    logger.info("Backend successfully dropped")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for backend to be dropped")
            }

            checkClusterStatus(3, 2, 2)

            // CASE 3. Add the dropped backend back
            logger.info("Adding back the dropped backend: {}:{}", backendHost, backendHeartbeatPort)
            sql """ ALTER SYSTEM ADD BACKEND "${backendHost}:${backendHeartbeatPort}" PROPERTIES ("tag.compute_group_name" = "another_compute_group"); """

            // Wait for the backend to be fully added back
            maxWaitSeconds = 300
            waited = 0
            while (waited < maxWaitSeconds) {
                def currentBackends = sql_return_maparray("SHOW BACKENDS")
                if (currentBackends.size() == 3) {
                    logger.info("Backend successfully added back")
                    break
                }
                sleep(10000)
                waited += 10
            }

            if (waited >= maxWaitSeconds) {
                throw new Exception("Timeout waiting for backend to be added back")
            }

            checkClusterStatus(3, 3, 3)

            // CASE 4. Check compute groups
            logger.info("Checking compute groups")

            def computeGroups = sql_return_maparray("SHOW COMPUTE GROUPS")
            logger.info("Compute groups: {}", computeGroups)

            // Verify that we have at least two compute groups
            assert computeGroups.size() >= 2, "Expected at least 2 compute groups, but got ${computeGroups.size()}"

            // Verify that we have a 'default_compute_group' and 'another_compute_group'
            def defaultGroup = computeGroups.find { it['IsCurrent'] == "TRUE" }
            def anotherGroup = computeGroups.find { it['IsCurrent'] == "FALSE" }

            assert defaultGroup != null, "Expected to find 'default_compute_group'"
            assert anotherGroup != null, "Expected to find 'another_compute_group'"

            // Verify that 'another_compute_group' has exactly one backend
            assert anotherGroup['BackendNum'] == '1', "Expected 'another_compute_group' to have 1 backend, but it has ${anotherGroup['BackendNum']}"

            // Verify that 'default_compute_group' has the remaining backends
            assert defaultGroup['BackendNum'] == '2', "Expected 'default_compute_group' to have 2 backends, but it has ${defaultGroup['BackendNum']}"

            logger.info("Compute groups verified successfully")

            // CASE 4. If a fe is dropped, query and writing also work.
            // Get the list of frontends
            def frontends = sql_return_maparray("SHOW FRONTENDS")
            logger.info("Current frontends: {}", frontends)

            // Find a non-master frontend to drop
            def feToDropMap = frontends.find { it['IsMaster'] == "false" }
            assert feToDropMap != null, "No non-master frontend found to drop"

            def feHost = feToDropMap['Host']
            def feEditLogPort = feToDropMap['EditLogPort']
            def feRole = feToDropMap['Role']

            def dropFeInx = cluster.getFrontends().find { it.host == feHost }.index 
            logger.info("Dropping non-master frontend: {}:{}, index: {}", feHost, feEditLogPort, dropFeInx)

            // Drop the selected non-master frontend
            sql """ ALTER SYSTEM DROP ${feRole} "${feHost}:${feEditLogPort}"; """
            // After drop feHost container will exit
            sleep(3 * 1000)
            cluster.dropFrontends(true, dropFeInx)
            logger.info("Dropping frontend index: {}, remove it from docker compose", dropFeInx)
            // Wait for the frontend to be fully dropped

            awaitUntil(300) {
                reconnectFe()
                def currentFrontends = sql_return_maparray("SHOW FRONTENDS")
                currentFrontends.size() == frontends.size() - 1
            }


            checkClusterStatus(2, 3, 4)

            // CASE 5. Add dropped frontend back
            logger.info("Adding dropped frontend back")

            // Get the current list of frontends
            def currentFrontends = sql_return_maparray("SHOW FRONTENDS")
            
            // Find the dropped frontend by comparing with the original list
            def droppedFE = frontends.find { fe ->
                !currentFrontends.any { it['Host'] == fe['Host'] && it['EditLogPort'] == fe['EditLogPort'] }
            }
            
            assert droppedFE != null, "Could not find the dropped frontend"
            
            // Up a new follower fe and add to docker compose
            // ATTN: in addFrontend, sql node mode, will execute `ALTER SYSTEM ADD FOLLOWER "${feHost}:${feEditLogPort}";`
            boolean fuzzyUpFollower = (getRandomBoolean() == "true") ? true : false
            logger.info("Want up a new role [{}] frontend", fuzzyUpFollower ? "FOLLOWER" : "OBSERVER")
            def addList = cluster.addFrontend(1, fuzzyUpFollower)
            logger.info("Up a new frontend, addList: {}", addList)

            def addFE = cluster.getFeByIndex(addList[0])
            feHost = addFE['Host']
            feEditLogPort = addFE['EditLogPort']
            def showFes = sql """SHOW FRONTENDS"""
            logger.info("Adding back frontend: {}", showFes)

            // Wait for the frontend to be fully added back
            awaitUntil(300, 5) {
                def updatedFrontends = sql_return_maparray("SHOW FRONTENDS")
                updatedFrontends.size() == frontends.size()
            }
           
            checkClusterStatus(3, 3, 5)

            logger.info("Frontend successfully added back and cluster status verified")

            // CASE 6. Drop frontend and add back again
            logger.info("Dropping frontend and adding back again")
            // Get the frontend to be dropped
            currentFrontends = sql_return_maparray("SHOW FRONTENDS")

            int obServerCount = currentFrontends.count { it['Role'] == 'OBSERVER' } 
            int followerServerCount = currentFrontends.count { it['Role'] == 'FOLLOWER' } 
            String fuzzyDropRole
            if (obServerCount != 0) {
                fuzzyDropRole = (getRandomBoolean() == "true" && followerServerCount != 1) ? "FOLLOWER" : "OBSERVER"
            } else {
                fuzzyDropRole = "FOLLOWER"
            }

            def frontendToDrop = currentFrontends.find {it['IsMaster'] == "false" && it['Role'] == fuzzyDropRole}
            logger.info("Find drop again frontend: {}, drop role [{}]", frontendToDrop, fuzzyDropRole)
            assert frontendToDrop != null, "Could not find the frontend to drop"

            def role = frontendToDrop.Role
            // Drop the frontend
            dropFeInx = cluster.getFrontends().find { it.host == frontendToDrop.Host }.index 
            sql """ ALTER SYSTEM DROP $role "${frontendToDrop.Host}:${frontendToDrop.EditLogPort}"; """
            // After drop frontendToDrop.Host container will exit
            sleep(3 * 1000)
            cluster.dropFrontends(true, dropFeInx)
            logger.info("Dropping again frontend index: {}, remove it from docker compose", dropFeInx)
            sleep(3 * 1000)
            reconnectFe()

            // Wait for the frontend to be fully dropped
            awaitUntil(300, 5) {
                def updatedFrontends = sql_return_maparray("SHOW FRONTENDS")
                !updatedFrontends.any { it['Host'] == frontendToDrop.Host && it['EditLogPort'] == frontendToDrop.EditLogPort }
            }

            // Up a new follower fe and add to docker compose
            // ATTN: in addFrontend, sql node mode, will execute `ALTER SYSTEM ADD FOLLOWER "${feHost}:${feEditLogPort}";`
            addList = cluster.addFrontend(1, true)
            logger.info("Up a new frontend, addList: {}", addList)

            awaitUntil(300, 5) {
                def updatedFrontends = sql_return_maparray("SHOW FRONTENDS")
                updatedFrontends.size() == 3
            }
            // Verify cluster status after adding the frontend back
            checkClusterStatus(3, 3, 6)

            logger.info("Frontend successfully added back and cluster status verified")
            // CASE 6. If fe can not drop itself.
            // 6. Attempt to drop the master FE and expect an exception
            logger.info("Attempting to drop the master frontend")

            // Get the master frontend information
            def masterFE = frontends.find { it['IsMaster'] == "true" }
            assert masterFE != null, "No master frontend found"

            def masterHost = masterFE['Host']
            def masterEditLogPort = masterFE['EditLogPort']

            logger.info("Attempting to drop master frontend: {}:{}", masterHost, masterEditLogPort)

            try {
                sql """ ALTER SYSTEM DROP FOLLOWER "${masterHost}:${masterEditLogPort}"; """
                throw new Exception("Expected an exception when trying to drop master frontend, but no exception was thrown")
            } catch (Exception e) {
                logger.info("Received expected exception when trying to drop master frontend: {}", e.getMessage())
                assert e.getMessage().contains("can not drop current master node."), "Unexpected exception message when trying to drop master frontend"
            }

            // Verify that the master frontend is still present
            currentFrontends = sql_return_maparray("SHOW FRONTENDS")
            assert currentFrontends.find { it['IsMaster'] == "true" && it['Host'] == masterHost && it['EditLogPort'] == masterEditLogPort } != null, "Master frontend should still be present"
            logger.info("Successfully verified that the master frontend cannot be dropped")


            // CASE 7. Attempt to drop a non-existent backend
            logger.info("Attempting to drop a non-existent backend")

            // Generate a non-existent host and port
            def nonExistentHost = "non.existent.host"
            def nonExistentPort = 12345

            try {
                sql """ ALTER SYSTEM DROPP BACKEND "${nonExistentHost}:${nonExistentPort}"; """
                throw new Exception("Expected an exception when trying to drop non-existent backend, but no exception was thrown")
            } catch (Exception e) {
                logger.info("Received expected exception when trying to drop non-existent backend: {}", e.getMessage())
                assert e.getMessage().contains("backend does not exists"), "Unexpected exception message when trying to drop non-existent backend"
            }

            // Verify that the number of backends remains unchanged
            def currentBackends = sql_return_maparray("SHOW BACKENDS")
            def originalBackendCount = 3 // As per the initial setup in this test
            assert currentBackends.size() == originalBackendCount, "Number of backends should remain unchanged after attempting to drop a non-existent backend"

            checkClusterStatus(3, 3, 7)

            // CASE 8. Decommission a backend and verify the process
            logger.info("Attempting to decommission a backend")

            // Get the list of current backends
            backends = sql_return_maparray("SHOW BACKENDS")
            assert backends.size() >= 1, "Not enough backends to perform decommission test"

            // Select a backend to decommission (not the first one, as it might be the master)
            def backendToDecommission = backends[1]
            def decommissionHost = backendToDecommission['Host']
            def decommissionPort = backendToDecommission['HeartbeatPort']

            logger.info("Decommissioning backend: {}:{}", decommissionHost, decommissionPort)

            // Decommission the selected backend
            sql """ ALTER SYSTEM DECOMMISSION BACKEND "${decommissionHost}:${decommissionPort}"; """

            // Wait for the decommission process to complete (this may take some time in a real environment)
            int maxAttempts = 60
            int attempts = 0
            boolean decommissionComplete = false

            while (attempts < maxAttempts && !decommissionComplete) {
                currentBackends = sql_return_maparray("SHOW BACKENDS")
                def decommissionedBackend = currentBackends.find { it['Host'] == decommissionHost && it['HeartbeatPort'] == decommissionPort }

                logger.info("decomissionedBackend {}", decommissionedBackend)
                // TODO: decomisssion is alive?
                if (decommissionedBackend && decommissionedBackend['Alive'] == "true" && decommissionedBackend['SystemDecommissioned'] == "true") {
                    decommissionComplete = true
                } else {
                    attempts++
                    sleep(1000) // Wait for 1 second before checking again
                }
            }

            assert decommissionComplete, "Backend decommission did not complete within the expected time"

            // Verify that the decommissioned backend is no longer active
            def finalBackends = sql_return_maparray("SHOW BACKENDS")
            def decommissionedBackend = finalBackends.find { it['Host'] == decommissionHost && it['HeartbeatPort'] == decommissionPort }
            assert decommissionedBackend['Alive'] == "true", "Decommissioned backend should not be alive"
            assert decommissionedBackend['SystemDecommissioned'] == "true", "Decommissioned backend should have SystemDecommissioned set to true"

            logger.info("Successfully decommissioned backend and verified its status")

            checkClusterStatus(3, 3, 8)

        }
    }

}
