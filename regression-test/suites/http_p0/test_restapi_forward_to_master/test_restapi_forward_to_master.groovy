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

import groovy.json.JsonSlurper

suite("test_restapi_forward_to_master", "p0") {
    def frontends = sql_return_maparray "show frontends"
    def feEndpoints = []
    for (def fe : frontends) {
        feEndpoints.add(fe.Host + ":" + fe.HttpPort)
    }
    log.info("All FE endpoints: ${feEndpoints}")

    def user = context.config.feHttpUser
    def pwd = context.config.feHttpPassword

    // Get disable_mini_load config value for PUT _load test branching
    def miniLoadConfigRows = sql_return_maparray "SHOW FRONTEND CONFIG LIKE 'disable_mini_load'"
    def disableMiniLoad = miniLoadConfigRows.size() > 0 ? miniLoadConfigRows[0].Value?.toString()?.toLowerCase() == "true" : true
    log.info("disable_mini_load config value: ${disableMiniLoad}")

    // ==================== Prepare test db and table ====================
    def dbName = "test_restapi_forward_db"
    def tableName = "test_restapi_forward_tbl"
    sql """ DROP DATABASE IF EXISTS ${dbName} """
    sql """ CREATE DATABASE ${dbName} """
    sql """ USE ${dbName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT NOT NULL,
            name VARCHAR(64) NOT NULL,
            age INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // ==================== Helper: streamLoad to specific FE endpoint ====================
    def doStreamLoad = { String feEndpoint, String label ->
        // feEndpoint format: "host:port", split on last ":" to handle IPv6
        def lastColonIdx = feEndpoint.lastIndexOf(":")
        def host = feEndpoint.substring(0, lastColonIdx)
        def port = feEndpoint.substring(lastColonIdx + 1).toInteger()
        streamLoad {
            setFeAddr host, port
            table "${tableName}"
            db "${dbName}"
            set 'label', "${label}"
            set 'column_separator', ','
            inputText "1,bob,20\n2,alice,30"
            time 30000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result on ${feEndpoint}: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }

    // ==================== Helper: GET with json code check ====================
    // checkCode: whether to check business code; expectedCode: the expected business code value
    // expectedMsg: when expectedCode != 0, verify the msg/data field contains this substring
    // For v2 APIs (ResponseEntityBuilder): response has "code" and "msg"/"data" fields
    // For v1 APIs (RestBaseResult): response has "status" and "msg" fields
    def httpGetCheck = { String feEp, String uriStr, boolean checkCode = true, int expectedCode = 0,
                         String expectedMsg = null ->
        httpTest {
            basicAuthorization "${user}", "${pwd}"
            endpoint "${feEp}"
            uri "${uriStr}"
            op "get"
            check { respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                if (checkCode) {
                    def data = new JsonSlurper().parseText("${body}".toString())
                    if (data.code != null) {
                        assertEquals(data.code as int, expectedCode)
                        if (expectedCode != 0 && expectedMsg != null) {
                            assertTrue(data.msg?.toString()?.contains(expectedMsg) ||
                                       data.data?.toString()?.contains(expectedMsg),
                                       "Expected msg/data to contain '${expectedMsg}', but got msg=${data.msg}, data=${data.data}")
                        }
                    } else if (data.status != null) {
                        if (expectedCode == 0) {
                            assertEquals(data.status, "OK")
                        } else {
                            assertEquals(data.status, "FAILED")
                            if (expectedMsg != null) {
                                assertTrue(data.msg?.toString()?.contains(expectedMsg),
                                           "Expected msg to contain '${expectedMsg}', but got msg=${data.msg}")
                            }
                        }
                    }
                }
            }
        }
    }

    // ==================== Helper: POST with json code check ====================
    def httpPostCheck = { String feEp, String uriStr, String reqBody = null, boolean checkCode = true,
                          int expectedCode = 0, String expectedMsg = null ->
        httpTest {
            basicAuthorization "${user}", "${pwd}"
            endpoint "${feEp}"
            uri "${uriStr}"
            op "post"
            if (reqBody != null) {
                body "${reqBody}"
            }
            check { respCode, body ->
                assertEquals("${respCode}".toString(), "200")
                if (checkCode) {
                    def data = new JsonSlurper().parseText("${body}".toString())
                    if (data.code != null) {
                        assertEquals(data.code as int, expectedCode)
                        if (expectedCode != 0 && expectedMsg != null) {
                            assertTrue(data.msg?.toString()?.contains(expectedMsg) ||
                                       data.data?.toString()?.contains(expectedMsg),
                                       "Expected msg/data to contain '${expectedMsg}', but got msg=${data.msg}, data=${data.data}")
                        }
                    } else if (data.status != null) {
                        if (expectedCode == 0) {
                            assertEquals(data.status, "OK")
                        } else {
                            assertEquals(data.status, "FAILED")
                            if (expectedMsg != null) {
                                assertTrue(data.msg?.toString()?.contains(expectedMsg),
                                           "Expected msg to contain '${expectedMsg}', but got msg=${data.msg}")
                            }
                        }
                    }
                }
            }
        }
    }

    // ==================== Helper: PUT with json code check ====================
    def httpPutCheck = { String feEp, String uriStr, String reqBody = null, boolean checkCode = true,
                         int expectedCode = 0, String expectedMsg = null ->
        httpTest {
            basicAuthorization "${user}", "${pwd}"
            endpoint "${feEp}"
            uri "${uriStr}"
            op "put"
            if (reqBody != null) {
                body "${reqBody}"
            }
            check { respCode, body ->
                log.info("PUT ${uriStr} on ${feEp} resp Code {}", "${respCode}".toString())
                log.info("PUT ${uriStr} on ${feEp} resp Body: ${body}".toString())
                assertEquals("${respCode}".toString(), "200")
                if (checkCode) {
                    def data = new JsonSlurper().parseText("${body}".toString())
                    log.info("PUT ${uriStr} parsed data: code=${data.code}, status=${data.status}, msg=${data.msg}, data.data=${data.data}, expectedCode=${expectedCode}".toString())
                    if (data.code != null) {
                        assertEquals(data.code as int, expectedCode)
                        if (expectedCode != 0 && expectedMsg != null) {
                            assertTrue(data.msg?.toString()?.contains(expectedMsg) ||
                                       data.data?.toString()?.contains(expectedMsg),
                                       "Expected msg/data to contain '${expectedMsg}', but got msg=${data.msg}, data=${data.data}")
                        }
                    } else if (data.status != null) {
                        if (expectedCode == 0) {
                            assertEquals(data.status, "OK")
                        } else {
                            assertEquals(data.status, "FAILED")
                            if (expectedMsg != null) {
                                assertTrue(data.msg?.toString()?.contains(expectedMsg),
                                           "Expected msg to contain '${expectedMsg}', but got msg=${data.msg}")
                            }
                        }
                    }
                }
            }
        }
    }

    try {
        for (def feEndpoint : feEndpoints) {
            log.info("Testing on FE: ${feEndpoint}")

            // ==================== StreamLoad (forward to master) ====================
            def testLabel = "test_restapi_forward_label_" + UUID.randomUUID().toString().replace("-", "_")
            doStreamLoad(feEndpoint, testLabel)

            // ==================== NodeAction APIs (fetchNodeInfo - commit changed) ====================
            httpGetCheck(feEndpoint, "/rest/v2/manager/node/backends")
            httpGetCheck(feEndpoint, "/rest/v2/manager/node/frontends")
            httpGetCheck(feEndpoint, "/rest/v2/manager/node/brokers")

            // ==================== NodeAction APIs (operateBackend/Frontends/Broker - commit changed) ====================
            // These POST endpoints have forward changes; use invalid action to avoid real side effects
            // operateBackend/operateFrontends with INVALID_ACTION: no branch matches, returns code=0
            // operateBroker with INVALID_ACTION: throws Exception, returns code=1, data="Unsupported broker operation type"
            httpPostCheck(feEndpoint, "/rest/v2/manager/node/INVALID_ACTION/be", '{"hostPorts":["0.0.0.0:9050"]}')
            httpPostCheck(feEndpoint, "/rest/v2/manager/node/INVALID_ACTION/fe", '{"hostPort":"0.0.0.0:9030","role":"FOLLOWER"}')
            httpPostCheck(feEndpoint, "/rest/v2/manager/node/INVALID_ACTION/broker", '{"brokerName":"nonexistent_broker","hostPortList":["0.0.0.0:8000"]}', true, 1, "Unsupported broker operation type")

            // ==================== ShowAction APIs (show_proc - commit changed) ====================
            httpGetCheck(feEndpoint, "/api/show_proc?path=/frontends&forward=true")

            // ==================== ColocateMetaService APIs (commit changed) ====================
            httpGetCheck(feEndpoint, "/api/colocate")
            // group_stable: forward is before checkAndGetGroupId, so forward succeeds on follower
            // On master, invalid group_id causes DdlException(extends UserException) which is caught
            // by RestApiExceptionHandler and returns code=1 (COMMON_ERROR) with error msg
            httpPostCheck(feEndpoint, "/api/colocate/group_stable?group_id=0&db_id=0", null, true, 1, "isn't  exist")

            // ==================== StatisticAction APIs ====================
            httpGetCheck(feEndpoint, "/rest/v2/api/cluster_overview")

            // ==================== GetLoadInfoAction APIs ====================
            // _load_info uses v1 RestBaseResult format (status field, no code field)
            // Stream load jobs may not be queryable via _load_info (which targets mini load),
            // so only verify forward works (HTTP 200 returned), not business status
            httpGetCheck(feEndpoint, "/api/${dbName}/_load_info?label=${testLabel}", false)

            // ==================== GetStreamLoadState APIs ====================
            httpGetCheck(feEndpoint, "/api/${dbName}/get_load_state?label=${testLabel}")

            // ==================== CancelLoadAction APIs ====================
            // v2 API (ResponseEntityBuilder): forward to master succeeds, business returns code=1, msg="does not exist"
            httpPostCheck(feEndpoint, "/api/${dbName}/_cancel?label=nonexistent_test_label_for_forward", null, true, 1, "does not exist")

            // ==================== LoadAction multi mini load (commit changed) ====================
            // PUT /api/{db}/{table}/_load with sub_label triggers forward to master
            if (disableMiniLoad) {
                // When disable_mini_load=true (default), returns code=404 before reaching label logic
                httpPutCheck(feEndpoint, "/api/${dbName}/${tableName}/_load?label=nonexistent_test_label_for_forward&sub_label=nonexistent_sub_label", null, true, 404, "disabled")
            } else {
                // When disable_mini_load=false, forward succeeds and returns V1 RestBaseResult with label error
                httpPutCheck(feEndpoint, "/api/${dbName}/${tableName}/_load?label=nonexistent_test_label_for_forward&sub_label=nonexistent_sub_label", null, true, 1, "label")
            }

            // ==================== MultiAction APIs (all 6 methods - commit changed) ====================
            // All forward checks are now BEFORE label check, so no label needed to trigger forward
            // v1 API (RestBaseResult): response is {"status":"OK/FAILED", "msg":"..."}, no "code" field
            // expectedCode=1 (non-zero) triggers status=="FAILED" check in helper; msg verified
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_list")
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_desc", null, true, 1, "No label selected")
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_start", null, true, 1, "No label selected")
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_unload", null, true, 1, "No label selected")
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_commit", null, true, 1, "No label selected")
            httpPostCheck(feEndpoint, "/api/${dbName}/_multi_abort", null, true, 1, "No label selected")

            // ==================== AddStoragePolicyAction APIs ====================
            def policyName = "test_policy_" + UUID.randomUUID().toString().replace("-", "").take(24)
            def resourceName = "test_res_" + UUID.randomUUID().toString().replace("-", "").take(24)
            sql """ CREATE RESOURCE IF NOT EXISTS "${resourceName}"
                    PROPERTIES("type"="hdfs",
                    "hadoop.security.authentication"="kerberos",
                    "hadoop.kerberos.principal"="hdfs/test@poc",
                    "hadoop.kerberos.keytab"="/tmp/test.keytab",
                    "fs.defaultFS"="hdfs://hdfstest",
                    "dfs.nameservices"="test",
                    "dfs.ha.namenodes.test"="nn1,nn2",
                    "dfs.namenode.rpc-address.test.nn1"="host1:54310",
                    "dfs.namenode.rpc-address.test.nn2"="host2:54310",
                    "dfs.client.failover.proxy.provider.test"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider") """
            def policyBody = """{"policyName":"${policyName}","policyId":0,"storageResource":"${resourceName}","cooldownTimestampMs":0,"cooldownTtl":86400}"""
            httpPostCheck(feEndpoint, "/rest/v2/api/storage_policy", policyBody, true, 1, "cooldown_datetime and cooldown_ttl can't be set together")
            sql """ DROP STORAGE POLICY IF EXISTS ${policyName} """
            sql """ DROP RESOURCE IF EXISTS "${resourceName}" """

            // ==================== ESCatalogAction APIs ====================
            // Non-existent catalog: forward to master succeeds (forward is before catalog validation),
            // business returns code=403, data="unknown ES Catalog: nonexistent_catalog"
            httpGetCheck(feEndpoint, "/rest/v2/api/es_catalog/get_mapping?catalog=nonexistent_catalog&table=nonexistent_table", true, 403, "unknown ES Catalog")
            httpPostCheck(feEndpoint, "/rest/v2/api/es_catalog/search?catalog=nonexistent_catalog&table=nonexistent_table", '{"query":{"match_all":{}}}', true, 403, "unknown ES Catalog")
        }
    } finally {
        sql """ DROP DATABASE IF EXISTS ${dbName} """
    }
}
