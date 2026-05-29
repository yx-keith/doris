// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version  2.0 (the
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

suite("test_forward_to_master_with_request_body", "p0,auth") {

    def jsonSlurper = new JsonSlurper()

    def get_follower_fe = {
        def frontends = sql_return_maparray "show frontends"
        for (def fe : frontends) {
            if (fe.Role == "FOLLOWER" && fe.IsMaster == "false") {
                return fe
            }
        }
        return null
    }

    def follower_fe = get_follower_fe()
    if (follower_fe == null) {
        logger.info("No follower FE found, skip test")
        return
    }

    def follower_endpoint = "${follower_fe.Host}:${follower_fe.HttpPort}"
    logger.info("Follower FE: ${follower_endpoint}")

    // Test 1: PUT /api/_set_vars with request body (List<Variable>)
    // This API calls forwardToMaster(request, vars) with non-null body
    httpTest {
        endpoint follower_endpoint
        uri "/api/_set_vars"
        op "put"
        basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
        body '[{"name":"wait_timeout","value":"1000"}]'
        check { respCode, body ->
            assertEquals(200, respCode)
            def json = jsonSlurper.parseText(body)
            assertEquals(0, json.code, "Expected code=0 but got: ${body}")
        }
    }

    // Verify the variable was actually set to the expected value
    def var_result = sql_return_maparray "show global variables like 'wait_timeout'"
    assertEquals("1000", var_result[0].Value, "wait_timeout should be 1000 after _set_vars")

    // Test 2: PUT /api/_set_vars with larger request body (multiple variables)
    // to ensure Content-Length recalculation works with different body sizes
    httpTest {
        endpoint follower_endpoint
        uri "/api/_set_vars"
        op "put"
        basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
        body '[{"name":"wait_timeout","value":"2000"},{"name":"sql_mode","value":"ONLY_FULL_GROUP_BY"}]'
        check { respCode, body ->
            assertEquals(200, respCode)
            def json = jsonSlurper.parseText(body)
            assertEquals(0, json.code, "Expected code=0 but got: ${body}")
        }
    }

    // Verify both variables were set correctly
    def var_result2 = sql_return_maparray "show global variables like 'wait_timeout'"
    assertEquals("2000", var_result2[0].Value, "wait_timeout should be 2000 after _set_vars")
    def var_result3 = sql_return_maparray "show global variables like 'sql_mode'"
    assertTrue(var_result3[0].Value.contains("ONLY_FULL_GROUP_BY"),
        "sql_mode should contain ONLY_FULL_GROUP_BY after _set_vars")

    // Test 3: PUT /api/_unset_vars with request body (List<String>)
    // This API calls forwardToMaster(request, varNames) with non-null body
    httpTest {
        endpoint follower_endpoint
        uri "/api/_unset_vars"
        op "put"
        basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
        body '["wait_timeout"]'
        check { respCode, body ->
            assertEquals(200, respCode)
            def json = jsonSlurper.parseText(body)
            assertEquals(0, json.code, "Expected code=0 but got: ${body}")
        }
    }

    // Verify the variable was reset to default
    def var_result4 = sql_return_maparray "show global variables like 'wait_timeout'"
    assertNotEquals("2000", var_result4[0].Value, "wait_timeout should have been reset after _unset_vars")

    // Test 4: PUT /api/_unset_all_vars with null body
    // This API calls forwardToMaster(request, null)
    httpTest {
        endpoint follower_endpoint
        uri "/api/_unset_all_vars"
        op "put"
        basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
        check { respCode, body ->
            assertEquals(200, respCode)
            def json = jsonSlurper.parseText(body)
            assertEquals(0, json.code, "Expected code=0 but got: ${body}")
        }
    }

    // Verify all variables were reset to default (no changed variables)
    def changed_vars = sql "show global variables where changed=1"
    assertEquals(0, changed_vars.size(), "There should be no changed variables after _unset_all_vars")

    // cleanup: reset vars to default
    try_sql "UNSET GLOBAL VARIABLE wait_timeout"
    try_sql "UNSET GLOBAL VARIABLE sql_mode"
}
