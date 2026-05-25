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

#include "runtime/fragment_mgr.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "service/backend_options.h"
#include "util/network_util.h"

namespace doris {

extern std::string construct_url(const std::string& host_port, const std::string& token,
                                 const std::string& path);

class LoadErrorUrlTest : public testing::Test {
protected:
    void SetUp() override {
        _orig_enable_https = config::enable_https;
        _orig_webserver_port = config::webserver_port;
        _orig_localhost = BackendOptions::get_localhost();
    }
    void TearDown() override {
        config::enable_https = _orig_enable_https;
        config::webserver_port = _orig_webserver_port;
        BackendOptions::set_localhost(_orig_localhost);
    }

private:
    bool _orig_enable_https;
    int32_t _orig_webserver_port;
    std::string _orig_localhost;
};

// ==================== to_load_error_http_path tests ====================

TEST_F(LoadErrorUrlTest, to_load_error_http_path_empty) {
    EXPECT_EQ("", to_load_error_http_path(""));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_http_default) {
    config::enable_https = false;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("127.0.0.1");
    EXPECT_EQ("http://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_https_enabled) {
    config::enable_https = true;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("127.0.0.1");
    EXPECT_EQ("https://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_custom_port) {
    config::enable_https = true;
    config::webserver_port = 9050;
    BackendOptions::set_localhost("10.0.0.1");
    EXPECT_EQ("https://10.0.0.1:9050/api/_load_error_log?file=import_error.log",
              to_load_error_http_path("import_error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_ipv6_http) {
    config::enable_https = false;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("::1");
    EXPECT_EQ("http://[::1]:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_ipv6_https) {
    config::enable_https = true;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("::1");
    EXPECT_EQ("https://[::1]:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_ipv6_full_addr) {
    config::enable_https = true;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("fe80::1");
    EXPECT_EQ("https://[fe80::1]:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(LoadErrorUrlTest, to_load_error_http_path_file_with_path) {
    config::enable_https = true;
    config::webserver_port = 8040;
    BackendOptions::set_localhost("127.0.0.1");
    EXPECT_EQ("https://127.0.0.1:8040/api/_load_error_log?file=/path/to/error.log",
              to_load_error_http_path("/path/to/error.log"));
}

// ==================== construct_url tests ====================

TEST_F(LoadErrorUrlTest, construct_url_http) {
    config::enable_https = false;
    EXPECT_EQ("http://127.0.0.1:8040/api/_tablet/_download?token=abc&file=/data/file.dat",
              construct_url("127.0.0.1:8040", "abc", "/data/file.dat"));
}

TEST_F(LoadErrorUrlTest, construct_url_https) {
    config::enable_https = true;
    EXPECT_EQ("https://127.0.0.1:8040/api/_tablet/_download?token=abc&file=/data/file.dat",
              construct_url("127.0.0.1:8040", "abc", "/data/file.dat"));
}

TEST_F(LoadErrorUrlTest, construct_url_ipv6) {
    config::enable_https = true;
    EXPECT_EQ("https://[::1]:8040/api/_tablet/_download?token=xyz&file=/path/file",
              construct_url("[::1]:8040", "xyz", "/path/file"));
}

TEST_F(LoadErrorUrlTest, construct_url_empty_token) {
    config::enable_https = false;
    EXPECT_EQ("http://10.0.0.1:9030/api/_tablet/_download?token=&file=/data/f",
              construct_url("10.0.0.1:9030", "", "/data/f"));
}

// ==================== get_host_port tests ====================

TEST_F(LoadErrorUrlTest, get_host_port_ipv4) {
    EXPECT_EQ("127.0.0.1:8040", get_host_port("127.0.0.1", 8040));
}

TEST_F(LoadErrorUrlTest, get_host_port_ipv6) {
    EXPECT_EQ("[::1]:8040", get_host_port("::1", 8040));
}

TEST_F(LoadErrorUrlTest, get_host_port_ipv6_full) {
    EXPECT_EQ("[fe80::1]:9060", get_host_port("fe80::1", 9060));
}

} // namespace doris
