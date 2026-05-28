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

#include <cstdint>

#include <gtest/gtest.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransport.h>

#include "common/config.h"
#include "gen_cpp/FrontendService.h"
#include "util/thrift_client.h"
#include "util/thrift_server.h"

namespace doris {

class ThriftClientTest : public testing::Test {
public:
    ThriftClientTest() {}
    virtual ~ThriftClientTest() {}
    void SetUp() override { _orig_max_msg_size = config::thrift_max_message_size; }
    void TearDown() override { config::thrift_max_message_size = _orig_max_msg_size; }

private:
    int32_t _orig_max_msg_size;
};

TEST_F(ThriftClientTest, default_constructor_max_message_size) {
    ThriftClient<FrontendServiceClient> client("127.0.0.1", 9030);
    auto transport = client._transport;
    ASSERT_NE(nullptr, transport);
    uint32_t expected = static_cast<uint32_t>(config::thrift_max_message_size);
    EXPECT_EQ(transport->getConfiguration()->getMaxMessageSize(), expected);
}

TEST_F(ThriftClientTest, threaded_server_type_max_message_size) {
    ThriftClient<FrontendServiceClient> client("127.0.0.1", 9030,
                                               ThriftServer::THREADED);
    auto transport = client._transport;
    ASSERT_NE(nullptr, transport);
    uint32_t expected = static_cast<uint32_t>(config::thrift_max_message_size);
    EXPECT_EQ(transport->getConfiguration()->getMaxMessageSize(), expected);
}

TEST_F(ThriftClientTest, nonblocking_server_type_max_message_size) {
    ThriftClient<FrontendServiceClient> client("127.0.0.1", 9030,
                                               ThriftServer::NON_BLOCKING);
    auto transport = client._transport;
    ASSERT_NE(nullptr, transport);
    uint32_t expected = static_cast<uint32_t>(config::thrift_max_message_size);
    EXPECT_EQ(transport->getConfiguration()->getMaxMessageSize(), expected);
}

TEST_F(ThriftClientTest, config_change_reflected_in_new_client) {
    config::thrift_max_message_size = 209715200;
    ThriftClient<FrontendServiceClient> client("127.0.0.1", 9030);
    EXPECT_EQ(client._transport->getConfiguration()->getMaxMessageSize(),
              static_cast<uint32_t>(209715200));
}

TEST_F(ThriftClientTest, default_max_message_size_exceeds_thrift_default) {
    EXPECT_GT(config::thrift_max_message_size, 16384000);
    EXPECT_EQ(config::thrift_max_message_size, 104857600);
}

TEST_F(ThriftClientTest, set_config_max_message_size) {
    EXPECT_TRUE(config::set_config("thrift_max_message_size", "209715200").ok());
    EXPECT_EQ(config::thrift_max_message_size, 209715200);
    EXPECT_TRUE(config::set_config("thrift_max_message_size", "104857600").ok());
}

} // namespace doris
