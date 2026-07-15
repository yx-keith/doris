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

#include <gtest/gtest.h>

#include <string>

#include "vec/data_types/data_type_decimal.h"
#include "vec/functions/function_cast.h"

namespace doris::vectorized {

using StrictDecimalStringParsing = StringParsing<DataTypeDecimal<Decimal128V3>, NameStrictCast>;

TEST(StrictDecimalCastTest, reject_lossy_decimal_strings) {
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string("100", 3, 38, 0));
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string("100.000", 7, 38, 0));
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string("10000000000e-10", 15, 38, 0));

    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("100.4", 5, 38, 0));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("300.5", 5, 38, 0));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("invalid", 7, 38, 0));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("+.", 2, 38, 0));

    std::string column_string_value = "100.000";
    column_string_value.push_back('\0');
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string(
            column_string_value.data(), column_string_value.size() - 1, 38, 0));

    std::string max_decimal(38, '9');
    std::string overflow_decimal(39, '9');
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string(
            max_decimal.data(), max_decimal.size(), 38, 0));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string(
            overflow_decimal.data(), overflow_decimal.size(), 38, 0));
}

TEST(StrictDecimalCastTest, preserve_scale_without_rounding) {
    ASSERT_TRUE(StrictDecimalStringParsing::is_lossless_decimal_string("12.3400", 7, 5, 2));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("12.345", 6, 5, 2));
    ASSERT_FALSE(StrictDecimalStringParsing::is_lossless_decimal_string("1234.56", 7, 5, 2));
}

} // namespace doris::vectorized
