/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "velox/omnisci/DataConvertor.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using facebook::velox::test::VectorMaker;
using namespace facebook::velox::cider;
using facebook::velox::cider::DataConvertor;

class ResultConvertTest : public testing::Test {
 protected:
  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

TEST_F(ResultConvertTest, VeloxToCiderDirectConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  int numRows = 10;
  auto rowType =
      ROW({"col_0", "col_1", "col_2", "col_3"},
          {INTEGER(), BIGINT(), DOUBLE(), BOOLEAN()});

  std::vector<std::optional<int32_t>> data_0 = {
      0, std::nullopt, 1, 3, std::nullopt, -1234, -99, -999, 1000, -1};
  auto c_0 = vectorMaker_.flatVectorNullable<int32_t>(data_0);
  std::vector<std::optional<int64_t>> data_1 = {
      0, 1, std::nullopt, 3, 1024, -123456, -99, -999, std::nullopt, -1};
  auto c_1 = vectorMaker_.flatVectorNullable<int64_t>(data_1);
  std::vector<std::optional<double>> data_2 = {
      0.5,
      1,
      std::nullopt,
      3.14,
      1024,
      -123456,
      -99.99,
      -999,
      std::nullopt,
      -1};
  auto c_2 = vectorMaker_.flatVectorNullable<double>(data_2);
  std::vector<std::optional<bool>> data_3 = {
      true,
      false,
      std::nullopt,
      false,
      true,
      true,
      false,
      std::nullopt,
      false,
      true,
  };
  auto c_3 = vectorMaker_.flatVectorNullable<bool>(data_3);
  auto rowVector = vectorMaker_.rowVector({c_0, c_1, c_2, c_3});
  CiderResultSet crs = convertor->convertToCider(rowVector, numRows);
  EXPECT_EQ(10, crs.numRows);

  int8_t** colBuffer = crs.colBuffer;
  int32_t* col_0 = reinterpret_cast<int32_t*>(colBuffer[0]);
  int64_t* col_1 = reinterpret_cast<int64_t*>(colBuffer[1]);
  double* col_2 = reinterpret_cast<double*>(colBuffer[2]);
  // uint8_t* col_3 = reinterpret_cast<uint8_t*>(colBuffer[3]);
  // int8_t* col_3 = reinterpret_cast<int8_t*>(colBuffer[3]);
  // uint64_t* col_3_tmp = reinterpret_cast<uint64_t*>(colBuffer[3]);
  int8_t* col_3 = colBuffer[3];
  for (auto idx = 0; idx < numRows; idx++) {
    if (data_0[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int32_t>(), col_0[idx]);
    } else {
      EXPECT_EQ(data_0[idx], col_0[idx]);
    }
  }
  for (auto idx = 0; idx < numRows; idx++) {
    if (data_1[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int64_t>(), col_1[idx]);
    } else {
      EXPECT_EQ(data_1[idx], col_1[idx]);
    }
  }
  for (auto idx = 0; idx < numRows; idx++) {
    if (data_2[idx] == std::nullopt) {
      EXPECT_EQ(DBL_MIN, col_2[idx]);
    } else {
      EXPECT_EQ(data_2[idx], col_2[idx]);
    }
  }

  for (auto idx = 0; idx < numRows; idx++) {
    if (data_3[idx] == std::nullopt) {
      EXPECT_EQ(inline_int_null_value<int8_t>(), col_3[idx]);
    } else {
      EXPECT_EQ(data_3[idx].value(), static_cast<bool>(col_3[idx]));
    }
  }
}

TEST_F(ResultConvertTest, VeloxToCiderArrowConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::ARROW);
  RowVectorPtr input;
  int num_rows;
  // CiderResultSet crs = convertor->convertToCider(input, num_rows);
  // CHECK EQUAL
}

TEST_F(ResultConvertTest, CiderToVeloxDirectConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::DIRECT);
  int num_rows = 10;
  int8_t** col_buffer = (int8_t**)std::malloc(sizeof(int8_t*) * 4);

  int32_t* col_0 = (int32_t*)std::malloc(sizeof(int32_t) * 10);
  int64_t* col_1 = (int64_t*)std::malloc(sizeof(int64_t) * 10);
  double* col_2 = (double*)std::malloc(sizeof(double) * 10);
  int8_t* col_3 = (int8_t*)std::malloc(sizeof(int8_t) * 10);

  for (int i = 0; i < num_rows; i++) {
    col_0[i] = i;
    col_1[i] = i * 123;
    col_2[i] = i * 3.14;
    col_3[i] = i % 2 ? true : false;
  }

  for (int i = 3; i < num_rows; i += 3) {
    col_0[i] = inline_int_null_value<int32_t>();
    col_1[i] = inline_int_null_value<int64_t>();
    col_2[i] = DBL_MIN;
    col_3[i] = inline_int_null_value<int8_t>();
  }

  col_buffer[0] = reinterpret_cast<int8_t*>(col_0);
  col_buffer[1] = reinterpret_cast<int8_t*>(col_1);
  col_buffer[2] = reinterpret_cast<int8_t*>(col_2);
  col_buffer[3] = col_3;

  std::vector<std::string> col_names = {"col_0", "col_1", "col_2", "col_3"};
  std::vector<std::string> col_types = {"INT", "BIGINT", "DOUBLE", "BOOL"};
  RowVectorPtr rvp = convertor->convertToRowVector(
      col_buffer, col_names, col_types, num_rows, pool_.get());
  RowVector* row = rvp.get();
  auto* rowVector = row->as<RowVector>();
  EXPECT_EQ(4, rowVector->childrenSize());
  VectorPtr& child_0 = rowVector->childAt(0);
  EXPECT_TRUE(child_0->mayHaveNulls());
  auto childVal_0 = child_0->asFlatVector<int32_t>();
  auto* rawValues_0 = childVal_0->mutableRawValues();
  auto nulls_0 = child_0->rawNulls();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_0[idx] == inline_int_null_value<int32_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_0, idx));
    } else {
      EXPECT_EQ(rawValues_0[idx], col_0[idx]);
    }
  }
  VectorPtr& child_1 = rowVector->childAt(1);
  EXPECT_TRUE(child_1->mayHaveNulls());
  auto childVal_1 = child_1->asFlatVector<int64_t>();
  auto* rawValues_1 = childVal_1->mutableRawValues();
  auto nulls_1 = child_1->rawNulls();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_1[idx] == inline_int_null_value<int64_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_1, idx));
    } else {
      EXPECT_EQ(rawValues_1[idx], col_1[idx]);
    }
  }
  VectorPtr& child_2 = rowVector->childAt(2);
  EXPECT_TRUE(child_2->mayHaveNulls());
  auto childVal_2 = child_2->asFlatVector<double>();
  auto* rawValues_2 = childVal_2->mutableRawValues();
  auto nulls_2 = child_2->rawNulls();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_2[idx] == DBL_MIN) {
      EXPECT_TRUE(bits::isBitNull(nulls_2, idx));
    } else {
      EXPECT_EQ(rawValues_2[idx], col_2[idx]);
    }
  }
  VectorPtr& child_3 = rowVector->childAt(3);
  EXPECT_TRUE(child_3->mayHaveNulls());
  auto childVal_3 = child_3->asFlatVector<bool>();
  auto* rawValues_3 = childVal_3->mutableRawValues();
  auto nulls_3 = child_3->rawNulls();
  for (auto idx = 0; idx < num_rows; idx++) {
    if (col_3[idx] == inline_int_null_value<int8_t>()) {
      EXPECT_TRUE(bits::isBitNull(nulls_3, idx));
    } else {
      EXPECT_EQ(childVal_3->valueAt(idx), col_3[idx]);
    }
  }
  // release buffer
  std::free(col_buffer[0]);
  std::free(col_buffer[1]);
  std::free(col_buffer[2]);
  std::free(col_buffer[3]);
  std::free(col_buffer);
}

TEST_F(ResultConvertTest, CiderToVeloxArrowConvert) {
  std::shared_ptr<DataConvertor> convertor =
      DataConvertor::create(CONVERT_TYPE::ARROW);
  int8_t** col_buffer;
  int num_rows;
  // RowVectorPtr rvp = convertor->convertToRowVector(col_buffer, num_rows);
  // CHECK EQUAL
}