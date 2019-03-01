/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include <gtest/gtest.h>

//#include "firmament_scheduler_service_queue.h"
#include "firmament_service_queue.h"

namespace firmament {

class FirmamentSchedulerServiceQueueTest : public ::testing::Test {
protected:
  FirmamentSchedulerServiceQueueTest() {
    // You can do initial set-up work for each test here.
  }

  virtual ~FirmamentSchedulerServiceQueueTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

};

TEST_F(FirmamentSchedulerServiceQueueTest, QueueTest) {
  FirmamentSchedulerServiceQueue<string, string> test_queue;
  test_queue.Add("10", "Value10");
  test_queue.Add("3", "Value3");
  test_queue.Add("2", "Value2");
  test_queue.Add("4", "Value4");
  //test_queue.Add("1", "Value11");
  //test_queue.Add("1", "Value111");
  test_queue.Add("10", "Value1010");
  test_queue.Add("10", "Value101010");
  test_queue.print_queue();
  struct KeyItems<string, string> get_key_items = test_queue.Get();
  LOG(INFO) << "key = " << get_key_items.key;
  LOG(INFO) << "values: ";
  for(auto it = get_key_items.Items.begin(); it != get_key_items.Items.end(); ++it)
    LOG(INFO) << *it << " ";
  test_queue.print_queue();
  test_queue.Add("1", "Value111");
  test_queue.print_queue();
  test_queue.Done("2");
  test_queue.print_queue();
}

} // namespace firmament

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
