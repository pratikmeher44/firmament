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

#include <unordered_map>
#include "misc/utils.h"

namespace firmament {

template <class KeyType, class ItemsType>
struct KeyItems {
  KeyType key;
  ItemsType Items;
  bool shuttingDown;
};

template <class Type1, class Type2>
class FirmamentSchedulerServiceQueue {
public:
  FirmamentSchedulerServiceQueue();
  ~FirmamentSchedulerServiceQueue();
  void Add(Type1, Type2);
  struct KeyItems<Type1, Type2>& Get();
  void Done(Type1);
  void ShutDown();
  bool ShuttingDown();

private:
  boost::recursive_mutex firmament_service_queue_lock_;
  boost::condition_variable_any firmament_service_cond_var_;
  vector<Type1> key_queue;
  unordered_set<Type1> processing;
  bool shuttingDown;
  unordered_multimap<Type1, Type2> items;
  unordered_multimap<Type1, Type2> toQueue;
  struct KeyItems<Type1, Type2> get_key_items;
};

template <typename KeyType, typename ItemType>
void CreateQueue(KeyType, ItemType);

} // namespace firmament
