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

#include "firmament_scheduler_service_queue.h"

namespace firmament {

template <typename Type1, typename Type2>
FirmamentSchedulerServiceQueue<Type1, Type2>::FirmamentSchedulerServiceQueue() {
  // Constructor
}

template <typename Type1, typename Type2>
FirmamentSchedulerServiceQueue<Type1, Type2>::~FirmamentSchedulerServiceQueue() {
  // Destructor
}

// Add function
template <typename Type1, typename Type2>
void FirmamentSchedulerServiceQueue<Type1, Type2>::Add(Type1 key, Type2 item) {
  boost::lock_guard<boost::recursive_mutex> lock(firmament_service_queue_lock_);
  if(shuttingDown)
    return;
  if (processing.find(key) != processing.end()) {
    // Key is under processing. Can not add it to the queue.
    toQueue.insert(pair<Type1, Type2>(key, item));
  }
  else {
    typename unordered_multimap<Type1, Type2>::iterator it = items.find(key);
    items.insert(pair<Type1, Type2>(key, item));
    if(it == items.end()) {
      // New key in the queue. Send signal.
      key_queue.push_back(key);
      // Notify any other threads waiting to execute processes
      firmament_service_cond_var_.notify_one();
    }
  }
}

// Get function
template <typename Type1, typename Type2>
struct KeyItems<Type1, Type2>& FirmamentSchedulerServiceQueue<Type1, Type2>::Get() {
  boost::lock_guard<boost::recursive_mutex> lock(firmament_service_queue_lock_);
  while((key_queue.size() == 0) && !shuttingDown) {
    firmament_service_cond_var_.wait(lock);
  }
  if(key_queue.size() == 0) {
    // We must be shutting down.
    get_key_items.key = 0;
    get_key_items.Items = 0;
    get_key_items.shuttingDown = true;
    return get_key_items;
  }
  Type1 key = key_queue.front();
  key_queue.erase(key_queue.begin() + 0);
  // Add key to the processing set.
  processing.insert(key);
  auto items = items.equal_range(key);
  items.erase(key);
  get_key_items.key = key;
  get_key_items.Items = items;
  get_key_items.shuttingDown = false;
  return get_key_items;
}

// Done function
template <typename Type1, typename Type2>
void FirmamentSchedulerServiceQueue<Type1, Type2>::Done(Type1 key) {
  boost::lock_guard<boost::recursive_mutex> lock(firmament_service_queue_lock_);
  processing.erase(key);
  auto items = toQueue.equal_range(key);
  if(items.first != items.second) {
    key_queue.insert(key);
    for (auto it = items.first; it != items.second; ++it) {
      items.insert(pair<Type1, Type2>(key, it->second));
    }
    toQueue.erase(key);
    firmament_service_cond_var_.notify_one();
  }
}

// ShutDown function
template <typename Type1, typename Type2>
void FirmamentSchedulerServiceQueue<Type1, Type2>::ShutDown() {
  boost::lock_guard<boost::recursive_mutex> lock(firmament_service_queue_lock_);
  shuttingDown = true;
  firmament_service_cond_var_.notify_all();
}

// ShuttingDown
template <typename Type1, typename Type2>
bool FirmamentSchedulerServiceQueue<Type1, Type2>::ShuttingDown() {
  boost::lock_guard<boost::recursive_mutex> lock(firmament_service_queue_lock_);
  return shuttingDown;
}

template <typename KeyType, typename ItemType>
void CreateQueue(KeyType, ItemType) {
    //FirmamentSchedulerServiceQueue<KeyType, ItemType> test_queue;
    
}

} // namespace firmament
