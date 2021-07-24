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

#pragma once

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/eviction_policy.h"
#include "ray/object_manager/plasma/object_store.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

class ObjectLifecycleManager {
 public:
  ObjectLifecycleManager(IAllocator &allocator,
                         ray::DeleteObjectCallback delete_object_callback);

  /// Create a new object given object's info.
  ///
  /// \param object_info Plasma object info.
  /// \param source From where the object is created.
  /// \param fallback_allocator Whether to allow fallback allocation.
  /// \return
  ///   - pointer to created object and PlasmaError::OK when succeeds.
  ///   - nullptr and error message, including ObjectExists/OutOfMemory
  std::pair<const LocalObject *, flatbuf::PlasmaError> CreateObject(
      const ray::ObjectInfo &object_info, plasma::flatbuf::ObjectSource source,
      bool fallback_allocator);

  /// Get object by id.
  /// \return
  ///   - nullptr if such object doesn't exist.
  ///   - otherwise, pointer to the object.
  const LocalObject *GetObject(const ObjectID &object_id) const;

  /// Seal created object by id.
  ///
  /// \param object_id Object ID of the object to be sealed.
  /// \return
  ///   - nulltpr if such object doesn't exist, or the object has already been sealed.
  ///   - otherise, pointer to the sealed object.
  const LocalObject *SealObject(const ObjectID &object_id);

  /// Abort object creation by id.
  ///
  /// \param object_id Object ID of the object to be aborted.
  /// \return
  ///   - false if such object doesn't exist, or the object has already been sealed.
  ///   - true if abort successfuly.
  bool AbortObject(const ObjectID &object_id);

  /// Delete a specific object by object_id that have been created in the hash table.
  ///
  /// \param object_id Object ID of the object to be deleted.
  /// \return One of the following error codes:
  ///  - PlasmaError::OK, if the object was delete successfully.
  ///  - PlasmaError::ObjectNonexistent, if ths object isn't existed.
  ///  - PlasmaError::ObjectInUse, if the object is in use.
  flatbuf::PlasmaError DeleteObject(const ObjectID &object_id);

  /// Bump up the reference count of the object.
  ///
  /// \return true if object exists, false otherise.
  bool AddReference(const ObjectID &object_id);

  /// Decrese the reference count of the object. When reference count
  /// drop to zero the object becomes evictable.
  ///
  /// \return true if object exists and reference count is greater than 0, false otherise.
  bool RemoveReference(const ObjectID &object_id);

  /// Ask it to evict objects until we have at least size of capacity
  /// available.
  ///
  /// \return The number of bytes evicted.
  int64_t RequireSpace(int64_t size);

  std::string EvictionPolicyDebugString() const;

  bool IsObjectSealed(const ObjectID &object_id) const;

  size_t GetNumBytesInUse() const;

  size_t GetNumBytesCreatedTotal() const;

  size_t GetNumBytesUnsealed() const;

  size_t GetNumObjectsUnsealed() const;

  void GetDebugDump(std::stringstream &buffer) const;

 private:
  const LocalObject *CreateObjectInternal(const ray::ObjectInfo &object_info,
                                          plasma::flatbuf::ObjectSource source,
                                          bool allow_fallback_allocation);

  /// Evict objects returned by the eviction policy.
  ///
  /// \param object_ids Object IDs of the objects to be evicted.
  void EvictObjects(const std::vector<ObjectID> &object_ids);

  void DeleteObjectImpl(const ObjectID &object_id);

  IAllocator &allocator_;
  ObjectStore object_store_;
  EvictionPolicy eviction_policy_;
  const ray::DeleteObjectCallback delete_object_callback_;
  /// The amount of time to wait between logging space usage debug messages.
  const uint64_t usage_log_interval_ns_;
  uint64_t last_usage_log_ns_;

  std::unordered_set<ObjectID> deletion_cache_;

  /// Total number of bytes allocated to objects that are in use by any client.
  /// This includes objects that are being created and objects that a client
  /// called get on.
  size_t num_bytes_in_use_;
};

}  // namespace plasma
