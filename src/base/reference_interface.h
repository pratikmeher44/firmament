// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common reference interface and implementation. This is subclassed by the
// various reference type classes.

#ifndef FIRMAMENT_BASE_REFERENCE_INTERFACE_H
#define FIRMAMENT_BASE_REFERENCE_INTERFACE_H

#include "base/common.h"
#include "base/reference_desc.pb.h"
#include "misc/printable_interface.h"

namespace firmament {

typedef ReferenceDescriptor::ReferenceType ReferenceType_t;
typedef uint64_t ReferenceID_t;

class ReferenceInterface : public PrintableInterface {
 public:
  // All reference types must defined 2 types of constructor:
  // 1. one taking a ReferenceID_t, and manufacturing a new reference of this
  //    name
  // 2. one taking a ReferenceDescriptor, which acts as a "copy" constructor,
  //    constructing a reference object from the serialized
  //    ReferenceDescriptor.
  explicit ReferenceInterface(ReferenceID_t id)
    : id_(id) { }
  explicit ReferenceInterface(const ReferenceDescriptor& desc)
    : id_(desc.id()) {}
  // Non-accessor members
  inline ReferenceDescriptor AsProtobuf() { return desc(); }
  virtual inline bool Consumable() = 0;
  virtual ostream& ToString(ostream* stream) const = 0;
  // Accessor methods
  inline ReferenceID_t id() { return id_; }
  inline ReferenceDescriptor desc() {
    ValidateInternalDescriptor();
    return desc_;
  }
 protected:
  virtual void ValidateInitDescriptor(const ReferenceDescriptor& desc) = 0;
  void ValidateInternalDescriptor() {
    CHECK_EQ(id_, desc_.id());
  }
  // Fields shared between all reference types
  ReferenceID_t id_;
  ReferenceDescriptor desc_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_REFERENCE_INTERFACE_H