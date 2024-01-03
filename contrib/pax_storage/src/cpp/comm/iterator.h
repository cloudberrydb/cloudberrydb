#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "comm/cbdb_api.h"

namespace pax {
template <typename T>
class IteratorBase {
 public:
  virtual bool HasNext() = 0;
  virtual T Next() = 0;
  virtual void Rewind() = 0;
  virtual ~IteratorBase() = default;
};  // class IteratorBase

// FilterIterator: wrap an iterator that may have a filter whether to pass
// the value from internal iterator. If the qual function returns true,
// the current item will return to the caller, otherwise the current item
// is ignored.
template <typename T>
class FilterIterator : public IteratorBase<T> {
 public:
 FilterIterator(std::unique_ptr<IteratorBase<T>> &&it, std::function<bool(const T &x)> &&qual)
  : it_(std::move(it)), qual_(std::move(qual)) {
    Assert(it_);
    Assert(qual_);
  }

  bool HasNext() override {
    if (valid_value_) return true;
    while (it_->HasNext()) {
      value_ = std::move(it_->Next());
      if (qual_(value_)) {
        valid_value_ = true;
        break;
      }
    }
    return valid_value_;
  }

  T Next() override {
    Assert(valid_value_);
    valid_value_ = false;
    return std::move(value_);
  }

  void Rewind() override {
    it_->Rewind();
    valid_value_ = false;
  }

  virtual ~FilterIterator() = default;

 protected:
  std::unique_ptr<IteratorBase<T>> it_;
  std::function<bool(const T &x)> qual_;
  T value_;
  bool valid_value_ = false;
};

template <typename T>
class VectorIterator : public IteratorBase<T> {
 public:
  explicit VectorIterator(std::vector<T> &&v): v_(std::move(v)){}
  virtual ~VectorIterator() = default;

  bool HasNext() override { return index_ < v_.size(); }
  T Next() override {
    Assert(HasNext());
    return v_[index_++];
  }
  void Rewind() override { index_ = 0; }

 protected:
  std::vector<T> v_;
  size_t index_ = 0;
};

}  // namespace pax
