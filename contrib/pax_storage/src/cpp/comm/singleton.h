#pragma once

#include <memory>
#include <mutex>
#include <utility>
namespace pax {

template <typename T>
class Singleton final {
 public:
  Singleton(const Singleton &) = delete;
  Singleton &operator=(const Singleton &) = delete;
  template <typename... ArgTypes>
  static T *GetInstance(ArgTypes &&...args) {
    std::call_once(
        of,
        [](ArgTypes &&...args) {
          instance.reset(new T(std::forward<ArgTypes>(args)...));
        },
        std::forward<ArgTypes>(args)...);

    return instance.get();
  }

  static inline void Destroy() {
    if (instance) {
      instance.reset();
    }
  }

 private:
  Singleton() = default;
  ~Singleton() = default;
  static std::once_flag of;
  static std::unique_ptr<T> instance;
};

template <class T>
std::once_flag Singleton<T>::of;

template <class T>
std::unique_ptr<T> Singleton<T>::instance = nullptr;
}  // namespace pax
