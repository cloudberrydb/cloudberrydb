#include "storage/vec/arrow_wrapper.h"

#ifdef VEC_BUILD

#include "comm/pax_memory.h"
#include "storage/pax_buffer.h"

/// export interface wrapper of arrow
namespace arrow {

void ExportArrayRelease(ArrowArray *array) {
  // The Exception throw from this call back won't be catch
  // Because caller will call this callback in destructor
  // just let long jump happen
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; i++) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }

    pax::PAX_DELETE_ARRAY<ArrowArray *>(array->children);
  }

  if (array->buffers) {
    for (int64_t i = 0; i < array->n_buffers; i++) {
      if (array->buffers[i]) {
        char *temp = const_cast<char *>((const char *)array->buffers[i]);
        pax::BlockBuffer::Free(temp);
      }
    }
    char **temp = const_cast<char **>((const char **)array->buffers);
    pax::PAX_DELETE_ARRAY<char *>(temp);
  }

  array->release = NULL;
  if (array->private_data) {
    ArrowArray *temp = static_cast<ArrowArray *>(array->private_data);
    pax::PAX_DELETE<ArrowArray>(temp);
  }
};

static void ExportArrayNodeDetails(ArrowArray *export_array,
                                   const std::shared_ptr<ArrayData> &data,
                                   const std::vector<ArrowArray *> &child_array,
                                   bool is_child) {
  export_array->length = data->length;
  export_array->null_count = data->null_count;
  export_array->offset = data->offset;

  export_array->n_buffers = static_cast<int64_t>(data->buffers.size());
  export_array->n_children = static_cast<int64_t>(child_array.size());
  export_array->buffers =
      export_array->n_buffers
          ? (const void **)pax::PAX_NEW_ARRAY<char *>(export_array->n_buffers)
          : nullptr;

  for (int64_t i = 0; i < export_array->n_buffers; i++) {
    auto buffer = data->buffers[i];
    export_array->buffers[i] = buffer ? buffer->data() : nullptr;
  }

  export_array->children =
      export_array->n_children
          ? pax::PAX_NEW_ARRAY<ArrowArray *>(export_array->n_children)
          : nullptr;
  for (int64_t i = 0; i < export_array->n_children; i++) {
    export_array->children[i] = child_array[i];
  }

  export_array->dictionary = nullptr;
  export_array->private_data = is_child ? (void *)export_array : nullptr;
  export_array->release = ExportArrayRelease;
}

static ArrowArray *ExportArrayNode(const std::shared_ptr<ArrayData> &data) {
  ArrowArray *export_array;
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }

  export_array = pax::PAX_NEW<ArrowArray>();
  ExportArrayNodeDetails(export_array, data, child_array, true);
  return export_array;
}

void ExportArrayRoot(const std::shared_ptr<ArrayData> &data,
                     ArrowArray *export_array) {
  std::vector<ArrowArray *> child_array;

  for (size_t i = 0; i < data->child_data.size(); ++i) {
    child_array.emplace_back(ExportArrayNode(data->child_data[i]));
  }
  Assert(export_array);

  ExportArrayNodeDetails(export_array, data, child_array, false);
}

}  // namespace arrow

#endif  // VEC_BUILD
