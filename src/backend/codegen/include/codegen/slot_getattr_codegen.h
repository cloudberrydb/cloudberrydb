//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    slot_getattr_codegen.h
//
//  @doc:
//    Contains slot_getattr generator
//
//---------------------------------------------------------------------------

#ifndef GPCODEGEN_SLOT_GETATTR_CODEGEN_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_SLOT_GETATTR_CODEGEN_H_

#include <string>
#include <utility>

#include "codegen/codegen_wrapper.h"
#include "codegen/base_codegen.h"
#include "codegen/utils/gp_codegen_utils.h"

extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#include "utils/elog.h"
#include "access/htup.h"
#include "nodes/execnodes.h"
#include "executor/tuptable.h"

}

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class SlotGetAttrCodegen : public BaseCodegen<SlotGetAttrFn> {
 public:
  /**
   * @brief Request code generation for the codepath slot_getattr >
   * _slot_getsomeattr > slot_deform_tuple for the given slot and max_attr
   *
   * @param codegen_utils Utilities for easy code generation
   * @param slot          Use the TupleDesc from this slot to generate
   * @param max_attr      Generate slot deformation up to this many attributes
   * @param out_func      Return the llvm::Function that will be generated
   *
   * @note This method does not actually do any code generation, but simply
   * caches the information necessary for code generation when
   * GenerateSlotGetAttr() is called. This way we can de-duplicate multiple
   * requests for multiple slot_getattr() implementation producing only one per
   * slot.
   *
   **/
  static SlotGetAttrCodegen* GetCodegenInstance(
      gpcodegen::CodegenManager* manager,
      TupleTableSlot* slot,
      int max_attr);

  virtual ~SlotGetAttrCodegen();

  /**
   * @brief Generate code for the codepath slot_getattr > _slot_getsomeattr >
   * slot_deform_tuple for the given slot and max_attr
   *
   * @param codegen_utils Utilities for easy code generation
   *
   * Based on parameters given to SlotGetAttr::GetCodegenInstance(), the maximum
   * max_attr is tracked for each slot. Then for each slot, we generate code for
   * slot_getattr() that deforms tuples in that slot up to the maximum max_attr.
   *
   * @note This is a wrapper around GenerateSlotGetAttrInternal that handles the
   * case when generation fails, and cleans up a possible broken function by
   * removing it from the module.
   *
   * TODO(shardikar, krajaraman) Remove this wrapper after a shared code
   * generation framework implementation is complete.
   */
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) override;

  /*
   * @return A pointer to the yet un-compiled llvm::Function that will be
   * generated and populate by this module
   *
   * @note This is initialized as NULL when on creation. Only if after code
   * generation is performed by calling SlotGetAttrCodegen::GenerateCode and is
   * successful, can this be assumed return a valid llvm::Function*
   */
  llvm::Function* GetGeneratedFunction() {
    return llvm_function_;
  }

 private:
  /**
   * @brief Constructor for SlotGetAttrCodegen
   *
   * @note The call to the constructor of BaseCodegen passes in a pointer to a
   * dummy SlotGetAttrFn stored in this class. Thus SlotGetAttrCodegen can
   * inherit all the functionality of BaseCodegen, including the pointer
   * swapping logic, with low risk. However the pointer swapping will be of no
   * consequence.
   */
  SlotGetAttrCodegen(gpcodegen::CodegenManager* manager,
                     TupleTableSlot* slot,
                     int max_attr)
  : BaseCodegen(manager, kSlotGetAttrPrefix, slot_getattr, &dummy_func_),
    slot_(slot),
    max_attr_(max_attr),
    llvm_function_(nullptr) {
  }

  /**
   * @brief Generate code for the codepath slot_getattr > _slot_getsomeattr >
   * slot_deform_tuple
   *
   * @param codegen_utils Utilities for easy code generation
   * @param slot          Use the TupleDesc from this slot to generate
   * @param max_attr      Generate slot deformation up to this many attributes
   * @param out_func      Return the llvm::Function that will be generated
   *
   * @return true on success generation; false otherwise
   *
   * @note Generate code for code path slot_getattr > * _slot_getsomeattrs >
   * slot_deform_tuple. slot_getattr() will eventually call slot_deform_tuple
   * (through _slot_getsomeattrs), which fetches all yet unread attributes of
   * the slot until the given attribute.
   *
   * This implementation does not support:
   *  (1) Attributes passed by reference
   *
   * If at execution time, we see any of the above types of attributes,
   * we fall backs to the regular function.
   **/
  bool GenerateSlotGetAttr(
      gpcodegen::GpCodegenUtils* codegen_utils,
      TupleTableSlot* slot,
      int max_attr,
      llvm::Function* out_func);

  /**
   * @brief Removes the entry of this SlotGetAttrCodegen from the static cache.
   */
  void RemoveSelfFromCache();

  TupleTableSlot* slot_;
  // Max attribute to deform to
  int max_attr_;
  // Primary function to be generated and populated
  llvm::Function* llvm_function_;
  // A dummy function pointer that can be swapped by the BaseCodegen
  // implementation
  SlotGetAttrFn dummy_func_;

  static constexpr char kSlotGetAttrPrefix[] = "slot_getattr";
  /**
   * Utility map indexed by CodegenManager pointer to track all instances of
   * SlotGetAttrCodegen created by SlotGetAttrCodegen::GetCodegenInstance.
   * Each entry of the utility map contains another map keyed on the de-duplication key
   * used by SlotGetAttrCodegen::GetCodegenInstance i.e slot.
   *
   * Entries are inserted into this map on creation of SlotGetAttrCodegen objects, and
   * removed on destruction. When an inner-map for some manager becomes empty
   * (when the manager itself is destroyed), the entry for that manager is
   * erased from the outer-map as well
   *
   * Map of the form: { manager -> { slot -> slot_getattr_codegen } }
   */
  typedef std::unordered_map<TupleTableSlot*, SlotGetAttrCodegen*>
      SlotGetAttrCodegenCache;
  static std::unordered_map<gpcodegen::CodegenManager*, SlotGetAttrCodegenCache>
      codegen_cache_by_manager;
};

/** @} */

}  // namespace gpcodegen
#endif  // GPCODEGEN_SLOT_GETATTR_CODEGEN_H_
