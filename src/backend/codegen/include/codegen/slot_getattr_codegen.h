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

#include "codegen/codegen_wrapper.h"
#include "codegen/utils/gp_codegen_utils.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class SlotGetAttrCodegen {
  /**
   * @brief Generate code for the codepath slot_getattr > _slot_getsomeattr >
   * slot_deform_tuple
   *
   * @param codegen_utils
   * @param function_name Name of the generated function
   * @param slot Use the TupleDesc for this slot to generate
   * @param max_attr Generate slot deformation upto this many attributes
   * @param out_func Return the generated llvm::Function
   *
   * @note This is a wrapper around GenerateSlotGetAttrInternal that handles the
   * case when generation fails, and cleans up a possible broken function by
   * removing it from the module.
   **/
 public:
  static bool GenerateSlotGetAttr(
      gpcodegen::GpCodegenUtils* codegen_utils,
      const std::string& function_name,
      TupleTableSlot* slot,
      int max_attr,
      llvm::Function** out_func);

 private:
  /**
   * @brief Generate code for the codepath slot_getattr > _slot_getsomeattr >
   * slot_deform_tuple
   *
   * @param codegen_utils
   * @param function_name Name of the generated function
   * @param slot Use the TupleDesc for this slot to generate
   * @param max_attr Generate slot deformation upto this many attributes
   * @param out_func Return the generated llvm::Function
   *
   * @return true on success generation; false otherwise
   *
   * @note Generate code for code path slot_getattr > * _slot_getsomeattrs >
   * slot_deform_tuple. slot_getattr() will eventually call slot_deform_tuple
   * (through _slot_getsomeattrs), which fetches all yet unread attributes of
   * the slot until the given attribute. Moreover, instead of looping over the
   * target list one at a time, this approach uses slot_getattr only once, with
   * the largest attribute index from the target list.
   *
   *
   * This implementation does not support:
   *  (1) Variable length attributes
   *  (2) Attributes passed by reference
   *
   * If at execution time, we see any of the above types of attributes,
   * we fall backs to the regular function.
   **/
  static bool GenerateSlotGetAttrInternal(
      gpcodegen::GpCodegenUtils* codegen_utils,
      const std::string& function_name,
      TupleTableSlot* slot,
      int max_attr,
      llvm::Function** out_func);
};

/** @} */

}  // namespace gpcodegen
#endif // GPCODEGEN_SLOT_GETATTR_CODEGEN_H_
