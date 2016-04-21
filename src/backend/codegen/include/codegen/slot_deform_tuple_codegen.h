//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    slot_deform_tuple_codegen.h
//
//  @doc:
//    Headers for slot_deform_tuple codegen
//
//---------------------------------------------------------------------------

#ifndef GPCODEGEN_SLOT_DEFORM_TUPLE_CODEGEN_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_SLOT_DEFORM_TUPLE_CODEGEN_H_

#include "codegen/codegen_wrapper.h"
#include "codegen/base_codegen.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class SlotDeformTupleCodegen: public BaseCodegen<SlotDeformTupleFn> {
 public:
  /**
   * @brief Constructor
   *
   * @param regular_func_ptr       Regular version of the target function.
   * @param ptr_to_chosen_func_ptr Reference to the function pointer that the caller will call.
   * @param slot         The slot to use for generating code.
   *
   * @note 	The ptr_to_chosen_func_ptr can refer to either the generated function or the
   * 			corresponding regular version.
   *
   **/
  explicit SlotDeformTupleCodegen(SlotDeformTupleFn regular_func_ptr,
                                  SlotDeformTupleFn* ptr_to_regular_func_ptr,
                                  TupleTableSlot* slot);

  virtual ~SlotDeformTupleCodegen() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::CodegenUtils* codegen_utils) final;

 private:
  TupleTableSlot* slot_;

  static constexpr char kSlotDeformTupleNamePrefix[] = "slot_deform_tuple";

  /**
   * @brief Generates runtime code that calls slot_deform_tuple as an external function.
   *
   * @param codegen_utils Utility to ease the code generation process.
   * @return true on successful generation.
   **/
  bool GenerateSimpleSlotDeformTuple(gpcodegen::CodegenUtils* codegen_utils);
};

/** @} */

}  // namespace gpcodegen
#endif  // GPCODEGEN_SLOT_DEFORM_TUPLE_CODEGEN_H_
