//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    codegen_utils.cpp
//
//  @doc:
//    Contains different code generators
//
//---------------------------------------------------------------------------
#include <cstdint>
#include <string>

#include "codegen/slot_deform_tuple_codegen.h"
#include "codegen/utils/clang_compiler.h"
#include "codegen/utils/utility.h"
#include "codegen/utils/instance_method_wrappers.h"
#include "codegen/utils/codegen_utils.h"

#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/APInt.h"
#include "llvm/IR/Argument.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constant.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Value.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/Casting.h"

extern "C" {
#include "utils/elog.h"
}

using gpcodegen::SlotDeformTupleCodegen;

constexpr char SlotDeformTupleCodegen::kSlotDeformTupleNamePrefix[];

SlotDeformTupleCodegen::SlotDeformTupleCodegen(
    SlotDeformTupleFn regular_func_ptr,
    SlotDeformTupleFn* ptr_to_regular_func_ptr,
    TupleTableSlot* slot) :
        BaseCodegen(
            kSlotDeformTupleNamePrefix,
            regular_func_ptr,
            ptr_to_regular_func_ptr), slot_(slot) {
}

static void ElogWrapper(const char* func_name) {
  elog(INFO, "Calling wrapped function: %s", func_name);
}

bool SlotDeformTupleCodegen::GenerateSimpleSlotDeformTuple(
    gpcodegen::CodegenUtils* codegen_utils) {
  llvm::Function* llvm_elog_wrapper = codegen_utils->RegisterExternalFunction(
      ElogWrapper);
  assert(llvm_elog_wrapper != nullptr);

  SlotDeformTupleFn regular_func_pointer = GetRegularFuncPointer();
  llvm::Function* llvm_regular_function =
      codegen_utils->RegisterExternalFunction(regular_func_pointer);
  assert(llvm_regular_function != nullptr);

  llvm::Function* llvm_function =
      codegen_utils->CreateFunction<SlotDeformTupleFn>(
          GetUniqueFuncName());

  llvm::BasicBlock* function_body = codegen_utils->CreateBasicBlock(
      "fn_body", llvm_function);

  codegen_utils->ir_builder()->SetInsertPoint(function_body);
  llvm::Value* func_name_llvm = codegen_utils->GetConstant(
      GetOrigFuncName().c_str());
  codegen_utils->ir_builder()->CreateCall(
      llvm_elog_wrapper, { func_name_llvm });

  std::vector<llvm::Value*> forwarded_args;

  for (llvm::Argument& arg : llvm_function->args()) {
    forwarded_args.push_back(&arg);
  }

  llvm::CallInst* call = codegen_utils->ir_builder()->CreateCall(
      llvm_regular_function, forwarded_args);

  codegen_utils->ir_builder()->CreateRetVoid();

  return true;
}

bool SlotDeformTupleCodegen::GenerateCodeInternal(CodegenUtils* codegen_utils) {
  GenerateSimpleSlotDeformTuple(codegen_utils);
  return true;
}
