//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    exec_eval_expr_codegen.cc
//
//  @doc:
//    Generates code for ExecEvalExpr function.
//
//---------------------------------------------------------------------------
#include <algorithm>
#include <cstdint>
#include <string>

#include "codegen/exec_eval_expr_codegen.h"
#include "codegen/expr_tree_generator.h"
#include "codegen/op_expr_tree_generator.h"
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
#include "postgres.h"
#include "utils/elog.h"
#include "nodes/execnodes.h"
}

using gpcodegen::ExecEvalExprCodegen;

constexpr char ExecEvalExprCodegen::kExecEvalExprPrefix[];

class ElogWrapper {
 public:
  ElogWrapper(gpcodegen::CodegenUtils* codegen_utils) :
    codegen_utils_(codegen_utils) {
    SetupElog();
  }
  ~ElogWrapper() {
    TearDownElog();
  }

  template<typename... V>
  void CreateElog(
      llvm::Value* llvm_elevel,
      llvm::Value* llvm_fmt,
      V ... args ) {

    assert(NULL != llvm_elevel);
    assert(NULL != llvm_fmt);

    codegen_utils_->ir_builder()->CreateCall(
        llvm_elog_start_, {
            codegen_utils_->GetConstant(""), // Filename
            codegen_utils_->GetConstant(0),  // line number
            codegen_utils_->GetConstant("")  // function name
    });
    codegen_utils_->ir_builder()->CreateCall(
        llvm_elog_finish_, {
            llvm_elevel,
            llvm_fmt,
            args...
    });
  }
  template<typename... V>
  void CreateElog(
      int elevel,
      const char* fmt,
      V ... args ) {
    CreateElog(codegen_utils_->GetConstant(elevel),
               codegen_utils_->GetConstant(fmt),
               args...);
  }
 private:
  llvm::Function* llvm_elog_start_;
  llvm::Function* llvm_elog_finish_;

  gpcodegen::CodegenUtils* codegen_utils_;

  void SetupElog(){
    assert(codegen_utils_ != nullptr);
    llvm_elog_start_ = codegen_utils_->RegisterExternalFunction(elog_start);
    assert(llvm_elog_start_ != nullptr);
    llvm_elog_finish_ = codegen_utils_->RegisterExternalFunction(elog_finish);
    assert(llvm_elog_finish_ != nullptr);
  }

  void TearDownElog(){
    llvm_elog_start_ = nullptr;
    llvm_elog_finish_ = nullptr;
  }

};


ExecEvalExprCodegen::ExecEvalExprCodegen
(
    ExecEvalExprFn regular_func_ptr,
    ExecEvalExprFn* ptr_to_regular_func_ptr,
    ExprState *exprstate,
    ExprContext *econtext) :
    BaseCodegen(kExecEvalExprPrefix,
                regular_func_ptr, ptr_to_regular_func_ptr),
                exprstate_(exprstate),
                econtext_(econtext){
}


bool ExecEvalExprCodegen::GenerateExecEvalExpr(
    gpcodegen::CodegenUtils* codegen_utils) {

  assert(NULL != codegen_utils);
  if (nullptr == exprstate_ ||
      nullptr == exprstate_->expr ||
      nullptr == econtext_) {
    return false;
  }

  ElogWrapper elogwrapper(codegen_utils);
  //TODO : krajaraman move to better place
  OpExprTreeGenerator::InitializeSupportedFunction();

  llvm::Function* exec_eval_expr_func = CreateFunction<ExecEvalExprFn>(
      codegen_utils, GetUniqueFuncName());

  // Function arguments to ExecVariableList
  llvm::Value* llvm_expression_arg =
      ArgumentByPosition(exec_eval_expr_func, 0);
  llvm::Value* llvm_econtext_arg = ArgumentByPosition(exec_eval_expr_func, 1);
  llvm::Value* llvm_isnull_arg = ArgumentByPosition(exec_eval_expr_func, 2);
  llvm::Value* llvm_isDone_arg = ArgumentByPosition(exec_eval_expr_func, 3);

  // External functions
  llvm::Function* llvm_slot_getattr =
      codegen_utils->RegisterExternalFunction(slot_getattr);

  // BasicBlock of function entry.
  llvm::BasicBlock* llvm_entry_block = codegen_utils->CreateBasicBlock(
      "entry", exec_eval_expr_func);
  llvm::BasicBlock* llvm_error_block = codegen_utils->CreateBasicBlock(
        "error_block", exec_eval_expr_func);

  auto irb = codegen_utils->ir_builder();

  irb->SetInsertPoint(llvm_entry_block);

  elogwrapper.CreateElog(
        DEBUG1,
        "Calling codegen'ed expression evaluation");

  // Check if we can codegen. If so create ExprTreeGenerator
  std::unique_ptr<ExprTreeGenerator> expr_tree_generator(nullptr);
  bool can_generate = ExprTreeGenerator::VerifyAndCreateExprTree(
      exprstate_, econtext_, expr_tree_generator);
  if (!can_generate ||
      expr_tree_generator.get() == nullptr) {
    return false;
  }

  // Generate code from expression tree generator
  llvm::Value* value = nullptr;
  bool is_generated = expr_tree_generator->GenerateCode(codegen_utils,
                                                        econtext_,
                                                        exec_eval_expr_func,
                                                        llvm_error_block,
                                                        llvm_isnull_arg,
                                                        value);
  if (!is_generated ||
      nullptr == value) {
    return false;
  }

  llvm::Value* llvm_ret_value = codegen_utils->CreateCast<int64_t>(value);
  irb->CreateRet(llvm_ret_value);

  irb->SetInsertPoint(llvm_error_block);
  irb->CreateRet(codegen_utils->GetConstant<int64_t>(0));
  exec_eval_expr_func->dump();
  return true;
}


bool ExecEvalExprCodegen::GenerateCodeInternal(CodegenUtils* codegen_utils) {
  bool isGenerated = GenerateExecEvalExpr(codegen_utils);

  if (isGenerated) {
    elog(DEBUG1, "ExecEvalExpr was generated successfully!");
    return true;
  }
  else {
    elog(DEBUG1, "ExecEvalExpr generation failed!");
    return false;
  }
}
