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

  ElogWrapper elogwrapper(codegen_utils);

  llvm::Function* exec_eval_expr_func = codegen_utils->
      CreateFunction<ExecEvalExprFn>(
          GetUniqueFuncName());

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
  llvm::BasicBlock* entry_block = codegen_utils->CreateBasicBlock(
      "entry", exec_eval_expr_func);
  llvm::BasicBlock* return_true_block = codegen_utils->CreateBasicBlock(
      "return_true", exec_eval_expr_func);
  llvm::BasicBlock* return_false_block = codegen_utils->CreateBasicBlock(
      "return_false", exec_eval_expr_func);

  auto irb = codegen_utils->ir_builder();

  irb->SetInsertPoint(entry_block);

  if (exprstate_ && exprstate_->expr && econtext_) {
    // Codegen Operation expression
    Expr *expr_ = exprstate_->expr;
    if (nodeTag(expr_) != T_OpExpr) {
      elog(DEBUG1, "Unsupported expression. Support only T_OpExpr");
      return false;
    }

    // In ExecEvalOper
    OpExpr *op = (OpExpr *) expr_;

    if (op->opno != 523 /* "<=" */ && op->opno != 1096 /* date "<=" */) {
      // Operators are stored in pg_proc table. See postgres.bki for more details.
      elog(DEBUG1, "Unsupported operator %d.", op->opno);
      return false;
    }

    // In ExecMakeFunctionResult
    // retrieve operator's arguments
    List *arguments = ((FuncExprState *)exprstate_)->args;
    // In ExecEvalFuncArgs
    if (list_length(arguments) != 2) {
      elog(DEBUG1, "Wrong number of arguments (!= 2)");
      return false;
    }

    ListCell   *arg;
    int arg_num = 0;
    llvm::Value *llvm_arg_val[2];

    foreach(arg, arguments)
    {
      // retrieve argument's ExprState
      ExprState  *argstate = (ExprState *) lfirst(arg);

      // Currently we support only variable and constant arguments
      if (nodeTag(argstate->expr) == T_Var) {
        // In ExecEvalVar
        Var *variable = (Var *) argstate->expr;
        int attnum = variable->varattno;

        // slot = econtext->ecxt_scantuple; {{{
        // At code generation time, slot is NULL.
        // For that reason, we keep a double pointer to slot and at execution time
        // we load slot.
        TupleTableSlot **ptr_to_slot_ptr = NULL;
        switch (variable->varno)
        {
          case INNER:  /* get the tuple from the inner node */
            ptr_to_slot_ptr = &econtext_->ecxt_innertuple;
            break;

          case OUTER:  /* get the tuple from the outer node */
            ptr_to_slot_ptr = &econtext_->ecxt_outertuple;
            break;

          default:     /* get the tuple from the relation being scanned */
            ptr_to_slot_ptr = &econtext_->ecxt_scantuple;
            break;
        }

        llvm::Value *llvm_slot = irb->CreateLoad(
            codegen_utils->GetConstant(ptr_to_slot_ptr));
        //}}}

        llvm::Value *llvm_variable_varattno = codegen_utils->
            GetConstant<int4>(variable->varattno);

        // retrieve variable
        llvm_arg_val[arg_num] = codegen_utils->ir_builder()->CreateCall(
            llvm_slot_getattr, {
                llvm_slot,
                llvm_variable_varattno,
                llvm_isnull_arg /* TODO: Fix isNull */
        });

      }
      else if (nodeTag(argstate->expr) == T_Const) {
        // In ExecEvalConst
        Const *con = (Const *) argstate->expr;
        llvm_arg_val[arg_num] = codegen_utils->GetConstant(con->constvalue);
      }
      else {
        elog(DEBUG1, "Unsupported argument type.");
        return false;
      }

      arg_num++;
    }

    if (op->opno == 523) {
      // Execute **int4le**
      irb->CreateCondBr(
          irb->CreateICmpSLE(llvm_arg_val[0], llvm_arg_val[1]),
          return_true_block /* true */,
          return_false_block /* false */);
    }
    else { // op->opno == 1096
      // Execute **date_le**
      // Similar to int4le but we trunc the values to i32 from i64
      irb->CreateCondBr(irb->CreateICmpSLE(
          irb->CreateTrunc(llvm_arg_val[0], codegen_utils->GetType<int32>()),
          irb->CreateTrunc(llvm_arg_val[1], codegen_utils->GetType<int32>())),
                        return_true_block /* true */,
                        return_false_block /* false */);
    }

    irb->SetInsertPoint(return_true_block);
    irb->CreateRet(codegen_utils->GetConstant<int64>(1));

    irb->SetInsertPoint(return_false_block);
    irb->CreateRet(codegen_utils->GetConstant<int64>(0));

    return true;
  }

  return false;
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
