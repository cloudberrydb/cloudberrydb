//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    exec_eval_expr_codegen.h
//
//  @doc:
//    Headers for ExecEvalExpr codegen.
//
//---------------------------------------------------------------------------

#ifndef GPCODEGEN_EXECEVALEXPR_CODEGEN_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_EXECEVALEXPR_CODEGEN_H_

#include "codegen/codegen_wrapper.h"
#include "codegen/base_codegen.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class ExecEvalExprCodegen: public BaseCodegen<ExecEvalExprFn> {
 public:
  /**
   * @brief Constructor
   *
   * @param regular_func_ptr       Regular version of the target function.
   * @param ptr_to_chosen_func_ptr Reference to the function pointer that the caller will call.
   * @param exprstate         The ExprState to use for generating code.
   *
   * @note 	The ptr_to_chosen_func_ptr can refer to either the generated function or the
   * 			corresponding regular version.
   *
   **/
  explicit ExecEvalExprCodegen(ExecEvalExprFn regular_func_ptr,
		  ExecEvalExprFn* ptr_to_regular_func_ptr,
		  ExprState *exprstate,
		  ExprContext *econtext);

  virtual ~ExecEvalExprCodegen() = default;

 protected:
  /**
   * @brief Generate code for expression evaluation.
   *
   * @param codegen_utils
   *
   * @return true on successful generation; false otherwise.
   *
   * @note Currently, it simply falls back to regular function for expression evaluation.
   */
  bool GenerateCodeInternal(gpcodegen::CodegenUtils* codegen_utils) final;

 private:
  ExprState *exprstate_;
  ExprContext *econtext_;

  static constexpr char kExecEvalExprPrefix[] = "ExecEvalExpr";

  /**
   * @brief Generates runtime code that implements expression evaluation.
   *
   * @param codegen_utils Utility to ease the code generation process.
   * @return true on successful generation.
   **/
  bool GenerateExecEvalExpr(gpcodegen::CodegenUtils* codegen_utils);
};

/** @} */

}  // namespace gpcodegen
#endif  // GPCODEGEN_EXECEVALEXPR_CODEGEN_H_
