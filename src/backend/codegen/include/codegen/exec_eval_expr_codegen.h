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

#include "codegen/base_codegen.h"
#include "codegen/codegen_wrapper.h"
#include "codegen/expr_tree_generator.h"
#include "codegen/slot_getattr_codegen.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class ExecEvalExprCodegen: public BaseCodegen<ExecEvalExprFn> {
 public:
  /**
   * @brief Constructor
   *
   * @param regular_func_ptr        Regular version of the target function.
   * @param ptr_to_chosen_func_ptr  Reference to the function pointer that the
   *                                caller will call.
   * @param exprstate               The ExprState to use for generating code.
   * @param econtext                The ExprContext to use for generating code.
   * @param slot                    The slot to use for generating code.
   *
   * @note 	The ptr_to_chosen_func_ptr can refer to either the generated
   *        function or the corresponding regular version.
   *
   **/
  explicit ExecEvalExprCodegen(CodegenManager* manager,
                               ExecEvalExprFn regular_func_ptr,
                               ExecEvalExprFn* ptr_to_regular_func_ptr,
                               ExprState *exprstate,
                               ExprContext *econtext,
                               PlanState* plan_state);

  virtual ~ExecEvalExprCodegen() = default;

  bool InitDependencies() override;

 protected:
  /**
   * @brief Generate code for expression evaluation.
   *
   * @param codegen_utils
   *
   * @return true on successful generation; false otherwise.
   *
   * @note Walks down expression tree and create respective ExprTreeGenerator
   * to generate code.
   *
   * This implementation does not support:
   *  (1) Null attributes
   *  (2) Variable length attributes
   *
   * If at execution time, we see any of the above types of attributes,
   * we fall backs to the regular function.
   *
   */
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final;

 private:
  ExprState *exprstate_;
  PlanState* plan_state_;

  ExprTreeGeneratorInfo gen_info_;
  SlotGetAttrCodegen* slot_getattr_codegen_;
  std::unique_ptr<ExprTreeGenerator> expr_tree_generator_;

  static constexpr char kExecEvalExprPrefix[] = "ExecEvalExpr";

  /**
   * @brief Generates runtime code that implements expression evaluation.
   *
   * @param codegen_utils Utility to ease the code generation process.
   * @return true on successful generation.
   **/
  bool GenerateExecEvalExpr(gpcodegen::GpCodegenUtils* codegen_utils);

  /**
   * @brief Prepare generation of dependent slot_getattr() if necessary
   * @return true on successful generation.
   **/
  void PrepareSlotGetAttr();
};

/** @} */

}  // namespace gpcodegen
#endif  // GPCODEGEN_EXECEVALEXPR_CODEGEN_H_
