//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    codegen_manager.h
//
//  @doc:
//    Object that manage all CodegenInterface and GpCodegenUtils
//
//---------------------------------------------------------------------------

#ifndef GPCODEGEN_CODEGEN_MANAGER_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_CODEGEN_MANAGER_H_

#include <memory>
#include <vector>
#include <string>

#include "codegen/utils/macros.h"
#include "codegen/codegen_config.h"
#include "codegen/codegen_interface.h"
#include "codegen/base_codegen.h"

namespace gpcodegen {
/** \addtogroup gpcodegen
 *  @{
 */

// Forward declaration of GpCodegenUtils to manage llvm module
class GpCodegenUtils;

// Forward declaration of a CodegenInterface that will be managed by manager
class CodegenInterface;

/**
 * @brief Object that manages all code gen.
 **/
class CodegenManager {
 public:
  /**
   * @brief Constructor.
   *
   * @param module_name A human-readable name for the module that this
   *        CodegenManager will manage.
   **/
  explicit CodegenManager(const std::string& module_name);

  ~CodegenManager() = default;

  /**
   * @brief Template function to facilitate enroll for any type of
   *        CodegenInterface that CodegenManager wants to keep track of.
   *
   * @tparam ClassType Type of Code Generator class that derives from
   *                   CodegenInterface.
   * @tparam FuncType Type of the function pointer that CodegenManager swaps.
   * @tparam Args Variable argument that ClassType will take in its constructor
   *
   * @param manager Current Codegen Manager
   * @param regular_func_ptr Regular version of the target function.
   * @param ptr_to_chosen_func_ptr Pointer to the function pointer that the
   *                               caller will call.
   * @param args Variable length argument for ClassType
   *
   * This function creates a new code generator object of type ClassType using
   * the passed-in args, and enrolls it in the given codegen manager.
   *
   * It does not create a generator when codegen or manager is unset, or the
   * code generator ClassType is disabled (with the appropriate GUC).
   * It always initializes the given double function pointer
   * (ptr_to_chosen_func_ptr) to the regular_func_ptr.
   *
   * This transfers the ownership of the code generator to the manager.
   *
   * @return Pointer to ClassType
   **/
  template <typename ClassType, typename FuncType, typename ...Args>
  static ClassType* CreateAndEnrollGenerator(
      CodegenManager* manager,
      FuncType regular_func_ptr,
      FuncType* ptr_to_chosen_func_ptr,
      Args&&... args) {  // NOLINT(build/c++11)
    assert(nullptr != regular_func_ptr);
    assert(nullptr != ptr_to_chosen_func_ptr);

    bool can_enroll =
        // manager may be NULL if ExecInitNode/ExecProcNode weren't previously
        // called. This happens e.g during gpinitsystem.
        (nullptr != manager) &&
        codegen &&  // if codegen guc is false
        // if generator is disabled
        CodegenConfig::IsGeneratorEnabled<ClassType>();
    if (!can_enroll) {
      gpcodegen::BaseCodegen<FuncType>::SetToRegular(
          regular_func_ptr, ptr_to_chosen_func_ptr);
      return nullptr;
    }

    ClassType* generator = new ClassType(
        manager,
        regular_func_ptr,
        ptr_to_chosen_func_ptr,
        std::forward<Args>(args)...);
    bool is_enrolled = manager->EnrollCodeGenerator(
        CodegenFuncLifespan_Parameter_Invariant, generator);
    assert(is_enrolled);
    return generator;
  }

  /**
   * @brief Enroll a code generator with manager
   *
   * @note Manager manages the memory of enrolled generator.
   *
   * @param funcLifespan Life span of the enrolling generator. Based on life span,
   *                     corresponding GpCodegenUtils will be used for code generation
   * @param generator    Generator that needs to be enrolled with manager.
   * @return true on successful enrollment.
   **/
  bool EnrollCodeGenerator(CodegenFuncLifespan funcLifespan,
                           CodegenInterface* generator);

  /**
   * @brief Request all enrolled generators to generate code.
   *
   * @return The number of enrolled codegen that successfully generated code.
   **/
  unsigned int GenerateCode();

  /**
   * @brief Compile all the generated functions. On success,
   *        a pointer to the generated method becomes available to the caller.
   *
   * @return The number of enrolled codegen that successully generated code
   *         and 0 on failure
   **/
  unsigned int PrepareGeneratedFunctions();

  /**
   * @brief 	Notifies the manager of a parameter change.
   *
   * @note 	This is called during a ReScan or other parameter change process.
   * 			Upon receiving this notification the manager may invalidate all the
   * 			generated code that depend on parameters.
   *
   **/
  void NotifyParameterChange();

  /**
   * @brief Invalidate all generated functions.
   *
   * @return true if successfully invalidated.
   **/
  bool InvalidateGeneratedFunctions();

  /**
   * @return Number of enrolled generators.
   **/
  size_t GetEnrollmentCount() {
    return enrolled_code_generators_.size();
  }

  /*
   * @brief Accumulate the explain string with a dump of all the underlying LLVM
   *        modules
   */
  void AccumulateExplainString();

  /*
   * @brief Return the previous accumulated explain string
   */
  const std::string& GetExplainString();

 private:
  // GpCodegenUtils provides a facade to LLVM subsystem.
  std::unique_ptr<gpcodegen::GpCodegenUtils> codegen_utils_;

  std::string module_name_;

  // List of all enrolled code generators.
  std::vector<std::unique_ptr<CodegenInterface>> enrolled_code_generators_;

  // Holds the dumped IR of all underlying modules for EXPLAIN CODEGEN queries
  std::string explain_string_;

  DISALLOW_COPY_AND_ASSIGN(CodegenManager);
};

/** @} */

}  // namespace gpcodegen
#endif  // GPCODEGEN_CODEGEN_MANAGER_H_
