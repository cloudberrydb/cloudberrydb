//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright 2016 Pivotal Software, Inc.
//
//  @filename:
//    gp_codegen_utils.h
//
//  @doc:
//    Object that extends the functionality of CodegenUtils by adding GPDB
//    specific functionality and utilities to aid in the runtime code generation
//    for a LLVM module
//
//  @test:
//
//
//---------------------------------------------------------------------------

#ifndef GPCODEGEN_GP_CODEGEN_UTILS_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_GP_CODEGEN_UTILS_H_

#include "codegen/utils/codegen_utils.h"

extern "C" {
#include "utils/elog.h"
}

namespace gpcodegen {

class GpCodegenUtils : public CodegenUtils {
 public:
  /**
   * @brief Constructor.
   *
   * @param module_name A human-readable name for the module that this
   *        CodegenUtils will manage.
   **/
  explicit GpCodegenUtils(llvm::StringRef module_name)
  : CodegenUtils(module_name) {
  }

  ~GpCodegenUtils() {
  }

  /*
   * @brief Create LLVM instructions to call elog_start() and elog_finish().
   *
   * @warning This method does not create instructions for any sort of exception
   *          handling. In the case that elog throws an error, the code with jump
   *          straight out of the compiled module back to the last location in GPDB
   *          that setjump was called.
   *
   * @param llvm_elevel llvm::Value pointer to an integer representing the error level
   * @param llvm_fmt    llvm::Value pointer to the format string
   * @tparam args       llvm::Value pointers to arguments to elog()
   */
  template<typename... V>
  void CreateElog(
      llvm::Value* llvm_elevel,
      llvm::Value* llvm_fmt,
      const V ... args ) {
    assert(NULL != llvm_elevel);
    assert(NULL != llvm_fmt);

    llvm::Function* llvm_elog_start =
        GetOrRegisterExternalFunction(elog_start);
    llvm::Function* llvm_elog_finish =
        GetOrRegisterExternalFunction(elog_finish);

    ir_builder()->CreateCall(
        llvm_elog_start, {
            GetConstant(""),   // Filename
            GetConstant(0),    // line number
            GetConstant("")    // function name
        });
    ir_builder()->CreateCall(
        llvm_elog_finish, {
            llvm_elevel,
            llvm_fmt,
            args...
        });
  }

  /*
   * @brief Create LLVM instructions to call elog_start() and elog_finish().
   *        A convenient alternative that automatically converts an integer elevel and
   *        format string to LLVM constants.
   *
   * @warning This method does not create instructions for any sort of exception
   *          handling. In the case that elog throws an error, the code with jump
   *          straight out of the compiled module back to the last location in GPDB
   *          that setjump was called.
   *
   * @param elevel Integer representing the error level
   * @param fmt    Format string
   * @tparam args  llvm::Value pointers to arguments to elog()
   */
  template<typename... V>
    void CreateElog(
        int elevel,
        const char* fmt,
        const V ... args ) {
    CreateElog(GetConstant(elevel), GetConstant(fmt), args...);
  }
};

}  // namespace gpcodegen

#endif  // GPCODEGEN_GP_CODEGEN_UTILS_H
// EOF
