//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    pg_func_generator.h
//
//  @doc:
//    Object to manage code generation for Greenplum's function
//
//---------------------------------------------------------------------------
#ifndef GPCODEGEN_PG_FUNC_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_PG_FUNC_GENERATOR_H_

#include <string>
#include <vector>
#include <memory>

#include "codegen/pg_func_generator_interface.h"
#include "codegen/utils/gp_codegen_utils.h"

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

typedef bool (*PGFuncGenerator)(gpcodegen::GpCodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::BasicBlock* llvm_error_block,
    const std::vector<llvm::Value*>& llvm_args,
    llvm::Value** llvm_out_value);


/**
 * @brief Base class for Greenplum Function generator.
 *
 * @tparam FuncPtrType  Function type that generate code
 * @tparam Arg0         First Argument CppType
 * @tparam Arg1         Second Argument CppType
 *
 **/
// TODO(krajaraman) : Make Variadic template to take
// variable length argument instead of two arguments
template <typename FuncPtrType, typename Arg0, typename Arg1>
class PGFuncBaseGenerator : public PGFuncGeneratorInterface {
 public:
  virtual ~PGFuncBaseGenerator() = default;
  std::string GetName() final { return pg_func_name_; };
  size_t GetTotalArgCount() final { return 2; }

 protected:
  /**
   * @brief Check for expected arguments length and cast all the
   *        argument to given type.
   *
   * @param codegen_utils Utility to easy code generation.
   * @param llvm_in_args  Vector of llvm arguments
   * @param llvm_out_args Vector of processed llvm arguments
   *
   * @return true on successful preprocess otherwise return false.
   **/
  bool PreProcessArgs(gpcodegen::GpCodegenUtils* codegen_utils,
                      const std::vector<llvm::Value*>& llvm_in_args,
                      std::vector<llvm::Value*>* llvm_out_args) {
    assert(nullptr != codegen_utils &&
           nullptr != llvm_out_args);
    if (llvm_in_args.size() != GetTotalArgCount()) {
      return false;
    }
    // Convert Datum to given cpp type.
    llvm_out_args->push_back(codegen_utils->CreateDatumToCppTypeCast
                             <Arg0>(llvm_in_args[0]));
    llvm_out_args->push_back(codegen_utils->CreateDatumToCppTypeCast
                             <Arg1>(llvm_in_args[1]));
    return true;
  }

  /**
   * @return Function Pointer that generate code
   **/
  FuncPtrType func_ptr() {
    return func_ptr_;
  }

  /**
   * @brief Constructor.
   *
   * @param pg_func_oid   Greenplum function Oid
   * @param pg_func_name  Greenplum function name
   * @param func_ptr      Function pointer that generate code
   **/
  PGFuncBaseGenerator(int pg_func_oid,
                  const std::string& pg_func_name,
                  FuncPtrType func_ptr) : pg_func_oid_(pg_func_oid),
                      pg_func_name_(pg_func_name),
                      func_ptr_(func_ptr) {
    }

 private:
  unsigned int pg_func_oid_;
  std::string pg_func_name_;
  FuncPtrType func_ptr_;
};

/**
 * @brief Object that use IRBuilder member function to generate code for
 *        given Greenplum function
 *
 * @tparam FuncPtrType  Function type that generate code
 * @tparam Arg0         First Argument CppType
 * @tparam Arg1         Second Argument CppType
 *
 **/
template <typename FuncPtrType, typename Arg0, typename Arg1>
class PGIRBuilderFuncGenerator : public PGFuncBaseGenerator<FuncPtrType,
Arg0, Arg1> {
 public:
  /**
   * @brief Constructor.
   *
   * @param pg_func_oid   Greenplum function Oid
   * @param pg_func_name  Greenplum function name
   * @param mem_func_ptr  IRBuilder member function pointer
   **/
  PGIRBuilderFuncGenerator(int pg_func_oid,
                           const std::string& pg_func_name,
                           FuncPtrType mem_func_ptr) :
                             PGFuncBaseGenerator<FuncPtrType,
                             Arg0, Arg1>(pg_func_oid,
                                             pg_func_name,
                                             mem_func_ptr) {
  }

  bool GenerateCode(gpcodegen::GpCodegenUtils* codegen_utils,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    const std::vector<llvm::Value*>& llvm_args,
                    llvm::Value** llvm_out_value) final {
    assert(nullptr != llvm_out_value);
    std::vector<llvm::Value*> llvm_preproc_args;
    if (!this->PreProcessArgs(codegen_utils, llvm_args, &llvm_preproc_args)) {
      return false;
    }
    llvm::IRBuilder<>* irb = codegen_utils->ir_builder();
    *llvm_out_value = (irb->*this->func_ptr())(llvm_preproc_args[0],
        llvm_preproc_args[1], "");
    return true;
  }
};

/**
 * @brief Object that use static member function to generate code for
 *        given Greenplum's function
 *
 * @tparam FuncPtrType  Function type that generate code
 * @tparam Arg0         First Argument CppType
 * @tparam Arg1         Second Argument CppType
 *
 **/
template <typename Arg0, typename Arg1>
class PGGenericFuncGenerator : public PGFuncBaseGenerator<PGFuncGenerator,
Arg0, Arg1> {
 public:
  /**
   * @brief Constructor.
   *
   * @param pg_func_oid   Greenplum function Oid
   * @param pg_func_name  Greenplum function name
   * @param func_ptr      function pointer to static function
   **/
  PGGenericFuncGenerator(int pg_func_oid,
                           const std::string& pg_func_name,
                           PGFuncGenerator func_ptr) :
                             PGFuncBaseGenerator<PGFuncGenerator,
                             Arg0, Arg1>(pg_func_oid,
                                             pg_func_name,
                                             func_ptr) {
  }

  bool GenerateCode(gpcodegen::GpCodegenUtils* codegen_utils,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    const std::vector<llvm::Value*>& llvm_args,
                    llvm::Value** llvm_out_value) final {
    assert(nullptr != codegen_utils &&
           nullptr != llvm_out_value);
    std::vector<llvm::Value*> llvm_preproc_args;
    if (!this->PreProcessArgs(codegen_utils, llvm_args, &llvm_preproc_args)) {
      return false;
    }
    this->func_ptr()(codegen_utils,
        llvm_main_func,
        llvm_error_block,
        llvm_preproc_args,
        llvm_out_value);
    return true;
  }
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_FUNC_GENERATOR_H_
