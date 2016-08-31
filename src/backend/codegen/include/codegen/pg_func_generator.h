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

// Helper template classes for gp_func_generators nested in here
namespace pg_func_generator_detail {

// PGFuncArgPreProcessor is a variadic template to assist in pre-processing a
// variable number of arguments to PGFuncBaseGenerator. Each specialization has
// a method called 'PreProcessArgs' that takes a pointer to 'CodegenUtils', an
// input vector of 'llvm:Values*' and another vector of 'llvm::Values*' that it
// populates by pre-processing the input vector. During pre-processsing, we
// generate LLVM instructions to convert the input 'llvm::Value*' values (that
// are expected to be of the type 'Datum') to the appropriate CppType from the
// variadic template list, and populate the output vector with coverted
// 'llvm::Values*' in the same order as input.
//
// e.g. PGFuncArgPreProcessor<int, bool>::PreProcessArgs(cu, vin, vout) will
// create instructions to convert vin[0] to int and vin[1] to bool and populate
// 'vout'.
template <typename... ArgumentTypes>
class PGFuncArgPreProcessor;

// Base version for zero argument types does nothing.
template <>
class PGFuncArgPreProcessor<> {
 private:
  static bool PreProcessArgsInternal(
      gpcodegen::GpCodegenUtils* codegen_utils,
      std::vector<llvm::Value*>::const_iterator llvm_in_args_iter,
      std::vector<llvm::Value*>* llvm_out_args) {
    return true;
  }

  // Make all template specializations of PGFuncArgPreProcessor friends
  template <typename ... T>
  friend class PGFuncArgPreProcessor;
};

// Version for 1+ arguments that recurses
template <typename HeadType, typename ... TailTypes>
class PGFuncArgPreProcessor<HeadType, TailTypes...> {
 public:
  static bool PreProcessArgs(
        gpcodegen::GpCodegenUtils* codegen_utils,
        const std::vector<llvm::Value*>& llvm_in_args,
        std::vector<llvm::Value*>* llvm_out_args) {
    assert(nullptr != codegen_utils &&
           nullptr != llvm_out_args);


    if (llvm_in_args.size() != 1 /* HeadType */ + sizeof...(TailTypes)) {
      return false;
    }

    return PreProcessArgsInternal(
        codegen_utils, llvm_in_args.begin(), llvm_out_args);
  }

 private:
  static bool PreProcessArgsInternal(
      gpcodegen::GpCodegenUtils* codegen_utils,
      std::vector<llvm::Value*>::const_iterator llvm_in_args_iter,
      std::vector<llvm::Value*>* llvm_out_args) {
    assert(nullptr != codegen_utils);
    assert(nullptr != llvm_out_args);
    assert(nullptr != *llvm_in_args_iter);

    assert(codegen_utils->GetType<Datum>() == (*llvm_in_args_iter)->getType());

    // Convert Datum to given cpp type.
    llvm_out_args->push_back(
        codegen_utils->CreateDatumToCppTypeCast<HeadType>(*llvm_in_args_iter));
    return PGFuncArgPreProcessor<TailTypes...>::PreProcessArgsInternal(
        codegen_utils, ++llvm_in_args_iter, llvm_out_args);
  }

  // Make all template specializations of PGFuncArgPreProcessor friends
  template <typename ... T>
  friend class PGFuncArgPreProcessor;
};

}  // namespace pg_func_generator_detail


/** \addtogroup gpcodegen
 *  @{
 */

typedef bool (*PGFuncGenerator)(gpcodegen::GpCodegenUtils* codegen_utils,
    const PGFuncGeneratorInfo& pg_func_info,
    llvm::Value** llvm_out_value);


/**
 * @brief Object that use an IRBuilder member function that to generate code for
 *        given binary GPDB function
 *
 * @tparam ReturnType   C++ type of return type of the GPDB function
 * @tparam Arg0         C++ type of 1st argument of the GPDB function
 * @tparam Arg1         C++ type of 2nd argument of the GPDB function
 *
 **/
template <typename ReturnType, typename Arg0, typename Arg1>
class PGIRBuilderFuncGenerator
    : public PGFuncGeneratorInterface {
 public:
  // Type of the IRBuilder function we need to call
  using IRBuilderFuncPtrType = llvm::Value* (llvm::IRBuilder<>::*) (
      llvm::Value*, llvm::Value*, const llvm::Twine&);

  /**
   * @brief Constructor.
   *
   * @param pg_func_oid   Greenplum function Oid
   * @param pg_func_name  Greenplum function name
   * @param mem_func_ptr  IRBuilder member function pointer
   **/
  PGIRBuilderFuncGenerator(int pg_func_oid,
                           const std::string& pg_func_name,
                           IRBuilderFuncPtrType mem_func_ptr)
  : pg_func_oid_(pg_func_oid),
    pg_func_name_(pg_func_name),
    func_ptr_(mem_func_ptr) {
  }

  std::string GetName() final {
    return pg_func_name_;
  }

  size_t GetTotalArgCount() final {
    // Support Binary IRBuilder functions only
    return 2;
  }

  bool GenerateCode(gpcodegen::GpCodegenUtils* codegen_utils,
                    const PGFuncGeneratorInfo& pg_func_info,
                    llvm::Value** llvm_out_value) final {
    assert(nullptr != llvm_out_value);
    std::vector<llvm::Value*> llvm_preproc_args;

    if (!pg_func_generator_detail::PGFuncArgPreProcessor<Arg0, Arg1>::
        PreProcessArgs(
            codegen_utils, pg_func_info.llvm_args, &llvm_preproc_args)) {
      return false;
    }

    llvm::IRBuilder<>* irb = codegen_utils->ir_builder();
    *llvm_out_value = (irb->*this->func_ptr_)(llvm_preproc_args[0],
        llvm_preproc_args[1], "");
    assert((*llvm_out_value)->getType() ==
           codegen_utils->GetType<ReturnType>());
    return true;
  }

 private:
  int pg_func_oid_;
  std::string pg_func_name_;
  IRBuilderFuncPtrType func_ptr_;
};

/**
 * @brief Object that uses a static member function to generate code for
 *        given Greenplum's function
 *
 * @tparam FuncPtrType  Function type that generate code
 * @tparam Args         Argument CppTypes
 *
 **/
template <typename ReturnType, typename ... Args>
class PGGenericFuncGenerator : public  PGFuncGeneratorInterface {
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
                         PGFuncGenerator func_ptr)
  : pg_func_oid_(pg_func_oid),
    pg_func_name_(pg_func_name),
    func_ptr_(func_ptr) {
  }

  std::string GetName() final {
    return pg_func_name_;
  }

  size_t GetTotalArgCount() final {
    return sizeof...(Args);
  }

  bool GenerateCode(gpcodegen::GpCodegenUtils* codegen_utils,
                    const PGFuncGeneratorInfo& pg_func_info,
                    llvm::Value** llvm_out_value) final {
    assert(nullptr != codegen_utils);
    assert(nullptr != llvm_out_value);
    std::vector<llvm::Value*> llvm_preproc_args;

    if (!pg_func_generator_detail::PGFuncArgPreProcessor<Args...>::
        PreProcessArgs(
            codegen_utils, pg_func_info.llvm_args, &llvm_preproc_args)) {
      return false;
    }

    PGFuncGeneratorInfo pg_processed_func_info(pg_func_info.llvm_main_func,
                                               pg_func_info.llvm_error_block,
                                               llvm_preproc_args);
    this->func_ptr_(codegen_utils,
        pg_processed_func_info,
        llvm_out_value);

    assert((*llvm_out_value)->getType() ==
           codegen_utils->GetType<ReturnType>());
    return true;
  }

 private:
  int pg_func_oid_;
  const std::string& pg_func_name_;
  PGFuncGenerator func_ptr_;
};



/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_FUNC_GENERATOR_H_
