//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    base_codegen.h
//
//  @doc:
//    Base class for expression tree to generate code
//
//---------------------------------------------------------------------------
#ifndef GPCODEGEN_PG_FUNC_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_PG_FUNC_GENERATOR_H_

#include <string>
#include <vector>
#include <memory>

#include "codegen/pg_func_generator_interface.h"
#include "codegen/utils/codegen_utils.h"

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

typedef bool (*ExprCodeGenerator)(gpcodegen::CodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::BasicBlock* llvm_error_block,
    std::vector<llvm::Value*>& llvm_args,
    llvm::Value* & llvm_out_value);



template <typename FuncPtrType, typename Arg0, typename Arg1>
class PGFuncGenerator : public PGFuncGeneratorInterface {
 public:
  std::string GetName() final { return pg_func_name_; };
  size_t GetTotalArgCount() final { return 2; }

 protected:

  // TODO : krajaraman
  // For now treating all argument. Later make template to take
  // variable length instead of two arguments
  bool PreProcessArgs(gpcodegen::CodegenUtils* codegen_utils,
                     std::vector<llvm::Value*>& llvm_in_args,
                     std::vector<llvm::Value*>& llvm_out_args) {
    assert(nullptr != codegen_utils);
    if (llvm_in_args.size() != GetTotalArgCount()) {
      return false;
    }
    llvm_out_args.push_back(codegen_utils->CreateCast<Arg0>(llvm_in_args[0]));
    llvm_out_args.push_back(codegen_utils->CreateCast<Arg1>(llvm_in_args[1]));
    return true;
  }

  FuncPtrType func_ptr() {
    return func_ptr_;
  }

  PGFuncGenerator(int pg_func_oid,
                  const std::string& pg_func_name,
                  FuncPtrType mem_func_ptr) : pg_func_oid_(pg_func_oid),
                      pg_func_name_(pg_func_name),
                      func_ptr_(mem_func_ptr) {

    }
 private:
  std::string pg_func_name_;
  unsigned int pg_func_oid_;
  FuncPtrType func_ptr_;

};

template <typename FuncPtrType, typename Arg0, typename Arg1>
class PGIRBuilderFuncGenerator : public PGFuncGenerator<FuncPtrType,
Arg0, Arg1> {
 public:
  PGIRBuilderFuncGenerator(int pg_func_oid,
                           const std::string& pg_func_name,
                           FuncPtrType func_ptr) :
                             PGFuncGenerator<FuncPtrType,
                             Arg0, Arg1>(pg_func_oid,
                                             pg_func_name,
                                             func_ptr) {

  }

  bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    std::vector<llvm::Value*>& llvm_args,
                    llvm::Value* & llvm_out_value) final {
    std::vector<llvm::Value*> llvm_preproc_args;
    if (!this->PreProcessArgs(codegen_utils, llvm_args, llvm_preproc_args)) {
      return false;
    }
    llvm::IRBuilder<>* irb = codegen_utils->ir_builder();
    llvm_out_value = (irb->*this->func_ptr())(llvm_preproc_args[0],
        llvm_preproc_args[1], "");
    return true;
  }
};

template <typename Arg0, typename Arg1>
class PGGenericFuncGenerator : public PGFuncGenerator<ExprCodeGenerator,
Arg0, Arg1> {
 public:
  PGGenericFuncGenerator(int pg_func_oid,
                           const std::string& pg_func_name,
                           ExprCodeGenerator func_ptr) :
                             PGFuncGenerator<ExprCodeGenerator,
                             Arg0, Arg1>(pg_func_oid,
                                             pg_func_name,
                                             func_ptr) {

  }

  bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    std::vector<llvm::Value*>& llvm_args,
                    llvm::Value* & llvm_out_value) final {
    assert(nullptr != codegen_utils);
    if (llvm_args.size() != this->GetTotalArgCount()) {
      return false;
    }
    this->func_ptr()(codegen_utils,
        llvm_main_func,
        llvm_error_block,
        llvm_args,
        llvm_out_value);
    return true;
  }
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_FUNC_GENERATOR_H_
