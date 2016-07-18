//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    codegen_manager.cpp
//
//  @doc:
//    Implementation of a code generator manager
//
//---------------------------------------------------------------------------
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "llvm/Support/raw_ostream.h"

#include "codegen/codegen_interface.h"
#include "codegen/codegen_manager.h"
#include "codegen/codegen_wrapper.h"
#include "codegen/utils/codegen_utils.h"
#include "codegen/utils/gp_codegen_utils.h"

using gpcodegen::CodegenManager;

CodegenManager::CodegenManager(const std::string& module_name) {
  module_name_ = module_name;
  codegen_utils_.reset(new gpcodegen::GpCodegenUtils(module_name));
}

bool CodegenManager::EnrollCodeGenerator(
    CodegenFuncLifespan funcLifespan, CodegenInterface* generator) {
  // Only CodegenFuncLifespan_Parameter_Invariant is supported as of now
  assert(funcLifespan == CodegenFuncLifespan_Parameter_Invariant);
  assert(nullptr != generator);
  enrolled_code_generators_.emplace_back(generator);
  return true;
}

unsigned int CodegenManager::GenerateCode() {
  // First, allow all code generators to initialize their dependencies
  for (size_t i = 0; i < enrolled_code_generators_.size(); ++i) {
    // NB: This list is still volatile at this time, as more generators may be
    // enrolled as we iterate to initialize dependencies.
    enrolled_code_generators_[i]->InitDependencies();
  }
  // Then ask them to generate code
  unsigned int success_count = 0;
  for (std::unique_ptr<CodegenInterface>& generator :
      enrolled_code_generators_) {
    success_count += generator->GenerateCode(codegen_utils_.get());
  }
  return success_count;
}

unsigned int CodegenManager::PrepareGeneratedFunctions() {
  unsigned int success_count = 0;

  // If no generator registered, just return with success count as 0
  if (enrolled_code_generators_.empty()) {
    return success_count;
  }


  // Call GpCodegenUtils to compile entire module
  bool compilation_status = codegen_utils_->PrepareForExecution(
      gpcodegen::GpCodegenUtils::OptimizationLevel::kDefault, true);

  if (!compilation_status) {
    return success_count;
  }

  // On successful compilation, go through all generator and swap
  // the pointer so compiled function get called
  gpcodegen::GpCodegenUtils* codegen_utils = codegen_utils_.get();
  for (std::unique_ptr<CodegenInterface>& generator :
      enrolled_code_generators_) {
    success_count += generator->SetToGenerated(codegen_utils);
  }
  return success_count;
}

void CodegenManager::NotifyParameterChange() {
  // no support for parameter change yet
  assert(false);
}

bool CodegenManager::InvalidateGeneratedFunctions() {
  // no support for invalidation of generated function
  assert(false);
  return false;
}

const std::string& CodegenManager::GetExplainString() {
  return explain_string_;
}

void CodegenManager::AccumulateExplainString() {
  explain_string_.clear();
  llvm::raw_string_ostream out(explain_string_);
  codegen_utils_->PrintUnderlyingModules(out);
}
