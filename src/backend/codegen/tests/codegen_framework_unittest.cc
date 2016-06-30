//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright 2016 Pivotal Software, Inc.
//
//  @filename:
//    code_generator_unittest.cc
//
//  @doc:
//    Unit tests for codegen_manager.cc
//
//  @test:
//
//---------------------------------------------------------------------------

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

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
#include "llvm/IR/Intrinsics.h"
#include "llvm/Support/Casting.h"

extern "C" {
    #include "utils/elog.h"
    #undef elog
    #define elog(...)
}

#include "codegen/utils/codegen_utils.h"
#include "codegen/utils/utility.h"
#include "codegen/codegen_manager.h"
#include "codegen/codegen_wrapper.h"
#include "codegen/codegen_interface.h"
#include "codegen/base_codegen.h"

extern bool codegen_validate_functions;
namespace gpcodegen {

typedef int (*SumFunc) (int x, int y);
typedef void (*UncompilableFunc)(int x);
typedef int (*MulFunc) (int x, int y);

int SumFuncRegular(int x, int y) {
  return x + y;
}

void UncompilableFuncRegular(int x) {
  return;
}

int MulFuncRegular(int x, int y) {
  return x * y;
}

SumFunc sum_func_ptr = nullptr;
SumFunc failed_func_ptr = nullptr;
UncompilableFunc uncompilable_func_ptr = nullptr;
MulFunc mul_func_ptr = nullptr;

class SumCodeGenerator : public BaseCodegen<SumFunc> {
 public:
  explicit SumCodeGenerator(SumFunc regular_func_ptr,
                             SumFunc* ptr_to_regular_func_ptr) :
                             BaseCodegen(kAddFuncNamePrefix,
                                         regular_func_ptr,
                                         ptr_to_regular_func_ptr) {
  }

  virtual ~SumCodeGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
    llvm::Function* add2_func
       = CreateFunction<SumFunc>(codegen_utils, GetUniqueFuncName());
    llvm::BasicBlock* add2_body = codegen_utils->CreateBasicBlock("body",
                                                                   add2_func);
    codegen_utils->ir_builder()->SetInsertPoint(add2_body);
    llvm::Value* add2_sum = codegen_utils->ir_builder()->CreateAdd(
       ArgumentByPosition(add2_func, 0),
       ArgumentByPosition(add2_func, 1));
    codegen_utils->ir_builder()->CreateRet(add2_sum);

    return true;
  }

 public:
  static constexpr char kAddFuncNamePrefix[] = "SumFunc";
};

class MulOverflowCodeGenerator : public BaseCodegen<MulFunc> {
 public:
  explicit MulOverflowCodeGenerator(MulFunc regular_func_ptr,
                                    MulFunc* ptr_to_regular_func_ptr) :
                                    BaseCodegen(kMulFuncNamePrefix,
                                         regular_func_ptr,
                                         ptr_to_regular_func_ptr) {
  }

  virtual ~MulOverflowCodeGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
    llvm::Function* mul2_func
       = CreateFunction<MulFunc>(codegen_utils, GetUniqueFuncName());
    llvm::BasicBlock* mul2_body = codegen_utils->CreateBasicBlock("body",
                                                                   mul2_func);
    llvm::BasicBlock* result_result_block = codegen_utils->CreateBasicBlock(
           "return_result", mul2_func);
    llvm::BasicBlock* return_overflow_block = codegen_utils->CreateBasicBlock(
           "return_overflow", mul2_func);

    codegen_utils->ir_builder()->SetInsertPoint(mul2_body);
    llvm::Value* arg0 = ArgumentByPosition(mul2_func, 0);
    llvm::Value* arg1 = ArgumentByPosition(mul2_func, 1);

    llvm::Function* llvm_mul_overflow = llvm::Intrinsic::getDeclaration(
        codegen_utils->module(),
        llvm::Intrinsic::smul_with_overflow,
        arg0->getType());
    llvm::Value* llvm_umul_results = codegen_utils->ir_builder()->CreateCall(
        llvm_mul_overflow, {arg0, arg1});
    llvm::Value* llvm_overflow_flag = codegen_utils->ir_builder()->
        CreateExtractValue(llvm_umul_results, 1);
    llvm::Value* llvm_results = codegen_utils->ir_builder()->CreateExtractValue(
        llvm_umul_results, 0);

    codegen_utils->ir_builder()->CreateCondBr(
        codegen_utils->ir_builder()->CreateICmpSLE(
            llvm_overflow_flag, codegen_utils->GetConstant<bool>(true)),
            return_overflow_block,
            result_result_block);

    codegen_utils->ir_builder()->SetInsertPoint(return_overflow_block);
    codegen_utils->ir_builder()->CreateRet(codegen_utils->GetConstant(1));

    codegen_utils->ir_builder()->SetInsertPoint(result_result_block);
    codegen_utils->ir_builder()->CreateRet(llvm_results);
    mul2_func->dump();
    return true;
  }

 public:
  static constexpr char kMulFuncNamePrefix[] = "MulOverflowFunc";
};

class FailingCodeGenerator : public BaseCodegen<SumFunc> {
 public:
  explicit FailingCodeGenerator(SumFunc regular_func_ptr,
                                SumFunc* ptr_to_regular_func_ptr):
                                BaseCodegen(kFailingFuncNamePrefix,
                                            regular_func_ptr,
                                            ptr_to_regular_func_ptr) {
  }

  virtual ~FailingCodeGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
     return false;
  }

 private:
  static constexpr char kFailingFuncNamePrefix[] = "SumFuncFailing";
};

template <bool GEN_SUCCESS>
class UncompilableCodeGenerator : public BaseCodegen<UncompilableFunc> {
 public:
  explicit UncompilableCodeGenerator(
      UncompilableFunc regular_func_ptr,
      UncompilableFunc* ptr_to_regular_func_ptr)
  : BaseCodegen(kUncompilableFuncNamePrefix,
                regular_func_ptr,
                ptr_to_regular_func_ptr) {
  }

  virtual ~UncompilableCodeGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
    llvm::Function* dummy_func
                = CreateFunction<UncompilableFunc>(codegen_utils,
                    GetUniqueFuncName());
    llvm::BasicBlock* dummy_func_body = codegen_utils->CreateBasicBlock("body",
                                                                  dummy_func);
    codegen_utils->ir_builder()->SetInsertPoint(dummy_func_body);
    llvm::Value* int_value = codegen_utils->GetConstant(4);
    codegen_utils->ir_builder()->CreateRet(int_value);
    return GEN_SUCCESS;
  }

 private:
  static constexpr char kUncompilableFuncNamePrefix[] = "UncompilableFunc";
};

constexpr char SumCodeGenerator::kAddFuncNamePrefix[];
constexpr char FailingCodeGenerator::kFailingFuncNamePrefix[];
constexpr char MulOverflowCodeGenerator::kMulFuncNamePrefix[];
template <bool GEN_SUCCESS>
constexpr char
UncompilableCodeGenerator<GEN_SUCCESS>::kUncompilableFuncNamePrefix[];

// Test environment to handle global per-process initialization tasks for all
// tests.
class CodegenManagerTestEnvironment : public ::testing::Environment {
 public:
  virtual void SetUp() {
    ASSERT_EQ(InitCodegen(), 1);
  }
};

class CodegenManagerTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    manager_.reset(new CodegenManager("CodegenManagerTest"));
    codegen_validate_functions = true;
  }

  template <typename ClassType, typename FuncType>
  void EnrollCodegen(FuncType reg_func, FuncType* ptr_to_chosen_func) {
    ClassType* code_gen = new ClassType(reg_func, ptr_to_chosen_func);
    ASSERT_TRUE(reg_func == *ptr_to_chosen_func);
    ASSERT_TRUE(manager_->EnrollCodeGenerator(
        CodegenFuncLifespan_Parameter_Invariant,
        code_gen));
  }

  std::unique_ptr<CodegenManager> manager_;
};

TEST_F(CodegenManagerTest, TestGetters) {
  sum_func_ptr = nullptr;
  SumCodeGenerator* code_gen = new SumCodeGenerator(SumFuncRegular,
                                                    &sum_func_ptr);

  EXPECT_EQ(SumCodeGenerator::kAddFuncNamePrefix,
            code_gen->GetOrigFuncName());
  // Static unique_counter will be zero to begin with.
  // So uniqueFuncName return with suffix zero.
  EXPECT_EQ(SumCodeGenerator::kAddFuncNamePrefix + std::to_string(0),
            code_gen->GetUniqueFuncName());
  ASSERT_TRUE(manager_->EnrollCodeGenerator(
      CodegenFuncLifespan_Parameter_Invariant, code_gen));
  ASSERT_FALSE(code_gen->IsGenerated());
  EXPECT_EQ(1, manager_->GenerateCode());
  ASSERT_TRUE(code_gen->IsGenerated());
}

TEST_F(CodegenManagerTest, EnrollCodeGeneratorTest) {
  sum_func_ptr = nullptr;
  EnrollCodegen<SumCodeGenerator, SumFunc>(SumFuncRegular, &sum_func_ptr);
  EXPECT_EQ(1, manager_->GetEnrollmentCount());
}

TEST_F(CodegenManagerTest, GenerateCodeTest) {
  // With no generator it should return false
  EXPECT_EQ(0, manager_->GenerateCode());

  // Test if generation happens successfully
  sum_func_ptr = nullptr;
  EnrollCodegen<SumCodeGenerator, SumFunc>(SumFuncRegular, &sum_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Test if generation fails with FailingCodeGenerator
  failed_func_ptr = nullptr;
  EnrollCodegen<FailingCodeGenerator, SumFunc>(SumFuncRegular,
                                               &failed_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Test if generation pass with UncompiledCodeGenerator
  uncompilable_func_ptr = nullptr;
  EnrollCodegen<UncompilableCodeGenerator<true>, UncompilableFunc>(
      UncompilableFuncRegular, &uncompilable_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());
}

TEST_F(CodegenManagerTest, PrepareGeneratedFunctionsNoCompilationErrorTest) {
  // Test if generation happens successfully
  sum_func_ptr = nullptr;
  EnrollCodegen<SumCodeGenerator, SumFunc>(SumFuncRegular, &sum_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Test if generation fails with FailingCodeGenerator
  failed_func_ptr = nullptr;
  EnrollCodegen<FailingCodeGenerator, SumFunc>(SumFuncRegular,
                                               &failed_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Make sure both the function pointers refer to regular versions
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);

  // This should update function pointers to generated version,
  // if generation was successful
  ASSERT_TRUE(manager_->PrepareGeneratedFunctions());

  // For sum_func_ptr, we successfully generated code.
  // So, pointer should reflect that.
  ASSERT_TRUE(SumFuncRegular != sum_func_ptr);

  // For failed_func_ptr, code generation was unsuccessful.
  // So, pointer should not change.
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);

  // Check generate SumFuncRegular works as expected;
  EXPECT_EQ(3, sum_func_ptr(1, 2));

  // Reset the manager, so that all the code generators go away
  manager_.reset(nullptr);

  // The manager reset should have restored all the function pointers
  // to point to regular version
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
}

TEST_F(CodegenManagerTest, UnCompilableFailedGenerationTest) {
  // Test if generation happens successfully
  sum_func_ptr = nullptr;
  EnrollCodegen<SumCodeGenerator, SumFunc>(SumFuncRegular, &sum_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Test if generation fails with FailingCodeGenerator
  failed_func_ptr = nullptr;
  EnrollCodegen<FailingCodeGenerator, SumFunc>(SumFuncRegular,
                                               &failed_func_ptr);

  // Create uncompilable generator which fails in generation
  // and produce broken function
  uncompilable_func_ptr = nullptr;
  EnrollCodegen<UncompilableCodeGenerator<false>, UncompilableFunc>(
      UncompilableFuncRegular, &uncompilable_func_ptr);

  EXPECT_EQ(1, manager_->GenerateCode());

  // Make sure the function pointers refer to regular versions
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);

  // This should update function pointers to generated version,
  // if generation was successful
  ASSERT_TRUE(manager_->PrepareGeneratedFunctions());

  // For sum_func_ptr, we successfully generated code.
  // So, pointer should reflect that.
  ASSERT_TRUE(SumFuncRegular != sum_func_ptr);

  // For failed_func_ptr, code generation was unsuccessful.
  // So, pointer should not change.
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);

  // For uncompilable_func_ptr, code generation was unsuccessful.
  // So, pointer should not change.
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);

  // Check generate SumFuncRegular works as expected;
  EXPECT_EQ(3, sum_func_ptr(1, 2));

  // Reset the manager, so that all the code generators go away
  manager_.reset(nullptr);

  // The manager reset should have restored all the function pointers
  // to point to regular version
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);
}

TEST_F(CodegenManagerTest, UnCompilablePassedGenerationTest) {
  // Test if generation happens successfully
  sum_func_ptr = nullptr;
  EnrollCodegen<SumCodeGenerator, SumFunc>(SumFuncRegular, &sum_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());

  // Test if generation fails with FailingCodeGenerator
  failed_func_ptr = nullptr;
  EnrollCodegen<FailingCodeGenerator, SumFunc>(SumFuncRegular,
                                               &failed_func_ptr);

  // Create uncompilable generator which generate broken
  // function and return success status on generation
  uncompilable_func_ptr = nullptr;
  EnrollCodegen<UncompilableCodeGenerator<true>, UncompilableFunc>(
      UncompilableFuncRegular, &uncompilable_func_ptr);

  EXPECT_EQ(1, manager_->GenerateCode());

  // Make sure both the function pointers refer to regular versions
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);

  EXPECT_EQ(1, manager_->PrepareGeneratedFunctions());

  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);

  // Reset the manager, so that all the code generators go away
  manager_.reset(nullptr);

  // The manager reset should have restored all the function pointers
  // to point to regular version
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
  ASSERT_TRUE(SumFuncRegular == failed_func_ptr);
  ASSERT_TRUE(UncompilableFuncRegular == uncompilable_func_ptr);
}

TEST_F(CodegenManagerTest, MulOverFlowTest) {
  // Test if generation happens successfully
  mul_func_ptr = nullptr;
  EnrollCodegen<MulOverflowCodeGenerator, MulFunc>(MulFuncRegular,
                                                   &mul_func_ptr);
  EXPECT_EQ(1, manager_->GenerateCode());


  ASSERT_TRUE(MulFuncRegular == mul_func_ptr);

  // This should cause program to exit because of
  // broken function
  EXPECT_EQ(1, manager_->PrepareGeneratedFunctions());

  EXPECT_EQ(6, mul_func_ptr(2, 3));

  EXPECT_EQ(1, mul_func_ptr(2147483647, 3));

  EXPECT_EQ(-6, mul_func_ptr(-2, 3));

  // Reset the manager, so that all the code generators go away
  manager_.reset(nullptr);
}

TEST_F(CodegenManagerTest, ResetTest) {
  sum_func_ptr = nullptr;
  SumCodeGenerator* code_gen = new SumCodeGenerator(SumFuncRegular,
                                                    &sum_func_ptr);

  ASSERT_TRUE(manager_->EnrollCodeGenerator(
      CodegenFuncLifespan_Parameter_Invariant, code_gen));
  EXPECT_EQ(1, manager_->GenerateCode());

  // Make sure the function pointers refer to regular versions
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);

  // This should update function pointers to generated version,
  // if generation was successful
  ASSERT_TRUE(manager_->PrepareGeneratedFunctions());

  // For sum_func_ptr, we successfully generated code.
  // So, pointer should reflect that.
  ASSERT_TRUE(SumFuncRegular != sum_func_ptr);

  code_gen->Reset();

  // Make sure Reset set the function pointer back to regular version.
  ASSERT_TRUE(SumFuncRegular == sum_func_ptr);
}

}  // namespace gpcodegen

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  AddGlobalTestEnvironment(new gpcodegen::CodegenManagerTestEnvironment);
  return RUN_ALL_TESTS();
}

