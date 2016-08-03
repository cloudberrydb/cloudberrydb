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

extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#undef newNode  // undef newNode so it doesn't have name collision with llvm
#include "utils/elog.h"
#undef elog
#define elog(...)
}

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


#include "codegen/utils/codegen_utils.h"
#include "codegen/utils/gp_codegen_utils.h"
#include "codegen/utils/utility.h"
#include "codegen/codegen_manager.h"
#include "codegen/codegen_wrapper.h"
#include "codegen/codegen_interface.h"
#include "codegen/base_codegen.h"

extern bool codegen_validate_functions;
using gpcodegen::GpCodegenUtils;
namespace gpcodegen {

typedef int (*SumFunc) (int x, int y);
typedef void (*UncompilableFunc)(int x);
typedef int (*MulFunc) (int x, int y);

template <typename dest_type, typename src_type>
using DatumCastFn = dest_type (*)(src_type);

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
  explicit SumCodeGenerator(gpcodegen::CodegenManager* manager,
                            SumFunc regular_func_ptr,
                             SumFunc* ptr_to_regular_func_ptr) :
                             BaseCodegen(manager,
                                         kAddFuncNamePrefix,
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
  explicit MulOverflowCodeGenerator(gpcodegen::CodegenManager* manager,
                                    MulFunc regular_func_ptr,
                                    MulFunc* ptr_to_regular_func_ptr) :
                                    BaseCodegen(manager,
                                                kMulFuncNamePrefix,
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
  explicit FailingCodeGenerator(gpcodegen::CodegenManager* manager,
                                SumFunc regular_func_ptr,
                                SumFunc* ptr_to_regular_func_ptr):
                                BaseCodegen(manager,
                                            kFailingFuncNamePrefix,
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
      gpcodegen::CodegenManager* manager,
      UncompilableFunc regular_func_ptr,
      UncompilableFunc* ptr_to_regular_func_ptr)
  : BaseCodegen(manager,
                kUncompilableFuncNamePrefix,
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

template <typename dest_type>
class DatumToCppCastGenerator :
    public BaseCodegen<DatumCastFn<dest_type, Datum>> {
  using DatumCastTemplateFn = DatumCastFn<dest_type, Datum>;

 public:
  explicit DatumToCppCastGenerator(
      gpcodegen::CodegenManager* manager,
      DatumCastTemplateFn regular_func_ptr,
      DatumCastTemplateFn* ptr_to_regular_func_ptr):
      BaseCodegen<DatumCastTemplateFn>(manager,
                  kDatumToCppCastFuncNamePrefix,
                  regular_func_ptr,
                  ptr_to_regular_func_ptr) {
  }

  virtual ~DatumToCppCastGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
    llvm::Function* cast_func
                = this->template CreateFunction<DatumCastTemplateFn>(
                    codegen_utils, this->GetUniqueFuncName());
    llvm::BasicBlock* entry_block = codegen_utils->CreateBasicBlock("entry",
                                                                  cast_func);
    codegen_utils->ir_builder()->SetInsertPoint(entry_block);
    llvm::Value* llvm_arg0 = ArgumentByPosition(cast_func, 0);
    codegen_utils->ir_builder()->CreateRet(
                    codegen_utils->CreateDatumToCppTypeCast<dest_type>(
                        llvm_arg0));
    cast_func->dump();
    return true;
  }

 private:
  static constexpr char kDatumToCppCastFuncNamePrefix[] = "DatumToCppCastFunc";
};

template <typename src_type>
class CppToDatumCastGenerator :
    public BaseCodegen<DatumCastFn<Datum, src_type>> {
  using DatumCastTemplateFn = DatumCastFn<Datum, src_type>;
 public:
  explicit CppToDatumCastGenerator(
      gpcodegen::CodegenManager* manager,
      DatumCastTemplateFn regular_func_ptr,
      DatumCastTemplateFn* ptr_to_regular_func_ptr):
      BaseCodegen<DatumCastTemplateFn>(manager,
                  kCppToDatumCastFuncNamePrefix,
                  regular_func_ptr,
                  ptr_to_regular_func_ptr) {
  }

  virtual ~CppToDatumCastGenerator() = default;

 protected:
  bool GenerateCodeInternal(gpcodegen::GpCodegenUtils* codegen_utils) final {
    llvm::Function* cast_func
                = this->template CreateFunction<DatumCastTemplateFn>(
                    codegen_utils, this->GetUniqueFuncName());
    llvm::BasicBlock* entry_block = codegen_utils->CreateBasicBlock("entry",
                                                                  cast_func);
    codegen_utils->ir_builder()->SetInsertPoint(entry_block);
    llvm::Value* llvm_arg0 = ArgumentByPosition(cast_func, 0);
    codegen_utils->ir_builder()->CreateRet(
                    codegen_utils->CreateCppTypeToDatumCast(
                        llvm_arg0, std::is_unsigned<src_type>::value));
    cast_func->dump();
    return true;
  }

 private:
  static constexpr char kCppToDatumCastFuncNamePrefix[] = "CppToDatumCastFunc";
};

constexpr char SumCodeGenerator::kAddFuncNamePrefix[];
constexpr char FailingCodeGenerator::kFailingFuncNamePrefix[];
constexpr char MulOverflowCodeGenerator::kMulFuncNamePrefix[];
template <typename dest_type>
constexpr char
DatumToCppCastGenerator<dest_type>::kDatumToCppCastFuncNamePrefix[];

template <typename src_type>
constexpr char
CppToDatumCastGenerator<src_type>::kCppToDatumCastFuncNamePrefix[];

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
    ClassType* code_gen = new ClassType(
        manager_.get(), reg_func, ptr_to_chosen_func);
    ASSERT_TRUE(reg_func == *ptr_to_chosen_func);
    ASSERT_TRUE(manager_->EnrollCodeGenerator(
        CodegenFuncLifespan_Parameter_Invariant,
        code_gen));
  }

  template <typename CppType>
  void CheckDatumCast(DatumCastFn<Datum, CppType> CppToDatumReg,
                      DatumCastFn<CppType, Datum> DatumToCppReg,
                      const std::vector<CppType>& values) {
    DatumCastFn<Datum, CppType> CppToDatumCgFn = CppToDatumReg;
    CppToDatumCastGenerator<CppType>* cpp_datum_gen =
        new CppToDatumCastGenerator<CppType>(
            manager_.get(), CppToDatumReg, &CppToDatumCgFn);


    DatumCastFn<CppType, Datum> DatumToCppCgFn = DatumToCppReg;
    DatumToCppCastGenerator<CppType>* datum_cpp_gen =
        new DatumToCppCastGenerator<CppType>(
            manager_.get(), DatumToCppReg, &DatumToCppCgFn);

    ASSERT_TRUE(manager_->EnrollCodeGenerator(
        CodegenFuncLifespan_Parameter_Invariant, cpp_datum_gen));

    ASSERT_TRUE(manager_->EnrollCodeGenerator(
        CodegenFuncLifespan_Parameter_Invariant, datum_cpp_gen));

    EXPECT_EQ(2, manager_->GenerateCode());

    ASSERT_TRUE(manager_->PrepareGeneratedFunctions());

    ASSERT_TRUE(CppToDatumCgFn != CppToDatumReg);
    ASSERT_TRUE(DatumToCppCgFn != DatumToCppReg);

    for (const CppType& v : values) {
      Datum d_gpdb = CppToDatumReg(v);
      Datum d_codegen = CppToDatumCgFn(v);
      std::cout << "d_gpdb " << d_gpdb
          << " d_codegen " << d_codegen << std::endl;
      EXPECT_EQ(d_gpdb, d_codegen);
      EXPECT_EQ(DatumToCppReg(d_gpdb), DatumToCppCgFn(d_codegen));
    }
  }

  std::unique_ptr<CodegenManager> manager_;
};

TEST_F(CodegenManagerTest, TestGetters) {
  sum_func_ptr = nullptr;
  SumCodeGenerator* code_gen = new SumCodeGenerator(manager_.get(),
                                                    SumFuncRegular,
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
  SumCodeGenerator* code_gen = new SumCodeGenerator(manager_.get(),
                                                    SumFuncRegular,
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

TEST_F(CodegenManagerTest, TestDatumBoolCast) {
  CheckDatumCast<bool>(BoolGetDatum,
                       DatumGetBool,
                        {true, false});
}

TEST_F(CodegenManagerTest, TestDatumCharCast) {
  CheckDatumCast<char>(CharGetDatum,
                       DatumGetChar,
                        {'a', 'A'});
}

TEST_F(CodegenManagerTest, TestDatumInt8Cast) {
  std::vector<int8_t> values = {0, -0, 23, -23,
      std::numeric_limits<int8_t>::max(),
      std::numeric_limits<int8_t>::min(),
      std::numeric_limits<int8_t>::lowest()
  };
  CheckDatumCast<int8_t>(Int8GetDatum,
                         DatumGetInt8,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumUInt8Cast) {
  std::vector<uint8_t> values = {0, -0, 23, static_cast<uint8_t>(-23),
        std::numeric_limits<uint8_t>::max(),
        std::numeric_limits<uint8_t>::min(),
        std::numeric_limits<uint8_t>::lowest()
    };
  CheckDatumCast<uint8_t>(UInt8GetDatum,
                         DatumGetUInt8,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumInt16Cast) {
  std::vector<int16_t> values = {0, -0, 23, -23,
        std::numeric_limits<int16_t>::max(),
        std::numeric_limits<int16_t>::min(),
        std::numeric_limits<int16_t>::lowest()
    };
  CheckDatumCast<int16_t>(Int16GetDatum,
                         DatumGetInt16,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumUInt16Cast) {
  std::vector<uint16_t> values = {0, -0, 23, static_cast<uint16_t>(-23),
          std::numeric_limits<uint16_t>::max(),
          std::numeric_limits<uint16_t>::min(),
          std::numeric_limits<uint16_t>::lowest()
      };
  CheckDatumCast<uint16_t>(UInt16GetDatum,
                         DatumGetUInt16,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumInt32Cast) {
  std::vector<int32_t> values = {0, -0, 23, -23,
          std::numeric_limits<int32_t>::max(),
          std::numeric_limits<int32_t>::min(),
          std::numeric_limits<int32_t>::lowest()
      };
  CheckDatumCast<int32_t>(Int32GetDatum,
                         DatumGetInt32,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumUInt32Cast) {
  std::vector<uint32_t> values = {0, -0, 23, static_cast<uint32_t>(-23),
            std::numeric_limits<uint32_t>::max(),
            std::numeric_limits<uint32_t>::min(),
            std::numeric_limits<uint32_t>::lowest()
        };
  CheckDatumCast<uint32_t>(UInt32GetDatum,
                         DatumGetUInt32,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumInt64Cast) {
  std::vector<int64> values = {0, -0, 23, -23,
            std::numeric_limits<int64>::max(),
            std::numeric_limits<int64>::min(),
            std::numeric_limits<int64>::lowest()
        };
  CheckDatumCast<int64>(Int64GetDatum,
                         DatumGetInt64,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumUInt64Cast) {
  std::vector<uint64> values = {0, -0, 23, static_cast<uint64>(-23),
              std::numeric_limits<uint64>::max(),
              std::numeric_limits<uint64>::min(),
              std::numeric_limits<uint64>::lowest()
          };
  CheckDatumCast<uint64>(UInt64GetDatum,
                         DatumGetUInt64,
                         values);
}

TEST_F(CodegenManagerTest, TestDatumOidCast) {
  std::vector<Oid> values = {0, -0, 23, static_cast<Oid>(-23),
              std::numeric_limits<Oid>::max(),
              std::numeric_limits<Oid>::min(),
              std::numeric_limits<Oid>::lowest()
          };
  CheckDatumCast<Oid>(ObjectIdGetDatum,
                      DatumGetObjectId,
                      values);
}

TEST_F(CodegenManagerTest, TestDatumTxIdCast) {
  std::vector<TransactionId> values = {0, -0, 23,
                static_cast<TransactionId>(-23),
                std::numeric_limits<TransactionId>::max(),
                std::numeric_limits<TransactionId>::min(),
                std::numeric_limits<TransactionId>::lowest()
            };
  CheckDatumCast<TransactionId>(TransactionIdGetDatum,
                                DatumGetTransactionId,
                                values);
}

TEST_F(CodegenManagerTest, TestDatumCmdIdCast) {
  std::vector<CommandId> values = {0, -0, 23,
                  static_cast<CommandId>(-23),
                  std::numeric_limits<CommandId>::max(),
                  std::numeric_limits<CommandId>::min(),
                  std::numeric_limits<CommandId>::lowest()
              };
  CheckDatumCast<CommandId>(CommandIdGetDatum,
                            DatumGetCommandId,
                            values);
}

TEST_F(CodegenManagerTest, TestDatumPtrCast) {
  struct DatumVoidPtr {
    static Datum PointerGetDatumNoConst(void *p) {
      return PointerGetDatum((const void*)p);
    }
  };
  std::vector<void*> values = {reinterpret_cast<void*>(23),
      reinterpret_cast<void*>(-23),
      reinterpret_cast<void*>(NULL)};
  CheckDatumCast<void*>(DatumVoidPtr::PointerGetDatumNoConst,
                        DatumGetPointer,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumCStringCast) {
  struct DatumVoidPtr {
    static Datum PointerGetDatumNoConst(char *p) {
      return PointerGetDatum((const char*)p);
    }
  };
  std::vector<char*> values = {const_cast<char*>("dfdFD"),
      const_cast<char*>(""),
      static_cast<char*>(nullptr)};
  CheckDatumCast<char*>(DatumVoidPtr::PointerGetDatumNoConst,
                        DatumGetCString,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumNameCast) {
  Name n1 = new nameData();
  Name n2 = new nameData();
  strncpy(n1->data, "iqbal", sizeof(n1->data) - 1);
  strncpy(n2->data, "", sizeof(n2->data) - 1);
  CheckDatumCast<Name>(NameGetDatum,
                            DatumGetName,
                            {n1, n2});
  delete n1;
  delete n2;
}

TEST_F(CodegenManagerTest, TestDatumFloatCast) {
  std::vector<float> values = {0.0, -0.0, 12.34, -12.34,
      std::numeric_limits<float>::min(),
      std::numeric_limits<float>::max(),
      std::numeric_limits<float>::lowest(),
      std::numeric_limits<float>::denorm_min(),
      std::numeric_limits<float>::infinity()};
  CheckDatumCast<float>(Float4GetDatum,
                        DatumGetFloat4,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumDoubleCast) {
  std::vector<double> values = {0.0, -0.0, 12.34, -12.34,
        std::numeric_limits<double>::min(),
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::lowest(),
        std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::infinity()};
  CheckDatumCast<double>(Float8GetDatum,
                        DatumGetFloat8,
                        values);
}

TEST_F(CodegenManagerTest, TestDatumItemPtrCast) {
  ItemPointer p1 = reinterpret_cast<ItemPointer>(23);
  ItemPointer p2 = reinterpret_cast<ItemPointer>(42);
  CheckDatumCast<ItemPointer>(ItemPointerGetDatum,
                        DatumGetItemPointer,
                        {p1, p2});
}

}  // namespace gpcodegen

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  AddGlobalTestEnvironment(new gpcodegen::CodegenManagerTestEnvironment);
  return RUN_ALL_TESTS();
}

