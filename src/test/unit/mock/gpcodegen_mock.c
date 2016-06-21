#include "postgres.h"

#include "codegen/codegen_wrapper.h"

// Do one-time global initialization of LLVM library. Returns 0
// on success, nonzero on error.
unsigned int
InitCodegen()
{
	elog(ERROR, "mock implementation of InitCodegen called");
	return 0;
}

// creates a manager for an operator
void*
CodeGeneratorManagerCreate(const char* module_name)
{
	elog(ERROR, "mock implementation of CodeGeneratorManager_Create called");
	return NULL;
}

// calls all the registered CodegenInterface to generate code
unsigned int
CodeGeneratorManagerGenerateCode(void* manager)
{
	elog(ERROR, "mock implementation of CodeGeneratorManager_GenerateCode called");
	return 1;
}

// compiles and prepares all the code gened function pointers
unsigned int
CodeGeneratorManagerPrepareGeneratedFunctions(void* manager)
{
	elog(ERROR, "mock implementation of CodeGeneratorManager_PrepareGeneratedFunctions called");
	return 1;
}

// notifies a manager that the underlying operator has a parameter change
unsigned int
CodeGeneratorManagerNotifyParameterChange(void* manager)
{
	elog(ERROR, "mock implementation of CodeGeneratorManager_NotifyParameterChange called");
	return 1;
}

// destroys a manager for an operator
void
CodeGeneratorManagerDestroy(void* manager)
{
	elog(ERROR, "mock implementation of CodeGeneratorManager_Destroy called");
}

// get the active code generator manager
void*
GetActiveCodeGeneratorManager()
{
	elog(ERROR, "mock implementation of GetActiveCodeGeneratorManager called");
	return NULL;
}

// set the active code generator manager
void
SetActiveCodeGeneratorManager(void* manager)
{
	elog(ERROR, "mock implementation of SetActiveCodeGeneratorManager called");
}

// returns the pointer to the ExecVariableListGenerator
void*
ExecVariableListCodegenEnroll(ExecVariableListFn regular_func_ptr,
                              ExecVariableListFn* ptr_to_regular_func_ptr,
                              struct ProjectionInfo* proj_info,
                              struct TupleTableSlot* slot)
{
  *ptr_to_regular_func_ptr = regular_func_ptr;
	elog(ERROR, "mock implementation of ExecVariableListEnroll called");
	return NULL;
}

// Enroll and returns the pointer to ExecEvalExprGenerator
void*
ExecEvalExprCodegenEnroll(ExecEvalExprFn regular_func_ptr,
                          ExecEvalExprFn* ptr_to_regular_func_ptr,
                          struct ExprState *exprstate,
                          struct ExprContext *econtext)
{
  *ptr_to_regular_func_ptr = regular_func_ptr;
   elog(ERROR, "mock implementation of ExecEvalExprCodegenEnroll called");
   return NULL;
}

