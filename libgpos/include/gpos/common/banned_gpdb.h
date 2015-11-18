//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2012 EMC Corp.
//
//  @filename:
//  	banned_gpdb.h
//
//  @doc:
//  	Ban GPDB APIs. Include this file to disallow functions listed in this file.
//
//  @owner:
//  	elhela
//
//  @test:
//
//---------------------------------------------------------------------------

#ifndef BANNED_GPDB_H

#define BANNED_GPDB_H

#include "gpos/common/banned_api.h"

#ifndef ALLOW_DatumGetBool
#undef DatumGetBool
#define DatumGetBool GPOS_BANNED_API(DatumGetBool)
#endif

#ifndef ALLOW_BoolGetDatum
#undef BoolGetDatum
#define BoolGetDatum GPOS_BANNED_API(BoolGetDatum)
#endif

#ifndef ALLOW_DatumGetChar
#undef DatumGetChar
#define DatumGetChar GPOS_BANNED_API(DatumGetChar)
#endif

#ifndef ALLOW_CharGetDatum
#undef CharGetDatum
#define CharGetDatum GPOS_BANNED_API(CharGetDatum)
#endif

#ifndef ALLOW_DatumGetInt8
#undef DatumGetInt8
#define DatumGetInt8 GPOS_BANNED_API(DatumGetInt8)
#endif

#ifndef ALLOW_Int8GetDatum
#undef Int8GetDatum
#define Int8GetDatum GPOS_BANNED_API(Int8GetDatum)
#endif

#ifndef ALLOW_DatumGetUInt8
#undef DatumGetUInt8
#define DatumGetUInt8 GPOS_BANNED_API(DatumGetUInt8)
#endif

#ifndef ALLOW_UInt8GetDatum
#undef UInt8GetDatum
#define UInt8GetDatum GPOS_BANNED_API(UInt8GetDatum)
#endif

#ifndef ALLOW_DatumGetInt16
#undef DatumGetInt16
#define DatumGetInt16 GPOS_BANNED_API(DatumGetInt16)
#endif

#ifndef ALLOW_Int16GetDatum
#undef Int16GetDatum
#define Int16GetDatum GPOS_BANNED_API(Int16GetDatum)
#endif

#ifndef ALLOW_DatumGetUInt16
#undef DatumGetUInt16
#define DatumGetUInt16 GPOS_BANNED_API(DatumGetUInt16)
#endif

#ifndef ALLOW_UInt16GetDatum
#undef UInt16GetDatum
#define UInt16GetDatum GPOS_BANNED_API(UInt16GetDatum)
#endif

#ifndef ALLOW_DatumGetInt32
#undef DatumGetInt32
#define DatumGetInt32 GPOS_BANNED_API(DatumGetInt32)
#endif

#ifndef ALLOW_Int32GetDatum
#undef Int32GetDatum
#define Int32GetDatum GPOS_BANNED_API(Int32GetDatum)
#endif

#ifndef ALLOW_DatumGetUInt32
#undef DatumGetUInt32
#define DatumGetUInt32 GPOS_BANNED_API(DatumGetUInt32)
#endif

#ifndef ALLOW_UInt32GetDatum
#undef UInt32GetDatum
#define UInt32GetDatum GPOS_BANNED_API(UInt32GetDatum)
#endif

#ifndef ALLOW_DatumGetInt64
#undef DatumGetInt64
#define DatumGetInt64 GPOS_BANNED_API(DatumGetInt64)
#endif

#ifndef ALLOW_Int64GetDatum
#undef Int64GetDatum
#define Int64GetDatum GPOS_BANNED_API(Int64GetDatum)
#endif

#ifndef ALLOW_DatumGetUInt64
#undef DatumGetUInt64
#define DatumGetUInt64 GPOS_BANNED_API(DatumGetUInt64)
#endif

#ifndef ALLOW_UInt64GetDatum
#undef UInt64GetDatum
#define UInt64GetDatum GPOS_BANNED_API(UInt64GetDatum)
#endif

#ifndef ALLOW_DatumGetObjectId
#undef DatumGetObjectId
#define DatumGetObjectId GPOS_BANNED_API(DatumGetObjectId)
#endif

#ifndef ALLOW_DatumGetPointer
#undef DatumGetPointer
#define DatumGetPointer GPOS_BANNED_API(DatumGetPointer)
#endif

#ifndef ALLOW_DatumGetFloat4
#undef DatumGetFloat4
#define DatumGetFloat4 GPOS_BANNED_API(DatumGetFloat4)
#endif

#ifndef ALLOW_DatumGetFloat8
#undef DatumGetFloat8
#define DatumGetFloat8 GPOS_BANNED_API(DatumGetFloat8)
#endif

#ifndef ALLOW_PointerGetDatum
#undef PointerGetDatum
#define PointerGetDatum GPOS_BANNED_API(PointerGetDatum)
#endif

#ifndef ALLOW_aggregate_exists
#undef aggregate_exists
#define aggregate_exists GPOS_BANNED_API(aggregate_exists)
#endif

#ifndef ALLOW_trigger_exists
#undef trigger_exists
#define trigger_exists GPOS_BANNED_API(trigger_exists)
#endif

#ifndef ALLOW_bms_add_member
#undef bms_add_member
#define bms_add_member GPOS_BANNED_API(bms_add_member)
#endif

#ifndef ALLOW_copyObject
#undef copyObject
#define copyObject GPOS_BANNED_API(copyObject)
#endif

#ifndef ALLOW_datumGetSize
#undef datumGetSize
#define datumGetSize GPOS_BANNED_API(datumGetSize)
#endif

#ifndef ALLOW_deconstruct_array
#undef deconstruct_array
#define deconstruct_array GPOS_BANNED_API(deconstruct_array)
#endif

#ifndef ALLOW_estimate_rel_size
#undef estimate_rel_size
#define estimate_rel_size GPOS_BANNED_API(estimate_rel_size)
#endif

#ifndef ALLOW_expression_tree_mutator
#undef expression_tree_mutator
#define expression_tree_mutator GPOS_BANNED_API(expression_tree_mutator)
#endif

#ifndef ALLOW_expression_tree_walker
#undef expression_tree_walker
#define expression_tree_walker GPOS_BANNED_API(expression_tree_walker)
#endif

#ifndef ALLOW_exprType
#undef exprType
#define exprType GPOS_BANNED_API(exprType)
#endif

#ifndef ALLOW_exprTypmod
#undef exprTypmod
#define exprTypmod GPOS_BANNED_API(exprTypmod)
#endif

#ifndef ALLOW_extract_nodes_plan
#undef extract_nodes_plan
#define extract_nodes_plan GPOS_BANNED_API(extract_nodes_plan)
#endif

#ifndef ALLOW_extract_nodes_expression
#undef extract_nodes_expression
#define extract_nodes_expression GPOS_BANNED_API(extract_nodes_expression)
#endif

#ifndef ALLOW_free_attstatsslot
#undef free_attstatsslot
#define free_attstatsslot GPOS_BANNED_API(free_attstatsslot)
#endif

#ifndef ALLOW_func_data_access
#undef func_data_access
#define func_data_access GPOS_BANNED_API(func_data_access)
#endif

#ifndef ALLOW_func_strict
#undef func_strict
#define func_strict GPOS_BANNED_API(func_strict)
#endif

#ifndef ALLOW_func_volatile
#undef func_volatile
#define func_volatile GPOS_BANNED_API(func_volatile)
#endif

#ifndef ALLOW_FuncnameGetCandidates
#undef FuncnameGetCandidates
#define FuncnameGetCandidates GPOS_BANNED_API(FuncnameGetCandidates)
#endif

#ifndef ALLOW_function_exists
#undef function_exists
#define function_exists GPOS_BANNED_API(function_exists)
#endif

#ifndef ALLOW_function_oids
#undef function_oids
#define function_oids GPOS_BANNED_API(function_oids)
#endif

#ifndef ALLOW_get_agg_transtype
#undef get_agg_transtype
#define get_agg_transtype GPOS_BANNED_API(get_agg_transtype)
#endif

#ifndef ALLOW_get_aggregate
#undef get_aggregate
#define get_aggregate GPOS_BANNED_API(get_aggregate)
#endif

#ifndef ALLOW_is_agg_ordered
#undef is_agg_ordered
#define is_agg_ordered GPOS_BANNED_API(is_agg_ordered)
#endif

#ifndef ALLOW_has_agg_prelimfunc
#undef has_agg_prelimfunc
#define has_agg_prelimfunc GPOS_BANNED_API(has_agg_prelimfunc)
#endif

#ifndef ALLOW_get_array_type
#undef get_array_type
#define get_array_type GPOS_BANNED_API(get_array_type)
#endif

#ifndef ALLOW_get_attstatsslot
#undef get_attstatsslot
#define get_attstatsslot GPOS_BANNED_API(get_attstatsslot)
#endif

#ifndef ALLOW_get_att_stats
#undef get_att_stats
#define get_att_stats GPOS_BANNED_API(get_att_stats)
#endif

#ifndef ALLOW_get_commutator
#undef get_commutator
#define get_commutator GPOS_BANNED_API(get_commutator)
#endif

#ifndef ALLOW_get_trigger_name
#undef get_trigger_name
#define get_trigger_name GPOS_BANNED_API(get_trigger_name)
#endif

#ifndef ALLOW_get_trigger_relid
#undef get_trigger_relid
#define get_trigger_relid GPOS_BANNED_API(get_trigger_relid)
#endif

#ifndef ALLOW_get_trigger_funcid
#undef get_trigger_funcid
#define get_trigger_funcid GPOS_BANNED_API(get_trigger_funcid)
#endif

#ifndef ALLOW_get_trigger_type
#undef get_trigger_type
#define get_trigger_type GPOS_BANNED_API(get_trigger_type)
#endif

#ifndef ALLOW_trigger_enabled
#undef trigger_enabled
#define trigger_enabled GPOS_BANNED_API(trigger_enabled)
#endif

#ifndef ALLOW_get_func_name
#undef get_func_name
#define get_func_name GPOS_BANNED_API(get_func_name)
#endif

#ifndef ALLOW_get_func_output_arg_types
#undef get_func_output_arg_types
#define get_func_output_arg_types GPOS_BANNED_API(get_func_output_arg_types)
#endif

#ifndef ALLOW_get_func_arg_types
#undef get_func_arg_types
#define get_func_arg_types GPOS_BANNED_API(get_func_arg_types)
#endif

#ifndef ALLOW_get_func_retset
#undef get_func_retset
#define get_func_retset GPOS_BANNED_API(get_func_retset)
#endif

#ifndef ALLOW_get_func_rettype
#undef get_func_rettype
#define get_func_rettype GPOS_BANNED_API(get_func_rettype)
#endif

#ifndef ALLOW_get_negator
#undef get_negator
#define get_negator GPOS_BANNED_API(get_negator)
#endif

#ifndef ALLOW_get_opcode
#undef get_opcode
#define get_opcode GPOS_BANNED_API(get_opcode)
#endif

#ifndef ALLOW_get_opname
#undef get_opname
#define get_opname GPOS_BANNED_API(get_opname)
#endif

#ifndef ALLOW_get_partition_attrs
#undef get_partition_attrs
#define get_partition_attrs GPOS_BANNED_API(get_partition_attrs)
#endif

#ifndef ALLOW_rel_is_leaf_partition
#undef rel_is_leaf_partition
#define rel_is_leaf_partition GPOS_BANNED_API(rel_is_leaf_partition)
#endif

#ifndef ALLOW_rel_partition_get_master
#undef rel_partition_get_master
#define rel_partition_get_master GPOS_BANNED_API(rel_partition_get_master)
#endif

#ifndef ALLOW_BuildLogicalIndexInfo
#undef BuildLogicalIndexInfo
#define BuildLogicalIndexInfo GPOS_BANNED_API(BuildLogicalIndexInfo)
#endif
	
#ifndef ALLOW_get_parts
#undef get_parts
#define get_parts GPOS_BANNED_API(get_parts)
#endif

#ifndef ALLOW_countLeafPartTables
#undef countLeafPartTables
#define countLeafPartTables GPOS_BANNED_API(countLeafPartTables)
#endif

#ifndef ALLOW_rel_partitioning_is_uniform
#undef rel_partitioning_is_uniform
#define rel_partitioning_is_uniform GPOS_BANNED_API(rel_partitioning_is_uniform)
#endif

#ifndef ALLOW_get_relation_keys
#undef get_relation_keys
#define get_relation_keys GPOS_BANNED_API(get_relation_keys)
#endif

#ifndef ALLOW_get_typ_typrelid
#undef get_typ_typrelid
#define get_typ_typrelid GPOS_BANNED_API(get_typ_typrelid)
#endif

#ifndef ALLOW_get_typbyval
#undef get_typbyval
#define get_typbyval GPOS_BANNED_API(get_typbyval)
#endif

#ifndef ALLOW_get_type_name
#undef get_type_name
#define get_type_name GPOS_BANNED_API(get_type_name)
#endif

#ifndef ALLOW_get_typlen
#undef get_typlen
#define get_typlen GPOS_BANNED_API(get_typlen)
#endif

#ifndef ALLOW_get_typlenbyval
#undef get_typlenbyval
#define get_typlenbyval GPOS_BANNED_API(get_typlenbyval)
#endif

#ifndef ALLOW_getgpsegmentCount
#undef getgpsegmentCount
#define getgpsegmentCount GPOS_BANNED_API(getgpsegmentCount)
#endif

#ifndef ALLOW_heap_attisnull
#undef heap_attisnull
#define heap_attisnull GPOS_BANNED_API(heap_attisnull)
#endif

#ifndef ALLOW_heap_freetuple
#undef heap_freetuple
#define heap_freetuple GPOS_BANNED_API(heap_freetuple)
#endif

#ifndef ALLOW_heap_getsysattr
#undef heap_getsysattr
#define heap_getsysattr GPOS_BANNED_API(heap_getsysattr)
#endif

#ifndef ALLOW_heap_open
#undef heap_open
#define heap_open GPOS_BANNED_API(heap_open)
#endif

#ifndef ALLOW_index_exists
#undef index_exists
#define index_exists GPOS_BANNED_API(index_exists)
#endif

#ifndef ALLOW_isGreenplumDbHashable
#undef isGreenplumDbHashable
#define isGreenplumDbHashable GPOS_BANNED_API(isGreenplumDbHashable)
#endif

#ifndef ALLOW_lappend
#undef lappend
#define lappend GPOS_BANNED_API(lappend)
#endif

#ifndef ALLOW_lappend_int
#undef lappend_int
#define lappend_int GPOS_BANNED_API(lappend_int)
#endif

#ifndef ALLOW_lappend_oid
#undef lappend_oid
#define lappend_oid GPOS_BANNED_API(lappend_oid)
#endif

#ifndef ALLOW_lcons
#undef lcons
#define lcons GPOS_BANNED_API(lcons)
#endif

#ifndef ALLOW_lcons_int
#undef lcons_int
#define lcons_int GPOS_BANNED_API(lcons_int)
#endif

#ifndef ALLOW_lcons_oid
#undef lcons_oid
#define lcons_oid GPOS_BANNED_API(lcons_oid)
#endif

#ifndef ALLOW_list_concat
#undef list_concat
#define list_concat GPOS_BANNED_API(list_concat)
#endif

#ifndef ALLOW_list_copy
#undef list_copy
#define list_copy GPOS_BANNED_API(list_copy)
#endif

#ifndef ALLOW_list_free
#undef list_free
#define list_free GPOS_BANNED_API(list_free)
#endif

#ifndef ALLOW_list_head
#undef list_head
#define list_head GPOS_BANNED_API(list_head)
#endif

#ifndef ALLOW_list_tail
#undef list_tail
#define list_tail GPOS_BANNED_API(list_tail)
#endif

#ifndef ALLOW_list_length
#undef list_length
#define list_length GPOS_BANNED_API(list_length)
#endif

#ifndef ALLOW_list_nth
#undef list_nth
#define list_nth GPOS_BANNED_API(list_nth)
#endif

#ifndef ALLOW_list_nth_int
#undef list_nth_int
#define list_nth_int GPOS_BANNED_API(list_nth_int)
#endif

#ifndef ALLOW_list_nth_oid
#undef list_nth_oid
#define list_nth_oid GPOS_BANNED_API(list_nth_oid)
#endif

#ifndef ALLOW_list_member_oid
#undef list_member_oid
#define list_member_oid GPOS_BANNED_API(list_member_oid)
#endif

#ifndef ALLOW_freeListAndNull
#undef freeListAndNull
#define freeListAndNull GPOS_BANNED_API(freeListAndNull)
#endif

#ifndef ALLOW_lookup_type_cache
#undef lookup_type_cache
#define lookup_type_cache GPOS_BANNED_API(lookup_type_cache)
#endif

#ifndef ALLOW_makeString
#undef makeString
#define makeString GPOS_BANNED_API(makeString)
#endif

#ifndef ALLOW_makeInteger
#undef makeInteger
#define makeInteger GPOS_BANNED_API(makeInteger)
#endif

#ifndef ALLOW_makeBoolConst
#undef makeBoolConst
#define makeBoolConst GPOS_BANNED_API(makeBoolConst)
#endif

#ifndef ALLOW_makeTargetEntry
#undef makeTargetEntry
#define makeTargetEntry GPOS_BANNED_API(makeTargetEntry)
#endif

#ifndef ALLOW_makeVar
#undef makeVar
#define makeVar GPOS_BANNED_API(makeVar)
#endif

#ifndef ALLOW_MemoryContextAllocImpl
#undef MemoryContextAllocImpl
#define MemoryContextAllocImpl GPOS_BANNED_API(MemoryContextAllocImpl)
#endif

#ifndef ALLOW_MemoryContextAllocZeroAlignedImpl
#undef MemoryContextAllocZeroAlignedImpl
#define MemoryContextAllocZeroAlignedImpl GPOS_BANNED_API(MemoryContextAllocZeroAlignedImpl)
#endif

#ifndef ALLOW_MemoryContextAllocZeroImpl
#undef MemoryContextAllocZeroImpl
#define MemoryContextAllocZeroImpl GPOS_BANNED_API(MemoryContextAllocZeroImpl)
#endif

#ifndef ALLOW_MemoryContextFreeImpl
#undef MemoryContextFreeImpl
#define MemoryContextFreeImpl GPOS_BANNED_API(MemoryContextFreeImpl)
#endif

#ifndef ALLOW_MemoryContextReallocImpl
#undef MemoryContextReallocImpl
#define MemoryContextReallocImpl GPOS_BANNED_API(MemoryContextReallocImpl)
#endif

#ifndef ALLOW_MemoryContextStrdup
#undef MemoryContextStrdup
#define MemoryContextStrdup GPOS_BANNED_API(MemoryContextStrdup)
#endif

#ifndef ALLOW_nocachegetattr
#undef nocachegetattr
#define nocachegetattr GPOS_BANNED_API(nocachegetattr)
#endif

#ifndef ALLOW_nodeToString
#undef nodeToString
#define nodeToString GPOS_BANNED_API(nodeToString)
#endif

#ifndef ALLOW_numeric_to_double_no_overflow
#undef numeric_to_double_no_overflow
#define numeric_to_double_no_overflow GPOS_BANNED_API(numeric_to_double_no_overflow)
#endif

#ifndef ALLOW_convert_timevalue_to_scalar
#undef convert_timevalue_to_scalar
#define convert_timevalue_to_scalar GPOS_BANNED_API(convert_timevalue_to_scalar)
#endif

#ifndef ALLOW_convert_network_to_scalar
#undef convert_network_to_scalar
#define convert_network_to_scalar GPOS_BANNED_API(convert_network_to_scalar)
#endif

#ifndef ALLOW_op_hashjoinable
#undef op_hashjoinable
#define op_hashjoinable GPOS_BANNED_API(op_hashjoinable)
#endif

#ifndef ALLOW_op_mergejoinable
#undef op_mergejoinable
#define op_mergejoinable GPOS_BANNED_API(op_mergejoinable)
#endif

#ifndef ALLOW_op_strict
#undef op_strict
#define op_strict GPOS_BANNED_API(op_strict)
#endif

#ifndef ALLOW_op_input_types
#undef op_input_types
#define op_input_types GPOS_BANNED_API(op_input_types)
#endif

#ifndef ALLOW_operator_exists
#undef operator_exists
#define operator_exists GPOS_BANNED_API(operator_exists)
#endif

#ifndef ALLOW_operator_oids
#undef operator_oids
#define operator_oids GPOS_BANNED_API(operator_oids)
#endif

#ifndef ALLOW_palloc
#undef palloc
#define palloc GPOS_BANNED_API(palloc)
#endif

#ifndef ALLOW_pfree
#undef pfree
#define pfree GPOS_BANNED_API(pfree)
#endif

#ifndef ALLOW_equal
#undef equal
#define equal GPOS_BANNED_API(equal)
#endif

#ifndef ALLOW_pg_detoast_datum
#undef pg_detoast_datum
#define pg_detoast_datum GPOS_BANNED_API(pg_detoast_datum)
#endif

#ifndef ALLOW_query_or_expression_tree_walker
#undef query_or_expression_tree_walker
#define query_or_expression_tree_walker GPOS_BANNED_API(query_or_expression_tree_walker)
#endif

#ifndef ALLOW_query_or_expression_tree_mutator
#undef query_or_expression_tree_mutator
#define query_or_expression_tree_mutator GPOS_BANNED_API(query_or_expression_tree_mutator)
#endif

#ifndef ALLOW_query_tree_mutator
#undef query_tree_mutator
#define query_tree_mutator GPOS_BANNED_API(query_tree_mutator)
#endif

#ifndef ALLOW_external_relation_exists
#undef external_relation_exists
#define external_relation_exists GPOS_BANNED_API(external_relation_exists)
#endif

#ifndef ALLOW_range_table_mutator
#undef range_table_mutator
#define range_table_mutator GPOS_BANNED_API(range_table_mutator)
#endif

#ifndef ALLOW_rel_part_status
#undef rel_part_status
#define rel_part_status GPOS_BANNED_API(rel_part_status)
#endif

#ifndef ALLOW_relation_close
#undef relation_close
#define relation_close GPOS_BANNED_API(relation_close)
#endif

#ifndef ALLOW_relation_exists
#undef relation_exists
#define relation_exists GPOS_BANNED_API(relation_exists)
#endif

#ifndef ALLOW_relation_oids
#undef relation_oids
#define relation_oids GPOS_BANNED_API(relation_oids)
#endif

#ifndef ALLOW_RelationClose
#undef RelationClose
#define RelationClose GPOS_BANNED_API(RelationClose)
#endif

#ifndef ALLOW_RelationGetIndexList
#undef RelationGetIndexList
#define RelationGetIndexList GPOS_BANNED_API(RelationGetIndexList)
#endif

#ifndef ALLOW_RelationBuildTriggers
#undef RelationBuildTriggers
#define RelationBuildTriggers GPOS_BANNED_API(RelationBuildTriggers)
#endif

#ifndef ALLOW_RelationIdGetRelation
#undef RelationIdGetRelation
#define RelationIdGetRelation GPOS_BANNED_API(RelationIdGetRelation)
#endif

#ifndef ALLOW_ReleaseSysCache
#undef ReleaseSysCache
#define ReleaseSysCache GPOS_BANNED_API(ReleaseSysCache)
#endif

#ifndef ALLOW_ScanKeyInit
#undef ScanKeyInit
#define ScanKeyInit GPOS_BANNED_API(ScanKeyInit)
#endif

#ifndef ALLOW_SearchSysCache
#undef SearchSysCache
#define SearchSysCache GPOS_BANNED_API(SearchSysCache)
#endif

#ifndef ALLOW_SearchSysCacheExists
#undef SearchSysCacheExists
#define SearchSysCacheExists GPOS_BANNED_API(SearchSysCacheExists)
#endif

#ifndef ALLOW_systable_beginscan
#undef systable_beginscan
#define systable_beginscan GPOS_BANNED_API(systable_beginscan)
#endif

#ifndef ALLOW_systable_endscan
#undef systable_endscan
#define systable_endscan GPOS_BANNED_API(systable_endscan)
#endif

#ifndef ALLOW_systable_getnext
#undef systable_getnext
#define systable_getnext GPOS_BANNED_API(systable_getnext)
#endif

#ifndef ALLOW_tlist_member
#undef tlist_member
#define tlist_member GPOS_BANNED_API(tlist_member)
#endif

#ifndef ALLOW_flatten_join_alias_var_optimizer
#undef flatten_join_alias_var_optimizer
#define flatten_join_alias_var_optimizer GPOS_BANNED_API(flatten_join_alias_var_optimizer)
#endif

#ifndef ALLOW_tlist_members
#undef tlist_members
#define tlist_members GPOS_BANNED_API(tlist_members)
#endif

#ifndef ALLOW_type_exists
#undef type_exists
#define type_exists GPOS_BANNED_API(type_exists)
#endif

#ifndef ALLOW_GetExtTableEntry
#undef GetExtTableEntry
#define GetExtTableEntry GPOS_BANNED_API(GetExtTableEntry)
#endif

#ifndef ALLOW_ParseExternalTableUri
#undef ParseExternalTableUri
#define ParseExternalTableUri GPOS_BANNED_API(ParseExternalTableUri)
#endif

#ifndef ALLOW_getCdbComponentDatabases
#undef getCdbComponentDatabases
#define getCdbComponentDatabases GPOS_BANNED_API(getCdbComponentDatabases)
#endif

#ifndef ALLOW_pg_strcasecmp
#undef pg_strcasecmp
#define pg_strcasecmp GPOS_BANNED_API(pg_strcasecmp)
#endif

#ifndef ALLOW_makeRandomSegMap
#undef makeRandomSegMap
#define makeRandomSegMap GPOS_BANNED_API(makeRandomSegMap)
#endif

#ifndef ALLOW_makeStringInfo
#undef makeStringInfo
#define makeStringInfo GPOS_BANNED_API(makeStringInfo)
#endif

#ifndef ALLOW_appendStringInfo
#undef appendStringInfo
#define appendStringInfo GPOS_BANNED_API(appendStringInfo)
#endif

#ifndef ALLOW_get_cast_func
#undef get_cast_func
#define get_cast_func GPOS_BANNED_API(get_cast_func)
#endif

#ifndef ALLOW_get_relation_part_constraints
#undef get_relation_part_constraints
#define get_relation_part_constraints GPOS_BANNED_API(get_relation_part_constraints)
#endif

#ifndef ALLOW_char_to_parttype
#undef char_to_parttype
#define char_to_parttype GPOS_BANNED_API(char_to_parttype)
#endif

#ifndef ALLOW_makeNullConst
#undef makeNullConst
#define makeNullConst GPOS_BANNED_API(makeNullConst)
#endif


#ifndef ALLOW_get_operator_type
#undef get_operator_type
#define get_operator_type GPOS_BANNED_API(get_operator_type)
#endif

#ifndef ALLOW_get_operator_type
#undef get_comparison_operator
#define get_comparison_operator GPOS_BANNED_API(get_comparison_operator)
#endif

#ifndef ALLOW_find_nodes
#undef find_nodes
#define find_nodes GPOS_BANNED_API(find_nodes)
#endif

#ifndef ALLOW_coerce_to_common_type
#undef coerce_to_common_type
#define coerce_to_common_type GPOS_BANNED_API(coerce_to_common_type)
#endif

#ifndef ALLOW_resolve_generic_type
#undef resolve_generic_type
#define resolve_generic_type GPOS_BANNED_API(resolve_generic_type)
#endif

#ifndef ALLOW_has_sublclass
#undef has_sublclass
#define has_sublclass GPOS_BANNED_API(has_sublclass)
#endif

#ifndef ALLOW_cdbhash_const
#undef cdbhash_const
#define cdbhash_const GPOS_BANNED_API(cdbhash_const)
#endif

#ifndef ALLOW_cdbhash_const_list
#undef cdbhash_const_list
#define cdbhash_const_list GPOS_BANNED_API(cdbhash_const_list)
#endif

#ifndef ALLOW_is_pxf_protocol
#undef is_pxf_protocol
#define is_pxf_protocol GPOS_BANNED_API(is_pxf_protocol)
#endif

#ifndef ALLOW_ExecCheckRTPerms
#undef ExecCheckRTPerms
#define ExecCheckRTPerms GPOS_BANNED_API(ExecCheckRTPerms)
#endif

#ifndef ALLOW_initStringInfoOfSize
#undef initStringInfoOfSize
#define initStringInfoOfSize GPOS_BANNED_API(initStringInfoOfSize)
#endif

#ifndef ALLOW_appendStringInfoString
#undef appendStringInfoString
#define appendStringInfoString GPOS_BANNED_API(appendStringInfoString)
#endif

#ifndef ALLOW_appendStringInfoChar
#undef appendStringInfoChar
#define appendStringInfoChar GPOS_BANNED_API(appendStringInfoChar)
#endif

#ifndef ALLOW_pxf_calc_max_participants_allowed
#undef pxf_calc_max_participants_allowed
#define pxf_calc_max_participants_allowed GPOS_BANNED_API(pxf_calc_max_participants_allowed)
#endif

#ifndef ALLOW_map_hddata_2gp_segments
#undef map_hddata_2gp_segments
#define map_hddata_2gp_segments GPOS_BANNED_API(map_hddata_2gp_segments)
#endif

#ifndef ALLOW_free_hddata_2gp_segments
#undef free_hddata_2gp_segments
#define free_hddata_2gp_segments GPOS_BANNED_API(free_hddata_2gp_segments)
#endif

#ifndef ALLOW_evaluate_expr
#undef evaluate_expr
#define evaluate_expr GPOS_BANNED_API(evaluate_expr)
#endif

#ifndef ALLOW_interpretOidsOption
#undef interpretOidsOption
#define interpretOidsOption GPOS_BANNED_API(interpretOidsOption)
#endif

#ifndef ALLOW_defGetString
#undef defGetString
#define defGetString GPOS_BANNED_API(defGetString)
#endif

#ifndef ALLOW_get_index_opclasses
#undef get_index_opclasses
#define get_index_opclasses GPOS_BANNED_API(get_index_opclasses)
#endif

#ifndef ALLOW_get_operator_opclasses
#undef get_operator_opclasses
#define get_operator_opclasses GPOS_BANNED_API(get_operator_opclasses)
#endif

#ifndef ALLOW_fold_arrayexpr_constants
#undef fold_arrayexpr_constants
#define fold_arrayexpr_constants GPOS_BANNED_API(fold_arrayexpr_constants)
#endif

#ifndef ALLOW_has_parquet_children
#undef has_parquet_children
#define has_parquet_children GPOS_BANNED_API(has_parquet_children)
#endif

#ifndef ALLOW_relation_policy
#undef relation_policy
#define relation_policy GPOS_BANNED_API(relation_policy)
#endif

#ifndef ALLOW_child_distribution_mismatch
#undef child_distribution_mismatch
#define child_distribution_mismatch GPOS_BANNED_API(child_distribution_mismatch)
#endif

#ifndef ALLOW_child_triggers
#undef child_triggers
#define child_triggers GPOS_BANNED_API(child_triggers)
#endif

#ifndef ALLOW_has_subclass
#undef has_subclass
#define has_subclass GPOS_BANNED_API(has_subclass)
#endif

#ifndef ALLOW_isMotionGather
#undef isMotionGather
#define isMotionGather GPOS_BANNED_API(isMotionGather)
#endif

#ifndef ALLOW_static_part_selection
#undef static_part_selection
#define static_part_selection GPOS_BANNED_API(static_part_selection)
#endif

#ifndef ALLOW_pxf_calc_participating_segments
#undef pxf_calc_participating_segments
#define pxf_calc_participating_segments GPOS_BANNED_API(pxf_calc_participating_segments)
#endif

#ifndef ALLOW_mdver_request_version
#undef mdver_request_version
#define mdver_request_version GPOS_BANNED_API(mdver_request_version)
#endif

#ifndef ALLOW_mdver_enabled
#undef mdver_enabled
#define mdver_enabled GPOS_BANNED_API(mdver_enabled)
#endif

#endif //#ifndef BANNED_GPDB_H

// EOF
