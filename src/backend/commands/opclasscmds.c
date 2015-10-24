/*-------------------------------------------------------------------------
 *
 * opclasscmds.c
 *
 *	  Routines for opclass manipulation commands
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/backend/commands/opclasscmds.c,v 1.49 2006/10/04 00:29:51 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/catquery.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"


/*
 * We use lists of this struct type to keep track of both operators and
 * procedures during DefineOpClass.
 */
typedef struct
{
	Oid			object;			/* operator or support proc's OID */
	int			number;			/* strategy or support proc number */
	Oid			subtype;		/* subtype */
	bool		recheck;		/* oper recheck flag (unused for proc) */
} OpClassMember;


static Oid	assignOperSubtype(Oid amoid, Oid typeoid, Oid operOid);
static Oid	assignProcSubtype(Oid amoid, Oid typeoid, Oid procOid);
static void addClassMember(List **list, OpClassMember *member, bool isProc);
static void storeOperators(Oid opclassoid, List *operators);
static void storeProcedures(Oid opclassoid, List *procedures);
static void AlterOpClassOwner_internal(cqContext *pcqCtx,
									   Relation rel, HeapTuple tuple,
									   Oid newOwnerId);


/*
 * DefineOpClass
 *		Define a new index operator class.
 */
void
DefineOpClass(CreateOpClassStmt *stmt)
{
	char	   *opcname;		/* name of opclass we're creating */
	Oid			amoid,			/* our AM's oid */
				typeoid,		/* indexable datatype oid */
				storageoid,		/* storage datatype oid, if any */
				namespaceoid,	/* namespace to create opclass in */
				opclassoid;		/* oid of opclass we create */
	int			numOperators,	/* amstrategies value */
				numProcs;		/* amsupport value */
	bool		amstorage;		/* amstorage flag */
	List	   *operators;		/* OpClassMember list for operators */
	List	   *procedures;		/* OpClassMember list for support procs */
	ListCell   *l;
	Relation	rel;
	HeapTuple	tup;
	Form_pg_am	pg_am;
	Datum		values[Natts_pg_opclass];
	bool		nulls[Natts_pg_opclass];
	AclResult	aclresult;
	NameData	opcName;
	int			i;
	ObjectAddress myself,
				referenced;
	cqContext	*pcqCtx;
	cqContext	 cqc;

	/* Convert list of names to a name and namespace */
	namespaceoid = QualifiedNameGetCreationNamespace(stmt->opclassname,
													 &opcname);

	/* Check we have creation rights in target namespace */
	aclresult = pg_namespace_aclcheck(namespaceoid, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceoid));

	/* Get necessary info about access method */

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_am "
				" WHERE amname = :1 ",
				CStringGetDatum(stmt->amname)));

	tup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist",
						stmt->amname)));

	amoid = HeapTupleGetOid(tup);
	pg_am = (Form_pg_am) GETSTRUCT(tup);
	numOperators = pg_am->amstrategies;
	numProcs = pg_am->amsupport;
	amstorage = pg_am->amstorage;

	/* XXX Should we make any privilege check against the AM? */

	caql_endscan(pcqCtx);

	/*
	 * The question of appropriate permissions for CREATE OPERATOR CLASS is
	 * interesting.  Creating an opclass is tantamount to granting public
	 * execute access on the functions involved, since the index machinery
	 * generally does not check access permission before using the functions.
	 * A minimum expectation therefore is that the caller have execute
	 * privilege with grant option.  Since we don't have a way to make the
	 * opclass go away if the grant option is revoked, we choose instead to
	 * require ownership of the functions.	It's also not entirely clear what
	 * permissions should be required on the datatype, but ownership seems
	 * like a safe choice.
	 *
	 * Currently, we require superuser privileges to create an opclass. This
	 * seems necessary because we have no way to validate that the offered set
	 * of operators and functions are consistent with the AM's expectations.
	 * It would be nice to provide such a check someday, if it can be done
	 * without solving the halting problem :-(
	 *
	 * XXX re-enable NOT_USED code sections below if you remove this test.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create an operator class")));

	/* Look up the datatype */
	typeoid = typenameTypeId(NULL, stmt->datatype);

#ifdef NOT_USED
	/* XXX this is unnecessary given the superuser check above */
	/* Check we have ownership of the datatype */
	if (!pg_type_ownercheck(typeoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TYPE,
					   format_type_be(typeoid));
#endif

	operators = NIL;
	procedures = NIL;

	/* Storage datatype is optional */
	storageoid = InvalidOid;

	/*
	 * Scan the "items" list to obtain additional info.
	 */
	foreach(l, stmt->items)
	{
		CreateOpClassItem *item = lfirst(l);
		Oid			operOid;
		Oid			funcOid;
		OpClassMember *member;

		Assert(IsA(item, CreateOpClassItem));
		switch (item->itemtype)
		{
			case OPCLASS_ITEM_OPERATOR:
				if (item->number <= 0 || item->number > numOperators)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("invalid operator number %d,"
									" must be between 1 and %d",
									item->number, numOperators)));
				if (item->args != NIL)
				{
					TypeName   *typeName1 = (TypeName *) linitial(item->args);
					TypeName   *typeName2 = (TypeName *) lsecond(item->args);

					operOid = LookupOperNameTypeNames(NULL, item->name,
													  typeName1, typeName2,
													  false, -1);
				}
				else
				{
					/* Default to binary op on input datatype */
					operOid = LookupOperName(NULL, item->name,
											 typeoid, typeoid,
											 false, -1);
				}

#ifdef NOT_USED
				/* XXX this is unnecessary given the superuser check above */
				/* Caller must own operator and its underlying function */
				if (!pg_oper_ownercheck(operOid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPER,
								   get_opname(operOid));
				funcOid = get_opcode(operOid);
				if (!pg_proc_ownercheck(funcOid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
								   get_func_name(funcOid));
#endif

				/* Save the info */
				member = (OpClassMember *) palloc0(sizeof(OpClassMember));
				member->object = operOid;
				member->number = item->number;
				member->subtype = assignOperSubtype(amoid, typeoid, operOid);
				member->recheck = item->recheck;
				addClassMember(&operators, member, false);
				break;
			case OPCLASS_ITEM_FUNCTION:
				if (item->number <= 0 || item->number > numProcs)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("invalid procedure number %d,"
									" must be between 1 and %d",
									item->number, numProcs)));
				funcOid = LookupFuncNameTypeNames(item->name, item->args,
												  false);
#ifdef NOT_USED
				/* XXX this is unnecessary given the superuser check above */
				/* Caller must own function */
				if (!pg_proc_ownercheck(funcOid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC,
								   get_func_name(funcOid));
#endif

				/* Save the info */
				member = (OpClassMember *) palloc0(sizeof(OpClassMember));
				member->object = funcOid;
				member->number = item->number;
				member->subtype = assignProcSubtype(amoid, typeoid, funcOid);
				addClassMember(&procedures, member, true);
				break;
			case OPCLASS_ITEM_STORAGETYPE:
				if (OidIsValid(storageoid))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						   errmsg("storage type specified more than once")));
				storageoid = typenameTypeId(NULL, item->storedtype);

#ifdef NOT_USED
				/* XXX this is unnecessary given the superuser check above */
				/* Check we have ownership of the datatype */
				if (!pg_type_ownercheck(storageoid, GetUserId()))
					aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TYPE,
								   format_type_be(storageoid));
#endif
				break;
			default:
				elog(ERROR, "unrecognized item type: %d", item->itemtype);
				break;
		}
	}

	/*
	 * If storagetype is specified, make sure it's legal.
	 */
	if (OidIsValid(storageoid))
	{
		/* Just drop the spec if same as column datatype */
		if (storageoid == typeoid)
			storageoid = InvalidOid;
		else if (!amstorage)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("storage type may not be different from data type for access method \"%s\"",
							stmt->amname)));
	}

	rel = heap_open(OperatorClassRelationId, RowExclusiveLock);
	pcqCtx = caql_beginscan(
			caql_addrel(cqclr(&cqc), rel), 
			cql("INSERT INTO pg_opclass",
				NULL));
	/*
	 * Make sure there is no existing opclass of this name (this is just to
	 * give a more friendly error message than "duplicate key").
	 */

	cqContext		 cqcx2;

	if (caql_getcount(
				caql_addrel(cqclr(&cqcx2), rel),
				cql("SELECT COUNT(*) FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(amoid),
					CStringGetDatum(opcname),
					ObjectIdGetDatum(namespaceoid))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("operator class \"%s\" for access method \"%s\" already exists",
						opcname, stmt->amname)));
	}

	/*
	 * If we are creating a default opclass, check there isn't one already.
	 * (Note we do not restrict this test to visible opclasses; this ensures
	 * that typcache.c can find unique solutions to its questions.)
	 */
	if (stmt->isDefault)
	{
		cqContext		*pcqCtx2;
		cqContext		 cqc3;

		pcqCtx2 = caql_beginscan(
				caql_addrel(cqclr(&cqc3), rel), 
				cql("SELECT * FROM pg_opclass "
					" WHERE opcamid = :1 ",
					ObjectIdGetDatum(amoid)));

		while (HeapTupleIsValid(tup = caql_getnext(pcqCtx2)))
		{
			Form_pg_opclass opclass = (Form_pg_opclass) GETSTRUCT(tup);

			if (opclass->opcintype == typeoid && opclass->opcdefault)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_OBJECT),
						 errmsg("could not make operator class \"%s\" be default for type %s",
								opcname,
								TypeNameToString(stmt->datatype)),
				   errdetail("Operator class \"%s\" already is the default.",
							 NameStr(opclass->opcname))));
		}

		caql_endscan(pcqCtx2);
	}

	/*
	 * Okay, let's create the pg_opclass entry.
	 */
	for (i = 0; i < Natts_pg_opclass; ++i)
	{
		nulls[i] = false;
		values[i] = 0;		/* redundant, but safe */
	}

	i = 0;
	values[i++] = ObjectIdGetDatum(amoid);		/* opcamid */
	namestrcpy(&opcName, opcname);
	values[i++] = NameGetDatum(&opcName);		/* opcname */
	values[i++] = ObjectIdGetDatum(namespaceoid);		/* opcnamespace */
	values[i++] = ObjectIdGetDatum(GetUserId());		/* opcowner */
	values[i++] = ObjectIdGetDatum(typeoid);	/* opcintype */
	values[i++] = BoolGetDatum(stmt->isDefault);		/* opcdefault */
	values[i++] = ObjectIdGetDatum(storageoid); /* opckeytype */
	
	tup = caql_form_tuple(pcqCtx, values, nulls);

	if (stmt->opclassOid !=0)
		HeapTupleSetOid(tup, stmt->opclassOid);
	
	opclassoid = caql_insert(pcqCtx, tup);
	/* and Update indexes (implicit) */

	stmt->opclassOid = opclassoid;

	heap_freetuple(tup);

	/*
	 * Now add tuples to pg_amop and pg_amproc tying in the operators and
	 * functions.
	 */
	storeOperators(opclassoid, operators);
	storeProcedures(opclassoid, procedures);

	/*
	 * Create dependencies.  Note: we do not create a dependency link to the
	 * AM, because we don't currently support DROP ACCESS METHOD.
	 */
	myself.classId = OperatorClassRelationId;
	myself.objectId = opclassoid;
	myself.objectSubId = 0;

	/* dependency on namespace */
	referenced.classId = NamespaceRelationId;
	referenced.objectId = namespaceoid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on indexed datatype */
	referenced.classId = TypeRelationId;
	referenced.objectId = typeoid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* dependency on storage datatype */
	if (OidIsValid(storageoid))
	{
		referenced.classId = TypeRelationId;
		referenced.objectId = storageoid;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependencies on operators */
	foreach(l, operators)
	{
		OpClassMember *op = (OpClassMember *) lfirst(l);

		referenced.classId = OperatorRelationId;
		referenced.objectId = op->object;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependencies on procedures */
	foreach(l, procedures)
	{
		OpClassMember *proc = (OpClassMember *) lfirst(l);

		referenced.classId = ProcedureRelationId;
		referenced.objectId = proc->object;
		referenced.objectSubId = 0;
		recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
	}

	/* dependency on owner */
	recordDependencyOnOwner(OperatorClassRelationId, opclassoid, GetUserId());

	caql_endscan(pcqCtx);
	heap_close(rel, RowExclusiveLock);
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt, "DefineOpClass");
	}
}

/*
 * Determine the subtype to assign to an operator, and do any validity
 * checking we can manage
 *
 * Currently this is done using hardwired rules; we don't let the user
 * specify it directly.
 */
static Oid
assignOperSubtype(Oid amoid, Oid typeoid, Oid operOid)
{
	Oid			subtype;
	Operator	optup;
	Form_pg_operator opform;
	cqContext  *pcqCtx;

	/* Subtypes are currently only supported by btree and bitmap, others use 0 */
	if (amoid != BTREE_AM_OID && amoid != BITMAP_AM_OID)
		return InvalidOid;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_operator "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(operOid)));

	optup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(optup))
		elog(ERROR, "cache lookup failed for operator %u", operOid);
	opform = (Form_pg_operator) GETSTRUCT(optup);

	/*
	 * btree operators must be binary ops returning boolean, and the left-side
	 * input type must match the operator class' input type.
	 */
	if (opform->oprkind != 'b')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("btree operators must be binary")));
	if (opform->oprresult != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("btree operators must return boolean")));
	if (opform->oprleft != typeoid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
			  errmsg("btree operators must have index type as left input")));

	/*
	 * The subtype is "default" (0) if oprright matches the operator class,
	 * otherwise it is oprright.
	 */
	if (opform->oprright == typeoid)
		subtype = InvalidOid;
	else
		subtype = opform->oprright;

	caql_endscan(pcqCtx);
	return subtype;
}

/*
 * Determine the subtype to assign to a support procedure, and do any validity
 * checking we can manage
 *
 * Currently this is done using hardwired rules; we don't let the user
 * specify it directly.
 */
static Oid
assignProcSubtype(Oid amoid, Oid typeoid, Oid procOid)
{
	Oid			subtype;
	HeapTuple	proctup;
	Form_pg_proc procform;
	cqContext  *pcqCtx;

	/* Subtypes are currently only supported by btree and bitmap, others use 0 */
	if (amoid != BTREE_AM_OID && amoid != BITMAP_AM_OID)
		return InvalidOid;

	pcqCtx = caql_beginscan(
			NULL,
			cql("SELECT * FROM pg_proc "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(procOid)));

	proctup = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", procOid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/*
	 * btree support procs must be 2-arg procs returning int4, and the first
	 * input type must match the operator class' input type.
	 */
	if (procform->pronargs != 2)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("btree procedures must have two arguments")));
	if (procform->prorettype != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("btree procedures must return integer")));
	if (procform->proargtypes.values[0] != typeoid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
			errmsg("btree procedures must have index type as first input")));

	/*
	 * The subtype is "default" (0) if second input type matches the operator
	 * class, otherwise it is the second input type.
	 */
	if (procform->proargtypes.values[1] == typeoid)
		subtype = InvalidOid;
	else
		subtype = procform->proargtypes.values[1];

	caql_endscan(pcqCtx);
	return subtype;
}

/*
 * Add a new class member to the appropriate list, after checking for
 * duplicated strategy or proc number.
 */
static void
addClassMember(List **list, OpClassMember *member, bool isProc)
{
	ListCell   *l;

	foreach(l, *list)
	{
		OpClassMember *old = (OpClassMember *) lfirst(l);

		if (old->number == member->number &&
			old->subtype == member->subtype)
		{
			if (isProc)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("procedure number %d appears more than once",
								member->number)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("operator number %d appears more than once",
								member->number)));
		}
	}
	*list = lappend(*list, member);
}

/*
 * Dump the operators to pg_amop
 */
static void
storeOperators(Oid opclassoid, List *operators)
{
	Datum		values[Natts_pg_amop];
	bool		nulls[Natts_pg_amop];
	HeapTuple	tup;
	ListCell   *l;
	int			i;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_amop", 
				NULL));

	foreach(l, operators)
	{
		OpClassMember *op = (OpClassMember *) lfirst(l);

		for (i = 0; i < Natts_pg_amop; ++i)
		{
			nulls[i] = false;
			values[i] = 0;
		}

		i = 0;
		values[i++] = ObjectIdGetDatum(opclassoid);		/* amopclaid */
		values[i++] = ObjectIdGetDatum(op->subtype);	/* amopsubtype */
		values[i++] = Int16GetDatum(op->number);		/* amopstrategy */
		values[i++] = BoolGetDatum(op->recheck);		/* amopreqcheck */
		values[i++] = ObjectIdGetDatum(op->object);		/* amopopr */

		tup = caql_form_tuple(pcqCtx, values, nulls);

		caql_insert(pcqCtx, tup); /* implicit update of index as well */

		heap_freetuple(tup);
	}

	caql_endscan(pcqCtx);
}

/*
 * Dump the procedures (support routines) to pg_amproc
 */
static void
storeProcedures(Oid opclassoid, List *procedures)
{
	Datum		values[Natts_pg_amproc];
	bool		nulls[Natts_pg_amproc];
	HeapTuple	tup;
	ListCell   *l;
	int			i;
	cqContext  *pcqCtx;

	pcqCtx = caql_beginscan(
			NULL,
			cql("INSERT INTO pg_amproc", 
				NULL));

	foreach(l, procedures)
	{
		OpClassMember *proc = (OpClassMember *) lfirst(l);

		for (i = 0; i < Natts_pg_amproc; ++i)
		{
			nulls[i] = false;
			values[i] = 0;
		}

		i = 0;
		values[i++] = ObjectIdGetDatum(opclassoid);		/* amopclaid */
		values[i++] = ObjectIdGetDatum(proc->subtype);	/* amprocsubtype */
		values[i++] = Int16GetDatum(proc->number);		/* amprocnum */
		values[i++] = ObjectIdGetDatum(proc->object);	/* amproc */

		tup = caql_form_tuple(pcqCtx, values, nulls);

		caql_insert(pcqCtx, tup); /* implicit update of index as well */

		heap_freetuple(tup);
	}

	caql_endscan(pcqCtx);
}


/*
 * RemoveOpClass
 *		Deletes an opclass.
 */
void
RemoveOpClass(RemoveOpClassStmt *stmt)
{
	Oid			amID,
				opcID;
	char	   *schemaname;
	char	   *opcname;
	ObjectAddress object;
	HeapTuple	tuple;
	cqContext  *pcqCtx;

	/*
	 * Get the access method's OID.
	 */
	amID = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_am "
				" WHERE amname = :1 ",
				CStringGetDatum(stmt->amname)));

	if (!OidIsValid(amID))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist",
						stmt->amname)));

	/*
	 * Look up the opclass.
	 */

	/* deconstruct the name list */
	DeconstructQualifiedName(stmt->opclassname, &schemaname, &opcname);

	if (schemaname)
	{
		/* Look in specific schema only */
		Oid			namespaceId;

		namespaceId = LookupExplicitNamespace(schemaname);

		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(amID),
					CStringGetDatum(opcname),
					ObjectIdGetDatum(namespaceId)));
	}
	else
	{
		Oid	opcID1;
		/* Unqualified opclass name, so search the search path */
		opcID1 = OpclassnameGetOpcid(amID, opcname);
		if (!OidIsValid(opcID1))
		{
			if (!stmt->missing_ok)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
								opcname, stmt->amname)));
			else
				ereport(NOTICE,
						(errmsg("operator class \"%s\" does not exist for access method \"%s\"",
								opcname, stmt->amname)));

			return;
		}


		pcqCtx = caql_beginscan(
				NULL,
				cql("SELECT * FROM pg_opclass "
					" WHERE oid = :1 ",
					ObjectIdGetDatum(opcID1)));
	}

	tuple = caql_getnext(pcqCtx);

	if (!HeapTupleIsValid(tuple))
	{

		if (!stmt->missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
						NameListToString(stmt->opclassname), stmt->amname)));
		else
			ereport(NOTICE,
					(errmsg("operator class \"%s\" does not exist for access method \"%s\"",
						NameListToString(stmt->opclassname), stmt->amname)));
		return;
	}

	opcID = HeapTupleGetOid(tuple);

	/* Permission check: must own opclass or its namespace */
	if (!pg_opclass_ownercheck(opcID, GetUserId()) &&
		!pg_namespace_ownercheck(((Form_pg_opclass) GETSTRUCT(tuple))->opcnamespace,
								 GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS,
					   NameListToString(stmt->opclassname));

	caql_endscan(pcqCtx);

	/*
	 * Do the deletion
	 */
	object.classId = OperatorClassRelationId;
	object.objectId = opcID;
	object.objectSubId = 0;

	performDeletion(&object, stmt->behavior);
	
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt, "RemoveOpClass");
	}
}

/*
 * Guts of opclass deletion.
 */
void
RemoveOpClassById(Oid opclassOid)
{
	int			numDel;

	/*
	 * First remove the pg_opclass entry itself.
	 */

	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_opclass "
				" WHERE oid = :1 ",
				ObjectIdGetDatum(opclassOid)));

	/*
	 * Remove associated entries in pg_amop.
	 */
	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_amop "
				" WHERE amopclaid = :1 ",
				ObjectIdGetDatum(opclassOid)));

	/*
	 * Remove associated entries in pg_amproc.
	 */
	numDel = caql_getcount(
			NULL,
			cql("DELETE FROM pg_amproc "
				" WHERE amopclaid = :1 ",
				ObjectIdGetDatum(opclassOid)));
}


/*
 * Rename opclass
 */
void
RenameOpClass(List *name, const char *access_method, const char *newname)
{
	Oid			opcOid;
	Oid			amOid;
	Oid			namespaceOid;
	char	   *schemaname;
	char	   *opcname;
	HeapTuple	tup;
	Relation	rel;
	AclResult	aclresult;
	cqContext	cqc2;
	cqContext	cqc;
	cqContext  *pcqCtx;

	amOid = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_am "
				" WHERE amname = :1 ",
				CStringGetDatum((char *) access_method)));

	if (!OidIsValid(amOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist",
						access_method)));

	rel = heap_open(OperatorClassRelationId, RowExclusiveLock);

	/*
	 * Look up the opclass
	 */
	DeconstructQualifiedName(name, &schemaname, &opcname);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	if (schemaname)
	{
		namespaceOid = LookupExplicitNamespace(schemaname);

		tup = caql_getfirst(
				pcqCtx,
				cql("SELECT * FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 "
					" FOR UPDATE ",
					ObjectIdGetDatum(amOid),
					CStringGetDatum(opcname),
					ObjectIdGetDatum(namespaceOid)));

		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, access_method)));

		opcOid = HeapTupleGetOid(tup);
	}
	else
	{
		opcOid = OpclassnameGetOpcid(amOid, opcname);
		if (!OidIsValid(opcOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, access_method)));

		tup = caql_getfirst(
				pcqCtx,
				cql("SELECT * FROM pg_opclass "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(opcOid)));

		if (!HeapTupleIsValid(tup))		/* should not happen */
			elog(ERROR, "cache lookup failed for opclass %u", opcOid);

		namespaceOid = ((Form_pg_opclass) GETSTRUCT(tup))->opcnamespace;
	}

	/* make sure the new name doesn't exist */
	if (caql_getcount(
				caql_addrel(cqclr(&cqc2), rel),
				cql("SELECT COUNT(*) FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 ",
					ObjectIdGetDatum(amOid),
					CStringGetDatum((char *) newname),
					ObjectIdGetDatum(namespaceOid))))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("operator class \"%s\" for access method \"%s\" already exists in schema \"%s\"",
						newname, access_method,
						get_namespace_name(namespaceOid))));
	}

	/* must be owner */
	if (!pg_opclass_ownercheck(opcOid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS,
					   NameListToString(name));

	/* must have CREATE privilege on namespace */
	aclresult = pg_namespace_aclcheck(namespaceOid, GetUserId(), ACL_CREATE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(namespaceOid));

	/* rename */
	namestrcpy(&(((Form_pg_opclass) GETSTRUCT(tup))->opcname), newname);
	caql_update_current(pcqCtx, tup); /* implicit update of index as well */

	heap_close(rel, NoLock);
	heap_freetuple(tup);
}

/*
 * Change opclass owner by oid
 */
#ifdef NOT_USED
void
AlterOpClassOwner_oid(Oid opcOid, Oid newOwnerId)
{
	Relation	rel;
	HeapTuple	tup;
	cqContext	cqc;

	rel = heap_open(OperatorClassRelationId, RowExclusiveLock);

	tup = caql_getfirst(
			caql_addrel(cqclr(&cqc), rel),
				cql("SELECT * FROM pg_opclass "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(opcOid)));

	if (!HeapTupleIsValid(tup)) /* shouldn't happen */
		elog(ERROR, "cache lookup failed for opclass %u", opcOid);

	AlterOpClassOwner_internal(&cqc, rel, tup, newOwnerId);

	heap_freetuple(tup);
	heap_close(rel, NoLock);
}
#endif

/*
 * Change opclass owner by name
 */
void
AlterOpClassOwner(List *name, const char *access_method, Oid newOwnerId)
{
	Oid			amOid;
	Relation	rel;
	HeapTuple	tup;
	char	   *opcname;
	char	   *schemaname;
	cqContext	cqc;
	cqContext  *pcqCtx;

	amOid = caql_getoid(
			NULL,
			cql("SELECT oid FROM pg_am "
				" WHERE amname = :1 ",
				CStringGetDatum((char *) access_method)));

	if (!OidIsValid(amOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("access method \"%s\" does not exist",
						access_method)));

	rel = heap_open(OperatorClassRelationId, RowExclusiveLock);

	/*
	 * Look up the opclass
	 */
	DeconstructQualifiedName(name, &schemaname, &opcname);

	pcqCtx = caql_addrel(cqclr(&cqc), rel);

	if (schemaname)
	{
		Oid			namespaceOid;

		namespaceOid = LookupExplicitNamespace(schemaname);

		tup = caql_getfirst(
				pcqCtx,
				cql("SELECT * FROM pg_opclass "
					" WHERE opcamid = :1 "
					" AND opcname = :2 "
					" AND opcnamespace = :3 "
					" FOR UPDATE ",
					ObjectIdGetDatum(amOid),
					CStringGetDatum(opcname),
					ObjectIdGetDatum(namespaceOid)));

		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, access_method)));
	}
	else
	{
		Oid			opcOid;

		opcOid = OpclassnameGetOpcid(amOid, opcname);
		if (!OidIsValid(opcOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("operator class \"%s\" does not exist for access method \"%s\"",
							opcname, access_method)));

		tup = caql_getfirst(
				pcqCtx,
				cql("SELECT * FROM pg_opclass "
					" WHERE oid = :1 "
					" FOR UPDATE ",
					ObjectIdGetDatum(opcOid)));

		if (!HeapTupleIsValid(tup))		/* should not happen */
			elog(ERROR, "cache lookup failed for opclass %u", opcOid);
	}

	AlterOpClassOwner_internal(pcqCtx, rel, tup, newOwnerId);

	heap_freetuple(tup);
	heap_close(rel, NoLock);
}

/*
 * The zeroeth parameter is the caql context, with a single valid tuple.
 * The first parameter is pg_opclass, opened and suitably locked.  The second
 * parameter is the tuple from pg_opclass we want to modify.
 */
static void
AlterOpClassOwner_internal(cqContext *pcqCtx,
						   Relation rel, HeapTuple tup, Oid newOwnerId)
{
	Oid			namespaceOid;
	AclResult	aclresult;
	Form_pg_opclass opcForm;

	Assert(RelationGetRelid(rel) == OperatorClassRelationId);

	opcForm = (Form_pg_opclass) GETSTRUCT(tup);

	namespaceOid = opcForm->opcnamespace;

	/*
	 * If the new owner is the same as the existing owner, consider the
	 * command to have succeeded.  This is for dump restoration purposes.
	 */
	if (opcForm->opcowner == newOwnerId)
		return;

	/* Superusers can always do it */
	if (!superuser())
	{
		/* Otherwise, must be owner of the existing object */
		if (!pg_opclass_ownercheck(HeapTupleGetOid(tup), GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_OPCLASS,
						   NameStr(opcForm->opcname));

		/* Must be able to become new owner */
		check_is_member_of_role(GetUserId(), newOwnerId);

		/* New owner must have CREATE privilege on namespace */
		aclresult = pg_namespace_aclcheck(namespaceOid, newOwnerId,
										  ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
						   get_namespace_name(namespaceOid));
	}

	/*
	 * Modify the owner --- okay to scribble on tup because it's a copy
	 */
	opcForm->opcowner = newOwnerId;

	caql_update_current(pcqCtx, tup); /* implicit update of index as well */

	/* Update owner dependency reference */
	changeDependencyOnOwner(OperatorClassRelationId, HeapTupleGetOid(tup),
							newOwnerId);
}
