#include "postgres.h"

#include "utils/acl.h"

PG_FUNCTION_INFO_V1(gp_add_truncate_priv_for_owner);
Datum gp_add_truncate_priv_for_owner(PG_FUNCTION_ARGS);

/*
 * gp_add_truncate_priv_for_owner adds the truncate bit 'D' to the
 * AclItem with grantee == grantor == owner, during migration from
 * 4.2.x to 4.3.3+, where new truncate bit is introduced.
 */
Datum
gp_add_truncate_priv_for_owner(PG_FUNCTION_ARGS)
{
	Acl		   *acl = PG_GETARG_ACL_P(0);
	Oid			ownerId = PG_GETARG_OID(1);
	AclItem		aclitem;
	int			modechg = ACL_MODECHG_ADD;
	DropBehavior	behavior = DROP_RESTRICT; /* doesn't matter */

	aclitem.ai_grantee = ownerId;
	aclitem.ai_grantor = ownerId;
	ACLITEM_SET_PRIVS_GOPTIONS(aclitem, ACL_TRUNCATE, ACL_NO_RIGHTS);

	acl = aclupdate(acl, &aclitem, modechg, ownerId, behavior, NULL);

	PG_RETURN_ACL_P(acl);
}
