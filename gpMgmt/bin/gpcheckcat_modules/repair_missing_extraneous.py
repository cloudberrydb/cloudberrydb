#!/usr/bin/env python
from gppylib.operations.backup_utils import escapeDoubleQuoteInSQLString


class RepairMissingExtraneous:

    def __init__(self, catalog_name,  issues, pk_name):
        self._catalog_name = catalog_name
        self._issues = issues
        if pk_name is None:
            pk_name = 'oid'
        self._pk_name = pk_name

    def get_delete_sql(self, oids):
        escaped_catalog_name = escapeDoubleQuoteInSQLString(self._catalog_name)
        escaped_pk_name = escapeDoubleQuoteInSQLString(self._pk_name)

        delete_sql = 'BEGIN;set allow_system_table_mods="dml";delete from {0} where {1} in ({2});COMMIT;'

        return delete_sql.format(escaped_catalog_name, escaped_pk_name, ','.join(str(oid) for oid in oids))

    def get_segment_to_oid_mapping(self, all_seg_ids):
        if not self._issues:
            return

        #   issues look like this
        #   [(49401, "extra", '{1,2}'),
        #    (49401, "extra", '{1,2}')]
        #               OR
        #   [(49401, 'cmax', "extra", '{1,2}'),
        #    (49401, 'cmax', "extra", '{1,2}')]

        all_seg_ids = set([str(seg_id) for seg_id in all_seg_ids])
        oids_to_segment_mapping = {}
        for issue in self._issues:

            oid = issue[0]

            issue_type = issue[-2]
            seg_ids = issue[-1].strip('{}').split(',')

            # if an oid is missing from a segment(s) , then it is considered to be extra
            # on all the other segments/master
            if issue_type == "missing":
                seg_ids = all_seg_ids - set(seg_ids)

            for seg_id in seg_ids:
                seg_id = int(seg_id)

                if not oids_to_segment_mapping.has_key(seg_id):
                    oids_to_segment_mapping[seg_id] = set()

                oids_to_segment_mapping[seg_id].add(oid)

        return oids_to_segment_mapping

