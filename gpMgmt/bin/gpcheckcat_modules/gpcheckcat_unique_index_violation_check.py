#!/usr/bin/env python

class UniqueIndexViolationCheck:
    unique_indexes_query = """
        select pg_class.oid as tableoid, indexname, tablename, substring(indexdef from '%#(#"%#"#)%' for '#') as columnnames
        from pg_indexes
        join pg_class on relname = tablename
        where indexdef like 'CREATE UNIQUE INDEX%'
        and schemaname='pg_catalog'
        and relkind = 'r';
    """

    def __init__(self):
        self.violated_index_query = """
            (select gp_segment_id, %s, count(*)
            from gp_dist_random('%s')
            group by gp_segment_id, %s
            having count(*) > 1)
            union
            (select gp_segment_id, %s, count(*)
            from %s
            group by gp_segment_id, %s
            having count(*) > 1)
        """


    def runCheck(self, db_connection):
        unique_indexes = db_connection.query(self.unique_indexes_query).getresult()
        violations = []

        for (table_oid, index_name, table_name, column_names) in unique_indexes:
            sql = self.get_violated_index_query(table_name, column_names)
            violated_indexes = db_connection.query(sql).getresult()
            for violated_index in violated_indexes:
                violations.append(dict(table_oid=table_oid,
                                       table_name=table_name,
                                       index_name=index_name,
                                       segment_id=violated_index[0]))

        return violations

    def get_violated_index_query(self, table_name, column_names):
        return self.violated_index_query % (
            column_names, table_name, column_names, column_names, table_name, column_names
        )