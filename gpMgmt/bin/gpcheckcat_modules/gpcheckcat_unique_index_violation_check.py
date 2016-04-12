#!/usr/bin/env python

class UniqueIndexViolationCheck:
    unique_indexes_query = """
        select table_class.oid as tableoid, index_class.relname as indexname, table_class.relname as tablename, substring(pg_get_indexdef(index_class.oid) from '%#(#"%#"#)%' for '#') as columnnames
        from pg_index
        join pg_class table_class on table_class.oid = pg_index.indrelid
        join pg_class index_class on index_class.oid = pg_index.indexrelid
        left join pg_namespace on pg_namespace.oid = table_class.relnamespace
        left join pg_tablespace on pg_tablespace.oid = index_class.reltablespace
        where table_class.relkind = 'r'::"char" and index_class.relkind = 'i'::"char" and pg_namespace.nspname = 'pg_catalog' and pg_index.indisunique='t';
    """

    def __init__(self):
        self.violated_index_query = """
            (select gp_segment_id, %s, count(*)
            from gp_dist_random('%s')
            where (%s) is not null
            group by gp_segment_id, %s
            having count(*) > 1)
            union
            (select gp_segment_id, %s, count(*)
            from %s
            where (%s) is not null
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
            column_names, table_name, column_names, column_names, column_names, table_name, column_names, column_names
        )