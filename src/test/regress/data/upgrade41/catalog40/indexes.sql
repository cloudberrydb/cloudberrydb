-- Indexes defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
--
/*
\o /tmp/index
SELECT 
   CASE when indisprimary then -- (primary key index)
        'BROKEN(primary key)'  -- (none in catalog, this would be an ALTER TABLE)
        when indisunique  then -- (unique index)
          'CREATE UNIQUE INDEX '
        else                   -- (normal index)
          'CREATE INDEX ' 
        end 
   || quote_ident(c.relname) || ' ON '
   || 'upg_catalog.' || quote_ident(rel.relname) || ' '
   || 'USING ' || am.amname || '('
   || array_to_string(array(
         select attname
         from pg_attribute, generate_series(array_lower(indkey,1), array_upper(indkey,1)) i
         where attrelid=indrelid and attnum = indkey[i]
         order by i), ', ')         
   || ');'
FROM pg_index 
     join pg_class c ON (indexrelid = c.oid)
     join pg_class rel ON (indrelid   = rel.oid)
     join pg_namespace n ON (c.relnamespace = n.oid)
     join pg_am am ON (c.relam = am.oid)
WHERE n.nspname = 'pg_catalog';
*/
 CREATE INDEX pg_authid_rolresqueue_index ON upg_catalog.pg_authid USING btree(rolresqueue);
 CREATE UNIQUE INDEX pg_authid_oid_index ON upg_catalog.pg_authid USING btree(oid);
 CREATE UNIQUE INDEX pg_authid_rolname_index ON upg_catalog.pg_authid USING btree(rolname);
 CREATE UNIQUE INDEX pg_statistic_relid_att_index ON upg_catalog.pg_statistic USING btree(starelid, staattnum);
 CREATE UNIQUE INDEX pg_user_mapping_user_server_index ON upg_catalog.pg_user_mapping USING btree(umuser, umserver);
 CREATE UNIQUE INDEX pg_user_mapping_oid_index ON upg_catalog.pg_user_mapping USING btree(oid);
 CREATE UNIQUE INDEX pg_type_typname_nsp_index ON upg_catalog.pg_type USING btree(typname, typnamespace);
 CREATE UNIQUE INDEX pg_type_oid_index ON upg_catalog.pg_type USING btree(oid);
 CREATE UNIQUE INDEX pg_attribute_relid_attnum_index ON upg_catalog.pg_attribute USING btree(attrelid, attnum);
 CREATE UNIQUE INDEX pg_attribute_relid_attnam_index ON upg_catalog.pg_attribute USING btree(attrelid, attname);
 CREATE UNIQUE INDEX pg_proc_proname_args_nsp_index ON upg_catalog.pg_proc USING btree(proname, proargtypes, pronamespace);
 CREATE UNIQUE INDEX pg_proc_oid_index ON upg_catalog.pg_proc USING btree(oid);
 CREATE UNIQUE INDEX pg_class_relname_nsp_index ON upg_catalog.pg_class USING btree(relname, relnamespace);
 CREATE UNIQUE INDEX pg_class_oid_index ON upg_catalog.pg_class USING btree(oid);
 CREATE UNIQUE INDEX pg_autovacuum_vacrelid_index ON upg_catalog.pg_autovacuum USING btree(vacrelid);
 CREATE UNIQUE INDEX pg_attrdef_oid_index ON upg_catalog.pg_attrdef USING btree(oid);
 CREATE UNIQUE INDEX pg_attrdef_adrelid_adnum_index ON upg_catalog.pg_attrdef USING btree(adrelid, adnum);
 CREATE UNIQUE INDEX pg_constraint_oid_index ON upg_catalog.pg_constraint USING btree(oid);
 CREATE INDEX pg_constraint_contypid_index ON upg_catalog.pg_constraint USING btree(contypid);
 CREATE INDEX pg_constraint_conrelid_index ON upg_catalog.pg_constraint USING btree(conrelid);
 CREATE INDEX pg_constraint_conname_nsp_index ON upg_catalog.pg_constraint USING btree(conname, connamespace);
 CREATE UNIQUE INDEX pg_inherits_relid_seqno_index ON upg_catalog.pg_inherits USING btree(inhrelid, inhseqno);
 CREATE UNIQUE INDEX pg_index_indexrelid_index ON upg_catalog.pg_index USING btree(indexrelid);
 CREATE INDEX pg_index_indrelid_index ON upg_catalog.pg_index USING btree(indrelid);
 CREATE UNIQUE INDEX pg_operator_oprname_l_r_n_index ON upg_catalog.pg_operator USING btree(oprname, oprleft, oprright, oprnamespace);
 CREATE UNIQUE INDEX pg_operator_oid_index ON upg_catalog.pg_operator USING btree(oid);
 CREATE UNIQUE INDEX pg_opclass_oid_index ON upg_catalog.pg_opclass USING btree(oid);
 CREATE UNIQUE INDEX pg_opclass_am_name_nsp_index ON upg_catalog.pg_opclass USING btree(opcamid, opcname, opcnamespace);
 CREATE UNIQUE INDEX pg_am_oid_index ON upg_catalog.pg_am USING btree(oid);
 CREATE UNIQUE INDEX pg_am_name_index ON upg_catalog.pg_am USING btree(amname);
 CREATE UNIQUE INDEX pg_amop_opr_opc_index ON upg_catalog.pg_amop USING btree(amopopr, amopclaid);
 CREATE UNIQUE INDEX pg_amop_opc_strat_index ON upg_catalog.pg_amop USING btree(amopclaid, amopsubtype, amopstrategy);
 CREATE UNIQUE INDEX pg_amproc_opc_proc_index ON upg_catalog.pg_amproc USING btree(amopclaid, amprocsubtype, amprocnum);
 CREATE UNIQUE INDEX pg_language_oid_index ON upg_catalog.pg_language USING btree(oid);
 CREATE UNIQUE INDEX pg_language_name_index ON upg_catalog.pg_language USING btree(lanname);
 CREATE UNIQUE INDEX pg_largeobject_loid_pn_index ON upg_catalog.pg_largeobject USING btree(loid, pageno);
 CREATE UNIQUE INDEX pg_aggregate_fnoid_index ON upg_catalog.pg_aggregate USING btree(aggfnoid);
 CREATE UNIQUE INDEX pg_statlastop_classid_objid_staactionname_index ON upg_catalog.pg_stat_last_operation USING btree(classid, objid, staactionname);
 CREATE INDEX pg_statlastop_classid_objid_index ON upg_catalog.pg_stat_last_operation USING btree(classid, objid);
 CREATE UNIQUE INDEX pg_statlastshop_classid_objid_staactionname_index ON upg_catalog.pg_stat_last_shoperation USING btree(classid, objid, staactionname);
 CREATE INDEX pg_statlastshop_classid_objid_index ON upg_catalog.pg_stat_last_shoperation USING btree(classid, objid);
 CREATE UNIQUE INDEX pg_rewrite_rel_rulename_index ON upg_catalog.pg_rewrite USING btree(ev_class, rulename);
 CREATE UNIQUE INDEX pg_rewrite_oid_index ON upg_catalog.pg_rewrite USING btree(oid);
 CREATE UNIQUE INDEX pg_trigger_oid_index ON upg_catalog.pg_trigger USING btree(oid);
 CREATE UNIQUE INDEX pg_trigger_tgrelid_tgname_index ON upg_catalog.pg_trigger USING btree(tgrelid, tgname);
 CREATE INDEX pg_trigger_tgconstrrelid_index ON upg_catalog.pg_trigger USING btree(tgconstrrelid);
 CREATE INDEX pg_trigger_tgconstrname_index ON upg_catalog.pg_trigger USING btree(tgconstrname);
 CREATE UNIQUE INDEX pg_description_o_c_o_index ON upg_catalog.pg_description USING btree(objoid, classoid, objsubid);
 CREATE UNIQUE INDEX pg_cast_source_target_index ON upg_catalog.pg_cast USING btree(castsource, casttarget);
 CREATE UNIQUE INDEX pg_cast_oid_index ON upg_catalog.pg_cast USING btree(oid);
 CREATE UNIQUE INDEX pg_namespace_oid_index ON upg_catalog.pg_namespace USING btree(oid);
 CREATE UNIQUE INDEX pg_namespace_nspname_index ON upg_catalog.pg_namespace USING btree(nspname);
 CREATE UNIQUE INDEX pg_conversion_oid_index ON upg_catalog.pg_conversion USING btree(oid);
 CREATE UNIQUE INDEX pg_conversion_name_nsp_index ON upg_catalog.pg_conversion USING btree(conname, connamespace);
 CREATE UNIQUE INDEX pg_conversion_default_index ON upg_catalog.pg_conversion USING btree(connamespace, conforencoding, contoencoding, oid);
 CREATE INDEX pg_depend_reference_index ON upg_catalog.pg_depend USING btree(refclassid, refobjid, refobjsubid);
 CREATE INDEX pg_depend_depender_index ON upg_catalog.pg_depend USING btree(classid, objid, objsubid);
 CREATE UNIQUE INDEX pg_tablespace_spcname_index ON upg_catalog.pg_tablespace USING btree(spcname);
 CREATE UNIQUE INDEX pg_tablespace_oid_index ON upg_catalog.pg_tablespace USING btree(oid);
 CREATE UNIQUE INDEX pg_pltemplate_name_index ON upg_catalog.pg_pltemplate USING btree(tmplname);
 CREATE INDEX pg_shdepend_reference_index ON upg_catalog.pg_shdepend USING btree(refclassid, refobjid);
 CREATE INDEX pg_shdepend_depender_index ON upg_catalog.pg_shdepend USING btree(dbid, classid, objid);
 CREATE UNIQUE INDEX pg_shdescription_o_c_index ON upg_catalog.pg_shdescription USING btree(objoid, classoid);
 CREATE UNIQUE INDEX pg_resqueue_rsqname_index ON upg_catalog.pg_resqueue USING btree(rsqname);
 CREATE UNIQUE INDEX pg_resqueue_oid_index ON upg_catalog.pg_resqueue USING btree(oid);
 CREATE UNIQUE INDEX pg_resourcetype_resname_index ON upg_catalog.pg_resourcetype USING btree(resname);
 CREATE UNIQUE INDEX pg_resourcetype_restypid_index ON upg_catalog.pg_resourcetype USING btree(restypid);
 CREATE UNIQUE INDEX pg_resourcetype_oid_index ON upg_catalog.pg_resourcetype USING btree(oid);
 CREATE INDEX pg_resqueuecapability_restypid_index ON upg_catalog.pg_resqueuecapability USING btree(restypid);
 CREATE INDEX pg_resqueuecapability_resqueueid_index ON upg_catalog.pg_resqueuecapability USING btree(resqueueid);
 CREATE UNIQUE INDEX pg_resqueuecapability_oid_index ON upg_catalog.pg_resqueuecapability USING btree(oid);
 CREATE UNIQUE INDEX gp_configuration_dbid_index ON upg_catalog.gp_configuration USING btree(dbid);
 CREATE UNIQUE INDEX gp_configuration_content_definedprimary_index ON upg_catalog.gp_configuration USING btree(content, definedprimary);
 CREATE INDEX gp_db_interfaces_dbid_index ON upg_catalog.gp_db_interfaces USING btree(dbid);
 CREATE UNIQUE INDEX gp_interfaces_interface_index ON upg_catalog.gp_interfaces USING btree(interfaceid);
 CREATE UNIQUE INDEX gp_policy_localoid_index ON upg_catalog.gp_distribution_policy USING btree(localoid);
 CREATE UNIQUE INDEX gp_segment_config_dbid_index ON upg_catalog.gp_segment_configuration USING btree(dbid);
 CREATE UNIQUE INDEX gp_segment_config_content_preferred_role_index ON upg_catalog.gp_segment_configuration USING btree(content, preferred_role);
 CREATE UNIQUE INDEX gp_san_config_mountid_index ON upg_catalog.gp_san_configuration USING btree(mountid);
 CREATE UNIQUE INDEX pg_window_fnoid_index ON upg_catalog.pg_window USING btree(winfnoid);
 CREATE UNIQUE INDEX pg_exttable_reloid_index ON upg_catalog.pg_exttable USING btree(reloid);
 CREATE UNIQUE INDEX pg_appendonly_relid_index ON upg_catalog.pg_appendonly USING btree(relid);
 CREATE UNIQUE INDEX pg_appendonly_alter_column_relid_index ON upg_catalog.pg_appendonly_alter_column USING btree(relid, changenum);
 CREATE UNIQUE INDEX gp_fastsequence_objid_objmod_index ON upg_catalog.gp_fastsequence USING btree(objid, objmod);
 CREATE INDEX pg_partition_parrelid_parlevel_istemplate_index ON upg_catalog.pg_partition USING btree(parrelid, parlevel, paristemplate);
 CREATE INDEX pg_partition_parrelid_index ON upg_catalog.pg_partition USING btree(parrelid);
 CREATE UNIQUE INDEX pg_partition_oid_index ON upg_catalog.pg_partition USING btree(oid);
 CREATE INDEX pg_partition_rule_paroid_parentrule_ruleord_index ON upg_catalog.pg_partition_rule USING btree(paroid, parparentrule, parruleord);
 CREATE INDEX pg_partition_rule_parchildrelid_parparentrule_parruleord_index ON upg_catalog.pg_partition_rule USING btree(parchildrelid, parparentrule, parruleord);
 CREATE INDEX pg_partition_rule_parchildrelid_index ON upg_catalog.pg_partition_rule USING btree(parchildrelid);
 CREATE UNIQUE INDEX pg_partition_rule_oid_index ON upg_catalog.pg_partition_rule USING btree(oid);
 CREATE UNIQUE INDEX pg_filespace_fsname_index ON upg_catalog.pg_filespace USING btree(fsname);
 CREATE UNIQUE INDEX pg_filespace_oid_index ON upg_catalog.pg_filespace USING btree(oid);
 CREATE UNIQUE INDEX pg_filespace_entry_fsdb_index ON upg_catalog.pg_filespace_entry USING btree(fsefsoid, fsedbid);
 CREATE INDEX pg_filespace_entry_fs_index ON upg_catalog.pg_filespace_entry USING btree(fsefsoid);
 CREATE UNIQUE INDEX gp_relation_node_index ON upg_catalog.gp_relation_node USING btree(relfilenode_oid, segment_file_num);
 CREATE UNIQUE INDEX pg_foreign_data_wrapper_name_index ON upg_catalog.pg_foreign_data_wrapper USING btree(fdwname);
 CREATE UNIQUE INDEX pg_foreign_data_wrapper_oid_index ON upg_catalog.pg_foreign_data_wrapper USING btree(oid);
 CREATE UNIQUE INDEX pg_foreign_table_reloid_index ON upg_catalog.pg_foreign_table USING btree(reloid);
 CREATE UNIQUE INDEX pg_foreign_server_name_index ON upg_catalog.pg_foreign_server USING btree(srvname);
 CREATE UNIQUE INDEX pg_foreign_server_oid_index ON upg_catalog.pg_foreign_server USING btree(oid);
 CREATE UNIQUE INDEX pg_database_oid_index ON upg_catalog.pg_database USING btree(oid);
 CREATE UNIQUE INDEX pg_database_datname_index ON upg_catalog.pg_database USING btree(datname);
 CREATE UNIQUE INDEX pg_auth_members_member_role_index ON upg_catalog.pg_auth_members USING btree(member, roleid);
 CREATE UNIQUE INDEX pg_auth_members_role_member_index ON upg_catalog.pg_auth_members USING btree(roleid, member);
