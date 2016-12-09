-- @author ramans2
-- @created 2013-08-12 12:00:00
-- @modified 2014-03-14 12:00:00
-- @gucs join_collapse_limit = 15;gp_enable_sequential_window_plans=off
-- @db_name idwh001p
-- @gpdiff False
-- @description Modified MPP-20785
-- @product_version gpdb:[4.3.0.0-MAIN]

select 20785 as explain_test;

explain analyze select * from (((((((((((((((((((((((((((((((("ddwh02_dim"."tdim_time_time" "Data_OCF" 

 inner join (select "V_Vehicle"."pvan_cd_cust_po_num" as "pvan_cd_cust_po_num" , "V_Vehicle"."pvan_fl_ncf_status" as "pvan_fl_ncf_status" 
 , "V_Vehicle"."pvan_id_crt_wrt_str_date" as "pvan_id_crt_wrt_str_date" , "V_Vehicle"."pvan_id_fdp" as "pvan_id_fdp" 
 , "V_Vehicle"."pvan_id_res_customer" as "pvan_id_res_customer" , "V_Vehicle"."pvan_id_sls_channel" as "pvan_id_sls_channel" 
 , "V_Vehicle"."pvan_id_update_wrt_str_date" as "pvan_id_update_wrt_str_date" , "V_Vehicle"."pvan_cd_accg_doc_code" as "pvan_cd_accg_doc_code" 
 , "V_Vehicle"."pvan_ds_admn_prg_stu" as "pvan_ds_admn_prg_stu" , "V_Vehicle"."pvan_cd_cnf_tp_code" as "pvan_cd_cnf_tp_code" 
 , "V_Vehicle"."pvan_cd_delete_van" as "pvan_cd_delete_van" , "V_Vehicle"."pvan_cd_eng_serial_nbr" as "pvan_cd_eng_serial_nbr" 
 , "V_Vehicle"."pvan_cd_invoice_tp_code" as "pvan_cd_invoice_tp_code" , "V_Vehicle"."pvan_ds_prd_prg_stu" as "pvan_ds_prd_prg_stu" 
 , "V_Vehicle"."pvan_id_address_nbr" as "pvan_id_address_nbr" , "V_Vehicle"."pvan_id_bp_valor_date" as "pvan_id_bp_valor_date" 
 , "V_Vehicle"."pvan_id_change_conf_deliv_date" as "pvan_id_change_conf_deliv_date" 
 , "V_Vehicle"."pvan_id_change_exp_assign_date" as "pvan_id_change_exp_assign_date" 
 , "V_Vehicle"."pvan_id_change_exp_sett_date" as "pvan_id_change_exp_sett_date" 
 , "V_Vehicle"."pvan_id_confirmation_date" as "pvan_id_confirmation_date" , "V_Vehicle"."pvan_id_creation_date" as "pvan_id_creation_date" 
 , "V_Vehicle"."pvan_id_customer" as "pvan_id_customer" , "V_Vehicle"."pvan_id_delete_van_date" as "pvan_id_delete_van_date" 
 , "V_Vehicle"."pvan_id_eff_assign_date" as "pvan_id_eff_assign_date" , "V_Vehicle"."pvan_id_invoicing_date" as "pvan_id_invoicing_date" 
 , "V_Vehicle"."pvan_id_nic" as "pvan_id_nic" , "V_Vehicle"."pvan_id_ocf_bus_par" as "pvan_id_ocf_bus_par" 
 , "V_Vehicle"."pvan_id_opt_colour" as "pvan_id_opt_colour" , "V_Vehicle"."pvan_id_opt_mont_tyres" as "pvan_id_opt_mont_tyres" 
 , "V_Vehicle"."pvan_id_opt_req_tyres" as "pvan_id_opt_req_tyres" , "V_Vehicle"."pvan_id_plant" as "pvan_id_plant" 
 , "V_Vehicle"."pvan_id_requ_assignation_date" as "pvan_id_requ_assignation_date" 
 , "V_Vehicle"."pvan_id_request_delivery_date" as "pvan_id_request_delivery_date" 
 , "V_Vehicle"."pvan_id_ship_to_logistic_unit" as "pvan_id_ship_to_logistic_unit" 
 , "V_Vehicle"."pvan_id_vcb" as "pvan_id_vcb" , "V_Vehicle"."pvan_id_vcl" as "pvan_id_vcl" 
 , "V_Vehicle"."pvan_id_vcl_pri_stu" as "pvan_id_vcl_pri_stu" , "V_Vehicle"."pvan_id_vcl_sec_stu" as "pvan_id_vcl_sec_stu" 
 , "V_Vehicle"."pvan_id_vcl_usage" as "pvan_id_vcl_usage" , "V_Vehicle"."pvan_id_vp" as "pvan_id_vp" 
 , "V_Vehicle"."pvan_id_warranty_start_date" as "pvan_id_warranty_start_date" , "V_Vehicle"."pvan_fl_refurbishing_stu" as "pvan_fl_refurbishing_stu" 
 , "V_Vehicle"."pvan_cd_scheduling_tp_code" as "pvan_cd_scheduling_tp_code" , "V_Vehicle"."pvan_cd_ccm_stu_code" as "pvan_cd_ccm_stu_code" 
 , "V_Vehicle"."pvan_cd_van_code" as "pvan_cd_van_code" , "V_Vehicle"."pvan_cd_vin_code" as "pvan_cd_vin_code" 
 , "V_Vehicle"."new_codice_van_numerico" as "C__it__Vehicle_Number_Code"  
 
 from (select
                            pvan_id_vcl                     
                           ,pvan_cd_van_code                
                           ,pvan_cd_vin_code                
                           ,pvan_nr_ckd_lot_nbr             
                           ,pvan_cd_eng_serial_nbr          
                           ,pvan_cd_job_ord_code            
                           ,pvan_fl_xghng_flg               
                           ,pvan_cd_commercial_model_code   
                           ,pvan_cd_delete_van              
                           ,pvan_cd_prep_stu_code           
                           ,pvan_cd_license_nbr             
                           ,case when pvan_id_ocf_delete_date <> 5373484
                               then -1
                               else pvan_id_ocf_bus_par
                            end as pvan_id_ocf_bus_par             
                           ,pvan_cd_invoicing_tp_code       
                           ,pvan_fl_mad_spv_flag            
                           ,pvan_cd_crt_tp_code             
                           ,pvan_cd_mig_tp_code             
                           ,pvan_id_tyres_qty_adj_date      
                           ,pvan_id_character_adj_date      
                           ,pvan_id_colour_adj_date         
                           ,pvan_id_vp_tyres_adj_date       
                           ,pvan_id_chassis_adj_date        
                           ,pvan_id_fx_active_date          
                           ,pvan_id_spv_date                
                           ,pvan_id_temp_esport_date        
                           ,pvan_id_cf_delivery_date        
                           ,pvan_id_registration_date       
                           ,pvan_id_ic_order_matching_date  
                           ,pvan_id_migration_date          
                           ,pvan_id_transition_date         
                           ,pvan_id_document_creation_date  
                           ,pvan_id_homolog_request_date    
                           ,pvan_id_ccm_car_adj_date        
                           ,pvan_id_ccm_ref_adj_date        
                           ,pvan_id_cmt_lot_adj_date        
                           ,pvan_id_opt_req_tyres           
                           ,pvan_id_opt_mont_tyres          
                           ,pvan_id_opt_colour              
                           ,pvan_id_sales_organizat         
                           ,pvan_id_nic                     
                           ,pvan_id_division                
                           ,pvan_id_plant                   
                           ,pvan_id_vcl_usage               
                           ,pvan_id_search_area             
                           ,pvan_id_vcl_sec_stu             
                           ,pvan_id_vcl_pri_stu             
                           ,pvan_id_refurb_status_date      
                           ,pvan_id_status_date             
                           ,pvan_id_customer                
                           ,pvan_id_phy_cust                
                           ,pvan_id_virt_cust               
                           ,pvan_id_company                 
                           ,pvan_id_req_bill_date           
                           ,pvan_id_scheduling_date         
                           ,pvan_id_group_release_date      
                           ,pvan_id_change_request_date     
                           ,pvan_id_delete_van_date         
                           ,pvan_id_mad_date                
                           ,pvan_id_imis_plant_date         
                           ,pvan_id_limo3_date              
                           ,pvan_id_sett_date               
                           ,pvan_id_end_line_date           
                           ,pvan_id_invoicing_date          
                           ,pvan_id_sending_confirm_date    
                           ,pvan_id_confirmation_date       
                           ,pvan_id_required_canc_date      
                           ,pvan_id_comm_progress_date      
                           ,pvan_id_approval_date           
                           ,pvan_fl_approval_flg            
                           ,pvan_cd_invoice_tp_code         
                           ,pvan_cd_mad_tp_code             
                           ,pvan_cd_scheduling_tp_code      
                           ,pvan_cd_transition_tp_code      
                           ,pvan_id_sls_ord_pos_id          
                           ,pvan_cd_sls_ord_pos_code        
                           ,pvan_fl_buy_back_flg            
                           ,pvan_id_logistic_unit           
                           ,pvan_id_ship_to_logistic_unit   
                           ,pvan_id_sls_ord_id              
                           ,pvan_cd_sls_ord_code            
                           ,pvan_id_creation_date           
                           ,pvan_id_min_invoicing_date
                           ,case when pvan_id_ocf_delete_date = 5373484
                               then pvan_id_bp_valor_date
                               else 5373484
                            end as pvan_id_bp_valor_date             
                           ,pvan_id_eff_sett_date           
                           ,pvan_id_fx_exp_sett_date        
                           ,pvan_id_cust_req_bill_date      
                           ,pvan_id_dealer_req_bill_date    
                           ,pvan_id_requested_billing_date  
                           ,pvan_id_warranty_start_date     
                           ,pvan_id_change_exp_sett_date    
                           ,pvan_id_eff_delivery_date       
                           ,pvan_id_change_conf_deliv_date  
                           ,pvan_id_orig_conf_deliv_date    
                           ,pvan_id_fx_exp_assign_date      
                           ,pvan_id_change_exp_assign_date  
                           ,pvan_id_orig_expe_assign_date   
                           ,pvan_id_eff_assign_date         
                           ,pvan_id_eff_arriv_online_date   
                           ,pvan_id_requ_assignation_date   
                           ,pvan_id_request_delivery_date   
                           ,coalesce(pvan_fl_visibility_flg , '#') as pvan_fl_visibility_flg         
                           ,pvan_id_mig_stu                 
                           ,pvan_cd_accg_doc_code           
                           ,pvan_cd_ccm_stu_code            
                           ,pvan_fl_vp_auth_flg 
                           ,case when pvan_id_delete_van_date = 5373484
                               then pvan_cd_cmm_prg_tp_code
                               else null
                            end as pvan_cd_cmm_prg_tp_code        
                           ,pvan_cd_cmm_prg_tp_code as pvan_cd_cmm_prg_tp_code_old        
                           ,pvan_cd_cnf_tp_code             
                           ,pvan_id_new_requ_deliv_date     
                           ,pvan_cd_interco_invoice_code    
                           ,pvan_fl_trns_competence_flg     
                           ,pvan_nr_accg_doc_fiscal_year    
                           ,pvan_id_address_nbr             
                           ,pvan_id_vp                      
                           ,pvan_id_vcb                     
                           ,pvan_fl_block_interchange       
                           ,pvan_id_new_requ_del_date       
                           ,pvan_id_shipping_to_date        
                           ,pvan_id_billable_date           
                           ,pvan_fl_refurbishing_stu        
                           ,pvan_ds_prd_prg_stu as pvan_ds_prd_prg_stu_old
                           ,case when pvan_ds_prd_prg_stu = 'SENDING CONFIRMATION'          then 'Created, to be sent to confirmation'         
                                 when pvan_ds_prd_prg_stu = 'IMIS PLANT'                    then 'Sent to confirmation, to be scheduled'                 
                                 when pvan_ds_prd_prg_stu = 'OVER SCHEDULING DATE'          then 'Scheduled out of the frozen period'          
                                 when pvan_ds_prd_prg_stu = 'WITHIN SCHEDULING DATE'        then ' Scheduled into the frozen period '        
                                 when pvan_ds_prd_prg_stu = 'RELEASE GROUP'                 then 'Group release'                                      
                                 when pvan_ds_prd_prg_stu = 'LIMO 3'                        then 'Sent to Limo 3 (Lined-up for build)'                       
                                 when pvan_ds_prd_prg_stu = 'SETTING'                       then 'Line setting (On-Line)'                                   
                                 when pvan_ds_prd_prg_stu = 'EFF_ARRIV_ONLINE'              then 'Line descending'                                 
                                 when pvan_ds_prd_prg_stu = 'END_LINE'                      then 'End of line'                                             
                                 when pvan_ds_prd_prg_stu = 'ASSIGNED'                      then 'Gate Released'                                    
                                 when pvan_ds_prd_prg_stu = 'BLOCKED FOR CREDIT MANAGEMENT' then 'Created, to be sent to confirmation'
                                 when pvan_ds_prd_prg_stu is null                           then upper(pvps_ds_pri_stu_sdes)                               
                                 else null                                                                                          
                            end as pvan_ds_prd_prg_stu           
                           ,pvan_id_actl_prd_prg_stu_date   
                           ,pvan_ds_admn_prg_stu            
                           ,pvan_id_actl_admn_prg_stu_date  
                           ,pvan_id_location_date           
                           ,pvan_id_ord_date
                           ,pvan_id_bm
                           ,case when pvan_id_ocf_delete_date = 5373484
                               then pvan_ds_ocf_bus_par_nm
                               else '#'
                            end as pvan_ds_ocf_bus_par_nm          
                           ,pvan_id_batch_id                
                           ,pvan_id_van_emission_doc_date   
                           ,pvan_id_crt_wrt_str_date        
                           ,pvan_id_update_wrt_str_date     
                           ,pvan_cd_ocf_code_retail         
                           ,pvan_ld_ocf_nm_retail           
                           ,pvan_cd_inv_fin_cust_date       
                           ,pvan_cd_eft_del_fin_cust_date   
                           ,pvan_cd_req_del_fin_cust_date   
                           ,pvan_cd_final_user_retail       
                           ,pvan_ld_final_user_nm_retail    
                           ,pvan_cd_vin_retail              
                           ,pvan_cd_customer_retail
                           ,pvan_id_ocf_delete_date
                           ,pvan_id_ocf_entry_date_rtl      
                           ,pvan_id_ocf_del_date_rtl        
                           ,pvan_id_fdp                     
                           ,pvan_ds_prd_prg_stu_new         
                           ,pvan_id_res_customer            
                           ,pvan_id_sls_channel             
                           ,pvan_id_country_manager         
                           ,pvan_id_billing_block           
                           ,pvan_id_delivery_block          
                           ,pvan_id_district_manager        
                           ,pvan_id_incoterms               
                           ,pvan_id_admn_sls_channel        
                           ,pvan_id_sales_group             
                           ,pvan_id_sales_district          
                           ,pvan_id_country                 
                           ,pvan_id_log_appr_date           
                           ,pvan_id_fin_appr_date           
                           ,pvan_fl_log_appr                
                           ,pvan_fl_fin_appr                
                           ,pvan_id_actual_date_mav         
                           ,pvan_id_fault                   
                           ,pvan_id_original_setting_date   
                           ,pvan_cd_ref_bb                  
                           ,pvan_cd_process_tp              
                           ,pvan_fl_block_invoice           
                           ,pvan_cd_invoice_proc_cluster    
                           ,pvan_id_billable_date_nosap     
                           ,pvan_in_block_invoice           
                           ,pvan_fl_camper                  
                           ,pvan_nr_lt_order                
                           ,pvan_id_ccm_stu_date            
                           ,pvan_id_act_za02_date           
                           ,pvan_ds_delay_reason            
                           ,pvan_id_forecast_date           
                           ,pvan_cd_event_typ               
                           ,pvan_cd_dcc_status              
                           ,pvan_id_vcl_pri_stu_date        
                           ,pvan_id_vcl_sec_stu_date        
                           ,pvan_fl_ncf_status              
                           ,pvan_id_cmm_sls_channel         
                           ,pvan_id_trans_stu               
                           ,pvan_cd_global_block_code       
                           ,pvan_ds_global_block_sdes       
                           ,pvan_id_int_fct_start_date      
                           ,pvan_id_int_cnf_date            
                           ,pvan_id_int_tp                  
                           ,pvan_cd_ov_cred_sts_chk  
                          , pvan_cd_cust_po_num
                          , pvan_id_supplier_int            
                          , pvan_id_ship_start_date         
                          , pvan_id_ship_end_date           
                          , pvan_fl_franco_fabbrica
                          , pvan_fl_forz_van
                          , pvan_id_weel 
                          , pvan_id_dcr_dar_orig_date
                          , pvan_ds_salesman
                          , pvan_ds_subdealer
                           ,pvan_ds_demand
                           ,pvan_id_time_reg_tror_date
                           ,pvan_id_time_sales_ship_end
                           ,pvan_id_time_sales_ship_str
                           ,pvan_cd_delivery_code    
                           ,pvan_cd_price_list_code
                           ,pvan_id_price_list_crt_date
                           ,pvan_am_net_sales_order_amount  
                           ,pvan_cd_loan_code  
                           ,pvan_id_currency  
                           ,pvan_am_loan_amount  
                           ,pvan_am_loan_order_amount  
                           ,pvan_am_loan_delivery_amount  
                           ,pvan_am_loan_billing_amount
                           ,pvan_id_imbarco_delay_date
                           ,pvan_id_loan_delay_date
                           ,pvan_id_incoterms_loan
                           ,pvan_cd_incoterms_2_loan
                           ,pvan_id_financial_doc_sts
                           ,pvan_id_financial_doc_typ
                           ,pvan_id_condition_term
                           ,pvan_ds_regole
                           ,pvan_id_dir_date
                           ,pvan_id_dir_orig_date
                           ,pvan_id_dar_orig_date
                            ,coalesce(dero.dero_fl_status, '#') as pvan_fl_dero_status
                           ,coalesce(dero.dero_fl_exist, 'N')  as pvan_fl_dero_exist
                           ,coalesce(work.dero_fl_status, '#') as pvan_fl_work_status
                           ,coalesce(work.dero_fl_exist, 'N')  as pvan_fl_work_exist
                           ,pvan_id_bupr_end_cust_cs
                           ,pvan_id_zdccf_date
                           ,pvan_ds_note_rmv
                           ,pvan_id_dero_tyre
                           ,pvan_cd_dero_tyre
                           ,pvan_id_dero_approval_date
                           ,case when (is_numeric(ddwh02_dim.tdim_pvan_vehicle.pvan_cd_van_code))
                         then cast(ddwh02_dim.tdim_pvan_vehicle.pvan_cd_van_code as integer)
                         else (null)
                         end  as new_codice_van_numerico
                         from ddwh02_dim.tdim_pvan_vehicle
                              left outer join ddwh02_dim.tdim_pvps_vehicle_prod_status 
							  on pvan_id_vcl_pri_stu = pvps_id_vcl_pri_stu and 'EN'                = pvps_cd_language
                              left outer join (select dero_id_vcl, 
                                      dero_cd_request_type, 
                                      dero_fl_status, 
                                      'Y'                   as dero_fl_exist     
                               from   ddwh02_sm.tfct_dero_deroghe_workflow,
                                      ddwh02_dim.tdim_pvan_vehicle
                               where  dero_id_vcl = pvan_id_vcl
                               and    dero_cd_request_type = 'D'
                               and    dero_cd_last_snapshot   = '1') dero on  pvan_id_vcl = dero.dero_id_vcl
                               left outer join (select dero_id_vcl, 
                                      dero_cd_request_type, 
                                      dero_fl_status, 
                                      'Y'                   as dero_fl_exist     
                               from   ddwh02_sm.tfct_dero_deroghe_workflow,
                                      ddwh02_dim.tdim_pvan_vehicle
                               where  dero_id_vcl = pvan_id_vcl
                               and    dero_cd_request_type = 'W'
                               and    dero_cd_last_snapshot   = '1') work on  pvan_id_vcl = work.dero_id_vcl) "V_Vehicle" 
 
 inner join 
(select "SAO_NIC"."nicc_id_nic" as "nicc_id_nic" , "SAO_NIC"."nicc_cd_nic_level_5" as "nicc_cd_nic_level_5"  
 from "ddwh02_dim"."tdim_nicc_nic" "SAO_NIC" 
 where "SAO_NIC"."nicc_cd_nic_level_5" = N'118-POLONIA' or "SAO_NIC"."nicc_cd_nic_level_5" = N'431-POLAND') "SAO_NIC" 

 on "SAO_NIC"."nicc_id_nic" = "V_Vehicle"."pvan_id_nic" 

 where "SAO_NIC"."nicc_cd_nic_level_5" = N'118-POLONIA' or "SAO_NIC"."nicc_cd_nic_level_5" = N'431-POLAND'
) "V_Vehicle" 
 on "Data_OCF"."time_id_day" = "V_Vehicle"."pvan_id_bp_valor_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_DCC_Mobile" on "Data_DCC_Mobile"."time_id_day" = "V_Vehicle"."pvan_id_change_conf_deliv_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_DPA_Mobile" on "Data_DPA_Mobile"."time_id_day" = "V_Vehicle"."pvan_id_change_exp_assign_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_DPI_Mobile" on "Data_DPI_Mobile"."time_id_day" = "V_Vehicle"."pvan_id_change_exp_sett_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_Conferma" on "Data_Conferma"."time_id_day" = "V_Vehicle"."pvan_id_confirmation_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_Creazione" on "Data_Creazione"."time_id_day" = "V_Vehicle"."pvan_id_creation_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_Assegnazione_Effettiva" 
 on "Data_Assegnazione_Effettiva"."time_id_day" = "V_Vehicle"."pvan_id_eff_assign_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_Documento_Contabile" 
 on "Data_Documento_Contabile"."time_id_day" = "V_Vehicle"."pvan_id_invoicing_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_DAR" on "Data_DAR"."time_id_day" = "V_Vehicle"."pvan_id_requ_assignation_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_DCR" on "Data_DCR"."time_id_day" = "V_Vehicle"."pvan_id_request_delivery_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "Data_Inizio_Garanzia" 
 on "Data_Inizio_Garanzia"."time_id_day" = "V_Vehicle"."pvan_id_warranty_start_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "T3" on "T3"."time_id_day" = "V_Vehicle"."pvan_id_crt_wrt_str_date") 
 
 inner join "ddwh02_dim"."tdim_time_time" "T21" on "T21"."time_id_day" = "V_Vehicle"."pvan_id_update_wrt_str_date") 
 
 inner join (select "Optional_Colori_Per_VAN"."popt_id_opt" as "popt_id_opt" 
 , "Optional_Colori_Per_VAN"."new_codice_colore" as "C__it__Colour_Code__Numeric_" , "Optional_Colori_Per_VAN"."popt_ld_opt_ldes" as "popt_ld_opt_ldes"  
 from (select tdim_popt_optional.*,
                         case when
                              public.is_numeric(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code) then
                                   cast(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code as integer)
                             
                              else
                              case when
                             rtrim(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code) = '#' then -1
                               else null
                             end 
                         end as new_codice_colore
                         from ddwh02_dim.tdim_popt_optional) "Optional_Colori_Per_VAN" 
						 where ("Optional_Colori_Per_VAN"."popt_cd_opt_code" = N'#' or 
						 "Optional_Colori_Per_VAN"."popt_cd_opt_code" >= N'50000' and "Optional_Colori_Per_VAN"."popt_cd_opt_code" < N'60000') 
						 and "Optional_Colori_Per_VAN"."popt_cd_language" = N'IT') "Optional_Colori_Per_VAN" 
						 on "Optional_Colori_Per_VAN"."popt_id_opt" = "V_Vehicle"."pvan_id_opt_colour") 

						 inner join (select "T1"."popt_id_opt" as "popt_id_opt" , "T1"."new_popt_cd_opt_code" as "c4" , "T1"."popt_ld_opt_ldes" as "popt_ld_opt_ldes"  from 
						 (select tdim_popt_optional.*,
                         case when is_numeric(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code) then
                         cast(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code as integer)
                         else null
                         end  as new_popt_cd_opt_code
                         from ddwh02_dim.tdim_popt_optional) "T1" 
						 where ("T1"."popt_cd_opt_code" = N'#' or "T1"."popt_cd_opt_code" >= N'20000' and "T1"."popt_cd_opt_code" < N'30000') 
						 and "T1"."popt_cd_language" = N'IT') "T26" 
						 on "T26"."popt_id_opt" = "V_Vehicle"."pvan_id_opt_mont_tyres") 
						 
						 
 inner join (select "T1"."popt_id_opt" as "popt_id_opt" , "T1"."new_popt_cd_opt_code" as "c4" , "T1"."popt_ld_opt_ldes" as "popt_ld_opt_ldes"  
						 from (select tdim_popt_optional.*,
                         case when is_numeric(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code) then
                         cast(ddwh02_dim.tdim_popt_optional.popt_cd_opt_code as integer)
                         else null
                         end  as new_popt_cd_opt_code
                         from ddwh02_dim.tdim_popt_optional ) "T1" 
						 where ("T1"."popt_cd_opt_code" = N'#' or "T1"."popt_cd_opt_code" >= N'20000' and "T1"."popt_cd_opt_code" < N'30000') 
						 and "T1"."popt_cd_language" = N'IT') "T25" 
						 on "T25"."popt_id_opt" = "V_Vehicle"."pvan_id_opt_req_tyres") 
						 
 inner join (select "T1"."pvps_id_vcl_pri_stu" as "pvps_id_vcl_pri_stu" , "T1"."pvps_cd_pri_stu_code" as "pvps_cd_pri_stu_code"  
 from "ddwh02_dim"."tdim_pvps_vehicle_prod_status" "T1" where "T1"."pvps_cd_language" = N'IT') "T34" 
 on "T34"."pvps_id_vcl_pri_stu" = "V_Vehicle"."pvan_id_vcl_pri_stu") 

 inner join (select "Utiilzzo_Veicolo_Per_VAN"."pvug_id_vcl_usage" as "pvug_id_vcl_usage" , "Utiilzzo_Veicolo_Per_VAN"."pvug_cd_vcl_usage_code" as "pvug_cd_vcl_usage_code" , "Utiilzzo_Veicolo_Per_VAN"."pvug_ld_vcl_usage_ldes" as "pvug_ld_vcl_usage_ldes"  
 from "ddwh02_dim"."tdim_pvug_vehicle_usage" "Utiilzzo_Veicolo_Per_VAN" 
 where "Utiilzzo_Veicolo_Per_VAN"."pvug_cd_language" = N'IT') "Utiilzzo_Veicolo_Per_VAN" 
 on "Utiilzzo_Veicolo_Per_VAN"."pvug_id_vcl_usage" = "V_Vehicle"."pvan_id_vcl_usage") 
 
 inner join (select "T1"."pvss_id_vcl_sec_stu" as "pvss_id_vcl_sec_stu" , "T1"."pvss_ld_sec_stu_ldes" as "pvss_ld_sec_stu_ldes"  
 from "ddwh02_dim"."tdim_pvss_vehicle_sec_status" "T1" where "T1"."pvss_cd_language" = N'IT') "T24" 
 on "T24"."pvss_id_vcl_sec_stu" = "V_Vehicle"."pvan_id_vcl_sec_stu") 
 
 inner join (select "plan_id_plant", "plan_cd_plant_code", trim(both from "plan_ds_plant_nm") as "plan_ds_plant_nm"  
 from "ddwh02_dim"."tdim_plan_plant" where "plan_cd_language" = 'IT') "ORG_Plant_for_VAN" 
 on "ORG_Plant_for_VAN"."plan_id_plant" = "V_Vehicle"."pvan_id_plant") 
 
 inner join (select "Destinatario_Merci_VAN"."cust_id_customer" as "cust_id_customer" 
 , case case "Destinatario_Merci_VAN"."cust_cd_cust_code_sap" when '#' then NULL else "Destinatario_Merci_VAN"."cust_cd_cust_code_sap" end  
 when NULL 
 then case "Destinatario_Merci_VAN"."cust_cd_cust_code_isis" when '#' then NULL else "Destinatario_Merci_VAN"."cust_cd_cust_code_isis" end  
 else case "Destinatario_Merci_VAN"."cust_cd_cust_code_sap" when '#' then NULL else "Destinatario_Merci_VAN"."cust_cd_cust_code_sap" end  end  as "Identificativo_Cliente" 
 , "Destinatario_Merci_VAN"."cust_ds_cust_nm" as "cust_ds_cust_nm" , "Destinatario_Merci_VAN"."cust_cd_company_code" as "cust_cd_company_code" , "Destinatario_Merci_VAN"."cust_ds_cust_city" as "cust_ds_cust_city"  
 from "ddwh02_dim"."tdim_cust_customer" "Destinatario_Merci_VAN") "Destinatario_Merci_VAN" 
 on "Destinatario_Merci_VAN"."cust_id_customer" = "V_Vehicle"."pvan_id_ship_to_logistic_unit") 
 
 inner join (select "Business_Partner_Per_VAN"."bupr_id_bus_par" as "bupr_id_bus_par" , case  when  NOT "Business_Partner_Per_VAN"."bupr_ds_org_name1" is null then trim(both from "Business_Partner_Per_VAN"."bupr_ds_org_name1" || N' ' || nvl("Business_Partner_Per_VAN"."bupr_ds_org_name2", ' ')) when  NOT "Business_Partner_Per_VAN"."bupr_ds_first_nm" is null or  NOT "Business_Partner_Per_VAN"."bupr_ds_last_nm" is null then trim(both from nvl("Business_Partner_Per_VAN"."bupr_ds_first_nm", ' ') || ' ' || nvl("Business_Partner_Per_VAN"."bupr_ds_last_nm", ' ')) else trim(both from "Business_Partner_Per_VAN"."bupr_cd_bus_par" || nvl("Business_Partner_Per_VAN"."bupr_ds_search_term_1", ' ')) end  as "Nome_OCF"  
 from "ddwh02_dim"."tdim_bupr_business_partner" "Business_Partner_Per_VAN") "Business_Partner_Per_VAN" 
 on "Business_Partner_Per_VAN"."bupr_id_bus_par" = "V_Vehicle"."pvan_id_ocf_bus_par") 
 
 inner join (select "Cliente_van"."cust_id_customer" as "cust_id_customer" , case case "Cliente_van"."cust_cd_cust_code_sap" when '#' then NULL else "Cliente_van"."cust_cd_cust_code_sap" end  when NULL then case "Cliente_van"."cust_cd_cust_code_isis" when '#' then NULL else "Cliente_van"."cust_cd_cust_code_isis" end  else case "Cliente_van"."cust_cd_cust_code_sap" when '#' then NULL else "Cliente_van"."cust_cd_cust_code_sap" end  end  as "Identificativo_Cliente" , "Cliente_van"."cust_ds_cust_nm" as "cust_ds_cust_nm"  from "ddwh02_dim"."tdim_cust_customer" "Cliente_van") "Cliente_van" on "Cliente_van"."cust_id_customer" = "V_Vehicle"."pvan_id_customer") 
 
 inner join (select * from "ddwh02_dim"."tdim_prvp_vp" where "prvp_cd_language" = 'IT') "PR_Prodotto_per_VAN" 
 on "PR_Prodotto_per_VAN"."prvp_id_vp" = "V_Vehicle"."pvan_id_vp") 
 
 inner join (select * from "ddwh02_dim"."tdim_pvcb_vcb_vehicle" where "pvcb_cd_language" = 'IT') "T19" 
 on "T19"."pvcb_id_vcb" = "V_Vehicle"."pvan_id_vcb") 
 
 inner join "ddwh02_dim"."tdim_addr_address" "Ubicazione" 
 on "Ubicazione"."addr_id_address_nbr" = "V_Vehicle"."pvan_id_address_nbr")
 
 inner join (select "ddwh02_dim"."tdim_pfdp_fdp_planning_family"."pfdp_ds_alternative_grp" as "pfdp_ds_alternative_grp" , "ddwh02_dim"."tdim_pfdp_fdp_planning_family"."pfdp_ds_cgm_desc" as "pfdp_ds_cgm_desc" , "ddwh02_dim"."tdim_pfdp_fdp_planning_family"."pfdp_cd_fdp_code" as "pfdp_cd_fdp_code" , "ddwh02_dim"."tdim_pfdp_fdp_planning_family"."pfdp_id_fdp" as "pfdp_id_fdp"  
 from "ddwh02_dim"."tdim_pfdp_fdp_planning_family" 
 where "pfdp_cd_language" = 'IT') "Prodotto_FDP_Per_VAN" 
 on "Prodotto_FDP_Per_VAN"."pfdp_id_fdp" = "V_Vehicle"."pvan_id_fdp") 
 
 inner join (select "Reservation_Client__VAN_"."cust_id_customer" as "cust_id_customer" , "Reservation_Client__VAN_"."cust_ds_cust_nm" as "cust_ds_cust_nm" , case case "Reservation_Client__VAN_"."cust_cd_cust_code_sap" when '#' then NULL else "Reservation_Client__VAN_"."cust_cd_cust_code_sap" end  when NULL then case "Reservation_Client__VAN_"."cust_cd_cust_code_isis" when '#' then NULL else "Reservation_Client__VAN_"."cust_cd_cust_code_isis" end  else case "Reservation_Client__VAN_"."cust_cd_cust_code_sap" when '#' then NULL else "Reservation_Client__VAN_"."cust_cd_cust_code_sap" end  end  as "Identificativo_Cliente"  
 from "ddwh02_dim"."tdim_cust_customer" "Reservation_Client__VAN_") "Reservation_Client__VAN_" 
 on "Reservation_Client__VAN_"."cust_id_customer" = "V_Vehicle"."pvan_id_res_customer") 
 
 inner join (select "scha"."scha_id_sls_channel", "scha"."scha_cd_op_sls_channel", (coalesce(min(case  when "scha"."scha_cd_legacy_code" = '91' then "scha"."scha_ld_op_sls_channel_ldes" else NULL end ) over (partition by "scha"."scha_cd_op_sls_channel"), "scha"."scha_ld_op_sls_channel_ldes")) as "scha_ld_op_sls_channel_ldes_m"  
 from "ddwh02_dim"."tdim_scha_comm_sales_channel" "scha") "Canale_Di_Vendita_Del_VAN"
 on "Canale_Di_Vendita_Del_VAN"."scha_id_sls_channel" = "V_Vehicle"."pvan_id_sls_channel") 
 
 inner join (select "SAO_NIC"."nicc_id_nic" as "nicc_id_nic" , "SAO_NIC"."nicc_cd_nic_level_5" as "nicc_cd_nic_level_5"  
 from "ddwh02_dim"."tdim_nicc_nic" "SAO_NIC" 
 where "SAO_NIC"."nicc_cd_nic_level_5" = N'118-POLONIA' or "SAO_NIC"."nicc_cd_nic_level_5" = N'431-POLAND') "SAO_NIC" 
 on "SAO_NIC"."nicc_id_nic" = "V_Vehicle"."pvan_id_nic") 
 
 left outer join "ddwh02_dim"."tdim_ovvm_vp_vm_opt" "C__it__TDIM_OVVM_VP_VM_OPT" 
 on "V_Vehicle"."pvan_id_vp" = "C__it__TDIM_OVVM_VP_VM_OPT"."ovvm_id_vp") 
 
 left outer join "ddwh02_dim"."tdim_ovca_vp_opt_cat" "C__it__TDIM_OVCA_VP_OPT_CAT" 
 on "V_Vehicle"."pvan_id_vp" = "C__it__TDIM_OVCA_VP_OPT_CAT"."ovca_id_vp" 
 and "V_Vehicle"."pvan_id_vcl" = "C__it__TDIM_OVCA_VP_OPT_CAT"."ovca_id_vcl") 
 
 left outer join "ddwh02_dim"."tdim_ovvp_vp_opt" "C__it__TDIM_OVVP_VP_OPT" 
 on "V_Vehicle"."pvan_id_vcl" = "C__it__TDIM_OVVP_VP_OPT"."ovvp_id_vcl" 
 and "V_Vehicle"."pvan_id_vp" = "C__it__TDIM_OVVP_VP_OPT"."ovvp_id_vp" 
 where "Utiilzzo_Veicolo_Per_VAN"."pvug_cd_vcl_usage_code"  NOT  in (N'19', N'41', N'42', N'43', N'44', N'99', N'16') 
 and ("V_Vehicle"."pvan_cd_delete_van"  NOT  in (N'VIR', N'FIS') or "V_Vehicle"."pvan_cd_delete_van" is null) 
 and "V_Vehicle"."pvan_id_vcl"  NOT  in (-1, -2) 
 and "V_Vehicle"."pvan_cd_van_code" < N'0200000000' 
 and "T34"."pvps_cd_pri_stu_code" <> N'ZM65' 
 and "V_Vehicle"."pvan_id_delete_van_date" = 5373484 
; 
