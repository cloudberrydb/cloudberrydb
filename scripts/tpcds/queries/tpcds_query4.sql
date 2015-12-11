-- start query 1 in stream 0 using template query4.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag
       ,c_birth_country
       ,c_login
       ,c_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk and
   (d_year = 2001 or d_year = 2002)
   and ss_quantity = 10
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag
       ,c_birth_country
       ,c_login
       ,c_email_address
       ,d_year dyear
       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
       ,'c' sale_type
 from customer
     ,catalog_sales
     ,date_dim
 where c_customer_sk = cs_bill_customer_sk
   and cs_sold_date_sk = d_date_sk and
   (d_year = 2001 or d_year = 2002)
   and cs_quantity = 10
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
         )
  select  t_s_secyear.customer_id
       ,t_s_secyear.customer_first_name
       ,t_s_secyear.customer_last_name
       ,t_s_secyear.c_preferred_cust_flag
       ,t_s_secyear.c_birth_country
       ,t_s_secyear.c_login 
 from year_total t_s_firstyear
     ,year_total t_s_secyear
     ,year_total t_c_firstyear
     ,year_total t_c_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
   and t_s_firstyear.customer_id = t_c_secyear.customer_id
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
   and t_s_firstyear.sale_type = 's'
   and t_c_firstyear.sale_type = 'c'
   and t_s_secyear.sale_type = 's'
   and t_c_secyear.sale_type = 'c'
   --and t_s_firstyear.dyear = 2001
   --and t_s_secyear.dyear = 2001+1
   --and t_c_firstyear.dyear = 2001
   --and t_c_secyear.dyear = 2001+1
   --and t_s_firstyear.year_total>0
   --and t_c_firstyear.year_total>0
   --and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
         --  > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
 order by t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
        ,t_s_secyear.c_preferred_cust_flag,t_s_secyear.c_birth_country,t_s_secyear.c_login
 limit 200;

-- end query 1 in stream 0 using template query4.tpl
