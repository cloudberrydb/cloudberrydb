-- start query 1 in stream 0 using template query27a.tpl
with results as
 (select i_item_id,
        s_state, 0 as g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'S' and
       cd_education_status = 'College' and
       d_year = 2002 and
       s_state in ('TN','TN', 'TN', 'TN', 'TN', 'TN')
 group by i_item_id, s_state)

  select  i_item_id,
  s_state, g_state, agg1, agg2, agg3, agg4
   from (
        select i_item_id, s_state, g_state, agg1, agg2, agg3, agg4 from results
         union
        select i_item_id, NULL AS s_state, 1 AS g_state, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
         avg(agg4) agg4 from results
        group by i_item_id
         union
        select NULL AS i_item_id, NULL as s_state, 1 as g_state, avg(agg1) agg1, avg(agg2) agg2, avg(agg3) agg3,
         avg(agg4) agg4 from results
        ) foo
  order by i_item_id, s_state
 limit 100;

-- end query 1 in stream 0 using template query27a.tpl
