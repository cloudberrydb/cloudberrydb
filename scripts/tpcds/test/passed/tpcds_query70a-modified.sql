-- start query 1 in stream 0 using template query70a.tpl
with results as
( select
    sum(ss_net_profit) as total_sum ,s_state ,s_county, 0::int8 as gstate, 0::int8 as g_county
 from
    store_sales
   ,date_dim       d1
   ,store
 where
    d1.d_year = 2000
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state in
             ( select s_state
               from  (select s_state as s_state,
 			    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
                      from   store_sales, store, date_dim
                      where  d_year =2000 
 			    and d_date_sk = ss_sold_date_sk
 			    and s_store_sk  = ss_store_sk
                      group by s_state
                     ) tmp1 
               where ranking <= 5)
  group by s_state,s_county) ,  
 
 results_rollup as
-- add explicit casting to int8 
( select total_sum ,s_state ,s_county, 0::int8 as g_state, 0::int8 as g_county, 0::int8 as lochierarchy from results
 union
-- add explicit casting to int8 and character
 select sum(total_sum) as total_sum,s_state, NULL::character varying(20)   as s_county, 0::int8 as g_state, 1::int8 as g_county, 1::int8 as lochierarchy from results group by s_state
 union
-- add explicit casting to int8 and character
 select sum(total_sum) as total_sum ,NULL::character(2) as s_state ,NULL::character varying(20) as s_county, 1::int8 as g_state, 1::int8 as g_county, 2::int8 as lochierarchy from results)

 select total_sum ,s_state ,s_county, lochierarchy 
   ,rank() over (
 	partition by lochierarchy, 
 	case when g_county = 0 then s_state end 
 	order by total_sum desc) as rank_within_parent
 from results_rollup
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent ;

-- end query 1 in stream 0 using template query70a.tpl
