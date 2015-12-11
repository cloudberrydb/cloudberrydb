-- start query 1 in stream 0 using template query70.tpl
select  
    sum(ss_net_profit) as total_sum
   ,s_state
   ,s_county
   ,rank() over (
 	partition by s_state,s_county
 	order by sum(ss_net_profit) desc) as rank_within_parent
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
               where ranking <= 5
             )
 group by s_state,s_county
 order by
  rank_within_parent
 limit 100;

-- end query 1 in stream 0 using template query70.tpl
