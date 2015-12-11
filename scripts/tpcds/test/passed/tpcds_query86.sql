-- start query 1 in stream 0 using template query86.tpl
select   
    sum(ws_net_paid) as total_sum
   ,i_category
   ,i_class
   ,rank() over (
 	partition by i_category,i_class
 	order by sum(ws_net_paid) desc) as rank_within_parent
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_year = 2000
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by i_category,i_class
 order by
   rank_within_parent
 limit 100;

-- end query 1 in stream 0 using template query86.tpl
