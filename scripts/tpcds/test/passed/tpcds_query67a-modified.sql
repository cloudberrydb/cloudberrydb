-- start query 1 in stream 0 using template query67a.tpl
with results as
(     select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy ,d_moy ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales ,date_dim ,store ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_year=2000
       group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id)
 ,
 results_rollup as
-- added casting ::text
 (select i_category::text, i_class::text, i_brand::text, i_product_name::text, d_year, d_qoy, d_moy, s_store_id::text, sumsales
  from results
  union all
-- added casting ::text
  select i_category::text as i_category, i_class::text as i_class, i_brand::text as i_brand, i_product_name::text as i_product_name, d_year, d_qoy, d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy
  union all
-- added casting ::text
-- added casting ::integer
  select i_category::text as i_category, i_class::text as i_class, i_brand::text as i_brand, i_product_name::text as i_product_name, d_year, d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy
  union all
-- added casting ::text
-- added casting ::integer
  select i_category::text as i_category, i_class::text as i_class, i_brand::text as i_brand, i_product_name::text as i_product_name, d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year
  union all
-- added casting ::text
-- added casting ::integer
  select i_category::text as i_category, i_class::text as i_class, i_brand::text as i_brand, i_product_name::text as i_product_name, null::integer d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name
  union all
-- added casting ::text
  select i_category::text as i_category, i_class::text as i_class, i_brand::text as i_brand, null::text i_product_name, null::integer d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand
  union all
-- added casting ::text
  select i_category::text as i_category, i_class::text as i_class, null::text i_brand, null::text i_product_name, null::integer d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class
  union all
-- added casting ::text
  select i_category::text as i_category, null::text i_class, null::text i_brand, null::text i_product_name, null::integer d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results
  group by i_category
  union all
-- added casting ::text
  select null::text i_category, null::text i_class, null::text i_brand, null::text i_product_name, null::integer d_year, null::integer d_qoy, null::integer d_moy, null::text s_store_id, sum(sumsales) sumsales
  from results)

 select  *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from results_rollup) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;

-- end query 1 in stream 0 using template query67a.tpl
