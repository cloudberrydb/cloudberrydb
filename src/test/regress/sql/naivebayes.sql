--
-- naivebayes.sql - test of naive bayesian classification aggregates
--

CREATE TABLE bayes1(id int, class text, A1 int, A2 int, A3 int)
DISTRIBUTED BY (id);
COPY bayes1 FROM stdin;
1	C1	1	2	3
2	C1	1	2	1
3	C1	1	4	3
4	C2	1	3	2
5	C2	0	2	2
6	C2	0	1	3
\.

CREATE TABLE bayes2 AS
  SELECT 
      id, 
      class,
      unnest(array['A1','A2','A3']) as attr,
      unnest(array[a1,a2,a3]) as value
  FROM bayes1
  DISTRIBUTED BY (id);

CREATE TABLE bayes_training AS 
  SELECT attr, value, pivot_sum(array['C1', 'C2'], class, 1) as class_count
  FROM bayes2
  GROUP BY attr, value
  DISTRIBUTED by (attr);

CREATE TABLE bayes_classify AS
  SELECT 
      attr, 
      value, 
      class_count, 
      array['C1', 'C2'] as classes,
      sum(class_count) over (wa)::integer[] as class_total,
      count(distinct value) over (wa) as attr_count
  FROM bayes_training
  WINDOW wa as (partition by attr)
  DISTRIBUTED BY (attr);

-- Classify the training data, results should mostly match
-- begin_equiv
SELECT
    id, 
    class,
    nb_classify(classes, attr_count, class_count, class_total) as classified,
    nb_probabilities(classes, attr_count, class_count, class_total)::float4[] as probabilities
FROM
    bayes2 join bayes_classify using (attr, value)
GROUP BY id, class
ORDER BY id;

SELECT
    id, 
    class,
    nb_classify(classes, attr_count, class_count, class_total) as classified,
    nb_probabilities(classes, attr_count, class_count, class_total)::float4[] as probabilities
FROM
    bayes1 b, bayes_classify c
WHERE (attr = 'A1' and b.a1 = c.value) or
      (attr = 'A2' and b.a2 = c.value) or
      (attr = 'A3' and b.a3 = c.value)
GROUP BY id, class
ORDER BY id;
-- end_equiv

-- Testing individual functions

-- begin_equiv
select null::nb_classification;
select nb_classify_accum(NULL, NULL, NULL, NULL, NULL);
select nb_classify_combine(NULL, NULL);
select nb_classify_final(NULL);
select nb_classify_probabilities(NULL);
-- end_equiv

-- begin_equiv
select classes, accum::float4[], apriori
from (values(array['a','b'], 
             array[-1.25276296849537,-1.50407739677627],
             array[5,7])) v(classes, accum, apriori);

select classes, accum::float4[], apriori
from nb_classify_accum(NULL, array['a','b'], 2, array[1,1], array[5,7]);

select classes, accum::float4[], apriori
from nb_classify_accum(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,1], array[5,7]),
        NULL, NULL, NULL, NULL);

select classes, accum::float4[], apriori
from nb_classify_combine(
        NULL, 
        nb_classify_accum(NULL, array['a','b'], 2, array[1,1], array[5,7]));

select classes, accum::float4[], apriori
from nb_classify_combine(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,1], array[5,7]),
        NULL);
-- end_equiv

-- begin_equiv
select classes, accum::float4[], apriori
from (values(array['a','b'], 
             array[-2.16905370036952,-1.50407739677627],
             array[5,7])) v(classes, accum, apriori);

select classes, accum::float4[], apriori
from nb_classify_accum(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7]),
        array['a','b'], 2, array[1,3], array[3,4]);

select classes, accum::float4[], apriori
from nb_classify_accum(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,3], array[3,4]),
        array['a','b'], 2, array[1,2], array[5,7]);

select classes, accum::float4[], apriori
from nb_classify_combine(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,3], array[3,4]),
        nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7])
        );

select classes, accum::float4[], apriori
from nb_classify_combine(
        nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7]),
        nb_classify_accum(NULL, array['a','b'], 2, array[1,3], array[3,4])
        );
-- end_equiv

select nb_classify_final(
   nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7]));

select nb_classify_probabilities(
   nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7]))::float4[];

select nb_classify_probabilities(
   nb_classify_accum(NULL, array['a','b','c','d'], 2, array[1,2,3,4], array[5,7,3,4]))::float4[];

-- all probabilities should add up to 1
-- begin_equiv
select 1::float4;

select sum(p)::float4
from unnest(nb_classify_probabilities(
   nb_classify_accum(NULL, array['a','b'], 2, array[1,2], array[5,7]))) p;

select sum(p)::float4
from unnest(nb_classify_probabilities(
   nb_classify_accum(NULL, array['a','b','c','d'], 2, array[1,2,3,4], array[5,7,3,4]))) p;
-- end_equiv

-- negative test cases

-- non-comformable, different length in input values
select nb_classify_accum(NULL, array['a','b'], 3, array[1,2,3,4], array[5,7]);
select nb_classify_accum(NULL, array['a','b'], 3, array[1,2], array[5,7,2]);
select nb_classify_combine(
        nb_classify_accum(NULL, array['a','b'], 3, array[1,2], array[5,7]),
        nb_classify_accum(NULL, array['a','b','c'], 3, array[1,2], array[5,7]));

-- invalid state (all arrays must be equal length, single dimensional)
select nb_classify_accum('("{a,b}","{5,3,7}","{5,7}")', 
                         '{a,b}', 3, '{1,2}', '{5,7}');
select nb_classify_combine('("{a,b}","{5,3,7}","{5,7}")', 
                           '("{a,b}","{5,3}","{5,7}")');
select nb_classify_combine('("{a,b}","{5,3}","{5,7}")', 
                           '("{a,b}","{5,3,7}","{5,7}")');
select nb_classify_final('("{a,b}","{5,3,7}","{5,7}")');
select nb_classify_probabilities('("{a,b}","{5,3,7}","{5,7}")');
select nb_classify_probabilities('("{a,b}","{{5},{3}}","{5,7}")');
select nb_classify_probabilities('("{a,b}",,"{5,7}")');

DROP TABLE bayes1;
DROP TABLE bayes2;
DROP TABLE bayes_training;
DROP TABLE bayes_classify;
