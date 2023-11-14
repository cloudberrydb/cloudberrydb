--
-- Tests of Answer Query Using Materialized Views.
--
set optimizer = off;
create table aqumv_t1(c1 int, c2 int, c3 int) distributed by (c1);
insert into aqumv_t1 select i, i+1, i+2 from generate_series(1, 1000) i;
insert into aqumv_t1 select * from aqumv_t1;
analyze aqumv_t1;
set answer_query_using_materialized_views = on;

-- drop views if there is no data populated
begin;
create incremental materialized view aqumv_mvt1_need_refresh as select * from aqumv_t1 where c1 = 2 with no data;
set local answer_query_using_materialized_views = on;
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
refresh materialized view aqumv_mvt1_need_refresh;
analyze aqumv_mvt1_need_refresh;
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
abort;

begin;
create incremental materialized view aqumv_mvt1_0 as select * from aqumv_t1 where c1 = 2;
analyze aqumv_mvt1_0;

set local answer_query_using_materialized_views = off;
select * from aqumv_t1 where c1 = 2;
select c1, c2, c3 from aqumv_t1 where c1 = 2;
select c2 from aqumv_t1 where c1 = 2;
select c3, c2 from aqumv_t1 where c1 = 2;

set local answer_query_using_materialized_views = on;
select * from aqumv_t1 where c1 = 2;
select c1, c2, c3 from aqumv_t1 where c1 = 2;
select c2 from aqumv_t1 where c1 = 2;
select c3, c2 from aqumv_t1 where c1 = 2;
-- tlist matched.
explain(verbose, costs off) select * from aqumv_t1 where c1 = 2;
explain(verbose, costs off) select c1, c2, c3 from aqumv_t1 where c1 = 2;
-- tlist partially matched.
explain(verbose, costs off) select c2 from aqumv_t1 where c1 = 2;
-- tlist disorder.
explain(verbose, costs off) select c3, c2 from aqumv_t1 where c1 = 2;
abort;

begin;
create incremental materialized view aqumv_mvt1_1 as select c2 as mc2, c3 as mc3, c1 as mc1, c2 as mc2_1 from aqumv_t1 where c1 = 3;
analyze aqumv_mvt1_1;
set local answer_query_using_materialized_views = off;
select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
select c1, c1 from aqumv_t1 where c1 = 3;

set local answer_query_using_materialized_views = on;
select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
select c1, c1 from aqumv_t1 where c1 = 3;

-- tlist alias.
explain(verbose, costs off) select c1 as col1, c2 as col2 from aqumv_t1 where c1 = 3;
-- duplicated projection.
explain(verbose, costs off) select c1, c1 from aqumv_t1 where c1 = 3;
abort;

begin;
create incremental materialized view aqumv_mvt1_nonvar_expr as
  select c2, 1 as mc_const_1, sqrt(100) as mc_sqrt_100
  from aqumv_t1 where c1 = 4;
analyze aqumv_mvt1_nonvar_expr;
set local answer_query_using_materialized_views = off;
select c2, 200 from aqumv_t1 where c1 = 4;
select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;

set local answer_query_using_materialized_views = on;
select c2, 200 from aqumv_t1 where c1 = 4;
select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;

-- Const are copied to output.
explain(verbose, costs off) select c2, 200 from aqumv_t1 where c1 = 4;
explain(verbose, costs off) select c2, 1, sqrt(100) from aqumv_t1 where c1 = 4;
abort;

begin;
CREATE OR REPLACE FUNCTION aqumv_func(i integer, j integer) RETURNS integer AS $$
    BEGIN
        RETURN i + j;
    END;
$$ LANGUAGE plpgsql IMMUTABLE;
create incremental materialized view aqumv_mvt1_func_has_var as
  select c2, aqumv_func(c1, c3) as mc_func_res
  from aqumv_t1 where c1 = 5;
analyze aqumv_mvt1_func_has_var;

set local answer_query_using_materialized_views = off; 
select c2, aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;

set local answer_query_using_materialized_views = on;
select c2, aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;

-- Functions has Vars are replaced.
explain(verbose, costs off) select c2, aqumv_func(c1, c3), aqumv_func(c1, c3) from aqumv_t1 where c1 = 5;
abort;

begin;
create incremental materialized view aqumv_mvt1_2 as select c2 as mc2, c1 as mc1 from aqumv_t1 where c1 > 1 and c1 < 5;
analyze aqumv_mvt1_2;
set local answer_query_using_materialized_views = on;
-- shoud be unable to use mv, projection doesn't exit in mv's tlist 
explain(verbose, costs off) select c3 from aqumv_t1 where c1 < 5 and c1 > 1;
-- no post quals.
explain(verbose, costs off) select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1;

-- post quals added to mv.
set local answer_query_using_materialized_views = off;
select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;
set local answer_query_using_materialized_views = on;
select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;
explain(verbose, costs off) select c1, c2 from aqumv_t1 where c1 < 5 and c1 > 1 and c2 = 4;

-- should be unable to use mv, post quals has column doesn't exit in mv's tlist.
explain(verbose, costs off) select * from aqumv_t1 where c1 < 5 and c1 > 1 and c3 > 1; 
abort;

begin;
create incremental materialized view aqumv_mvt1_3 as select c2 as mc2, c1 as mc1, c3+1 as mc3 from aqumv_t1 where c1 > 5 and c1 < 10;
analyze aqumv_mvt1_3;

set local answer_query_using_materialized_views = on;
-- should be unable to use mv, column c3 doesn't exist in mv's tlist.
explain(verbose, costs off) select * from aqumv_t1 where c1 > 5 and c1 < 10; 
-- expr c3+1 is in mv's tlist
explain(verbose, costs off) select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
-- expr c1+1 could be derived from mv's tlist
explain(verbose, costs off) select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 

select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
set local answer_query_using_materialized_views = off;
select c1 as col1, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
select c1+1 as col1, c2, c3+1 as col2 from aqumv_t1 where c1 > 5 and c1 < 10; 
abort;

begin;
create incremental materialized view aqumv_mvt1_4 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 10 and c1 < 15;

analyze aqumv_mvt1_4;

set local answer_query_using_materialized_views = on;
-- complex exprs
explain(verbose, costs off) select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;

set local answer_query_using_materialized_views = off;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1 from aqumv_t1 where c1 > 10 and c1 < 15;
abort;

-- post quals
begin;
create incremental materialized view aqumv_mvt1_post_quals as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 20 and c1 < 30;

analyze aqumv_mvt1_post_quals;

set local answer_query_using_materialized_views = on;

explain(verbose, costs off)
  select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;

explain(verbose, costs off)
  select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;

explain(verbose, costs off)
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

set local answer_query_using_materialized_views = off;

select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > 1;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(c2) + 1) > abs(c1) + 2;
select c1, sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) + 1, abs(c2) + 1
  from aqumv_t1 where c1 > 20 and c1 < 30 and sqrt(abs(abs(c2) - c1 - 1) + 10) > 2; 

abort;

-- choose the best one if there are multiple chooses based on cost.
begin;
set local answer_query_using_materialized_views = on;

create incremental materialized view aqumv_mvt1_candidate_0 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3
  from aqumv_t1 where c1 > 30;
analyze aqumv_mvt1_candidate_0;
-- choose aqumv_mvt1_candidate_0
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;

create incremental materialized view aqumv_mvt1_candidate_1 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 30 and c1 < 40;
analyze aqumv_mvt1_candidate_1;
-- choose aqumv_mvt1_candidate_1 as it has lower cost(less rows).
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;

create incremental materialized view aqumv_mvt1_candidate_2 as
  select c1 as mc1, c2 as mc2, abs(c2) as mc3, abs(abs(c2) - c1 - 1) as mc4
  from aqumv_t1 where c1 > 30 and c1 < 500;
analyze aqumv_mvt1_candidate_2;
-- still choose aqumv_mvt1_candidate_1 as it has lowest cost(less rows).
explain(verbose, costs off)
select sqrt(abs(abs(c2) - c1 - 1) + abs(c2)) from aqumv_t1 where c1 > 30 and c1 < 40 and sqrt(abs(c2)) > 5.8;
abort;

reset optimizer;
reset answer_query_using_materialized_views;
drop table aqumv_t1 cascade;
