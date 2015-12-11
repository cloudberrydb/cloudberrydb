-- nested loop outer join
-- explain
-- select count(*) from part p full outer join supplier s
-- on p.p_partkey > s.s_suppkey and p.p_size < s.s_nationkey
-- where p.p_partkey % 7 = 1 and s.s_suppkey % 11 = 1
-- ;

select 'Not supported';

