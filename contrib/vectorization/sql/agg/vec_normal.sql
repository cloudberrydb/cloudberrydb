SET vector.enable_vectorization = on;
SET optimizer=on;

explain (verbose on, costs off) select * from trow order by c1;
explain (verbose on, costs off) select c1 from trow order by c1;
explain (verbose on, costs off) select c2 from trow order by c2;


select * from trow order by c1;
select c1 from trow order by c1;
select c2 from trow order by c2;

