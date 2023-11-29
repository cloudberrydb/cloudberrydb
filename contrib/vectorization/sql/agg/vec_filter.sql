SET vector.enable_vectorization = on;
SET optimizer=on;

explain (verbose on, costs off) select c2 from trow where c2 > 15 order by c2;
select c2 from trow where c2 > 15 order by c2;
