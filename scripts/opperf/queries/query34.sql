-- This query is to test Targeted Dispatch for Subqueries and queries involving init-plans.
--Today this feature is not working for targeted dispatch need to move correct o/p file to ans file once MPP-7867 gets resolved 
--set test_print_direct_dispatch_info=on;
select max(nscale *4) from opperfscale where nseg=1;
--explain select max(nscale *4) from opperfscale where nseg=1;
