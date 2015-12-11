-- sort: string comp

select l_returnflag from lineitem order by l_returnflag, l_comment
limit 200;
