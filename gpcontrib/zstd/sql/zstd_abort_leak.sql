--
-- Test abort handling.
--

CREATE TABLE zstd_leak_test (distkey int4, id int4, t text)
WITH (appendonly=true, compresstype=zstd, orientation=column) DISTRIBUTED BY (distkey);

create function errfunc(int) returns int4 as
$$
begin
  if $1 >= 10 then
    -- raise division by zero
    return 1 / 0;
  end if;
  return $1;
end;
$$ language plpgsql immutable;

create table foo (x int) distributed by (x);
insert into foo select generate_series(1, 10);

-- The insert command in the loop errors out, after starting to insert to the
-- AO table and initializing the compressor. Repeat many times, so that if
-- there is a leak on abort, it would become noticeable.
--
-- If the ZSTD buffers are leaked, the leak isn't that large, so with a decent
-- amount of RAM, this still won't error out. But the excessive memory usage
-- should be visible in 'top', at least, if you look for it.
create function zstd_error_test(n int) returns int as
$$
declare
  i int;
  nerr int;
begin
  nerr := 0;
  for i in 1..n loop
    begin
      insert into zstd_leak_test select x, errfunc(x) from foo;
    exception
      when division_by_zero then nerr := nerr + 1;
    end;
  end loop;
  return nerr;
end;
$$ language plpgsql;

select zstd_error_test(10000);
