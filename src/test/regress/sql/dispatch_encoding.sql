-- More tests related to dispatching and QD->QE communication.

--
-- Test that error messages come out correctly, with non-default
-- client_encoding. (This test assumes that the regression database does
-- *not* use latin1, otherwise this doesn't test anything interesting.)
--
set client_encoding='utf8';

create function raise_notice(t text) returns void as $$
begin
  raise notice 'raise_notice called on "%"', t;
end;
$$ language plpgsql;

create function raise_error(t text) returns void as $$
begin
  raise 'raise_error called on "%"', t;
end;
$$ language plpgsql;

create table enctest(t text);
-- Unicode code point 196 is "Latin Capital Letter a with Diaeresis".
insert into enctest values ('funny char ' || chr(196));

select raise_notice(t) from enctest;
select raise_error(t) from enctest;

-- now do it again with latin1
set client_encoding='latin1';

select raise_notice(t) from enctest;
select raise_error(t) from enctest;
