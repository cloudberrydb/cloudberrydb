DROP TABLE test_trig;
DROP FUNCTION fn_trig();
--start_ignore
drop language plpgsql;
--end_ignore
create language plpgsql;
CREATE TABLE test_trig(id int, aaa text) DISTRIBUTED BY (id);
CREATE OR REPLACE FUNCTION fn_trig() RETURNS TRIGGER LANGUAGE plpgsql NO SQL AS $$
BEGIN
   NEW.id = NEW.id + 1;
   RAISE NOTICE '%', NEW.id;
RETURN NEW;
END
$$;
INSERT INTO test_trig VALUES (1, 'before creating trigger');
SELECT gp_segment_id, * FROM test_trig;
CREATE TRIGGER test_trig_1
BEFORE INSERT OR UPDATE ON test_trig
FOR EACH ROW
    EXECUTE PROCEDURE fn_trig();
INSERT INTO test_trig VALUES (0, 'after creating trigger');
--start_ignore
SELECT gp_segment_id, * FROM test_trig;
--end_ignore
