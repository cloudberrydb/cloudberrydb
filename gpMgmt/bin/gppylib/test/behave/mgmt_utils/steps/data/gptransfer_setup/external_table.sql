--
-- create a RET table
--
CREATE external web TABLE e_REGION  ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152))
                        execute 'bash -c "$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T r -s 1"'
                        on 1 format 'text' (delimiter '|');
--
-- create WET with similiar schema def as the original heap table
--
CREATE WRITABLE EXTERNAL TABLE wet_region ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152))
                            LOCATION ('gpfdist://10.1.2.12:2345/wet_region.tbl') FORMAT 'TEXT' (DELIMITER AS '|');

INSERT INTO wet_region SELECT * FROM e_REGION;

--
-- create a RET reading data from the file created by WET
--
CREATE EXTERNAL TABLE ret_region ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152))
                            LOCATION ('gpfdist://10.1.2.12:2345/wet_region.tbl') FORMAT 'TEXT' (DELIMITER AS '|');

CREATE TABLE region ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152));

INSERT INTO region SELECT * FROM e_REGION;
