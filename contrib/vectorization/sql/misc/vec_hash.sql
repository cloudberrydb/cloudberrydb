-- create function

CREATE FUNCTION gp_vec_hash_chk(data_string text, data_type text, is_vec boolean) RETURNS SETOF RECORD
AS '$libdir/gp_vec_hash_chk', 'vec_hash_chk'
LANGUAGE C STRICT EXECUTE ON MASTER;


-- single

-- uint64
-- SELECT gp_vec_hash_chk('100','uint64',true);
-- SELECT gp_vec_hash_chk('100','uint64',false);
-- SELECT gp_vec_hash_chk('101','uint64',true);
-- SELECT gp_vec_hash_chk('101','uint64',false);
-- SELECT gp_vec_hash_chk('-10','uint64',true);
-- SELECT gp_vec_hash_chk('-10','uint64',false);

-- int64
SELECT gp_vec_hash_chk('100','int64',true);
SELECT gp_vec_hash_chk('100','int64',false);
SELECT gp_vec_hash_chk('101','int64',true);
SELECT gp_vec_hash_chk('101','int64',false);
SELECT gp_vec_hash_chk('-10','int64',true);
SELECT gp_vec_hash_chk('-10','int64',false);

-- uint32
-- SELECT gp_vec_hash_chk('100','uint32',true);
-- SELECT gp_vec_hash_chk('100','uint32',false);
-- SELECT gp_vec_hash_chk('101','uint32',true);
-- SELECT gp_vec_hash_chk('101','uint32',false);
-- SELECT gp_vec_hash_chk('-10','uint32',true);
-- SELECT gp_vec_hash_chk('-10','uint32',false);

-- int32
SELECT gp_vec_hash_chk('100','int32',true);
SELECT gp_vec_hash_chk('100','int32',false);
SELECT gp_vec_hash_chk('101','int32',true);
SELECT gp_vec_hash_chk('101','int32',false);
SELECT gp_vec_hash_chk('-10','int32',true);
SELECT gp_vec_hash_chk('-10','int32',false);

-- uint16
-- SELECT gp_vec_hash_chk('100','uint16',true);
-- SELECT gp_vec_hash_chk('100','uint16',false);
-- SELECT gp_vec_hash_chk('101','uint16',true);
-- SELECT gp_vec_hash_chk('101','uint16',false);
-- SELECT gp_vec_hash_chk('-10','uint16',true);
-- SELECT gp_vec_hash_chk('-10','uint16',false);

-- int16
SELECT gp_vec_hash_chk('100','int16',true);
SELECT gp_vec_hash_chk('100','int16',false);
SELECT gp_vec_hash_chk('101','int16',true);
SELECT gp_vec_hash_chk('101','int16',false);
SELECT gp_vec_hash_chk('-10','int16',true);
SELECT gp_vec_hash_chk('-10','int16',false);

-- uint8
-- SELECT gp_vec_hash_chk('100','uint8',true);
-- SELECT gp_vec_hash_chk('100','uint8',false);
-- SELECT gp_vec_hash_chk('101','uint8',true);
-- SELECT gp_vec_hash_chk('101','uint8',false);
-- SELECT gp_vec_hash_chk('-10','uint8',true);
-- SELECT gp_vec_hash_chk('-10','uint8',false);

-- int8
SELECT gp_vec_hash_chk('100','int8',true);
SELECT gp_vec_hash_chk('100','int8',false);
SELECT gp_vec_hash_chk('101','int8',true);
SELECT gp_vec_hash_chk('101','int8',false);
SELECT gp_vec_hash_chk('-10','int8',true);
SELECT gp_vec_hash_chk('-10','int8',false);

-- oid
SELECT gp_vec_hash_chk('100','oid',true);
SELECT gp_vec_hash_chk('100','oid',false);
SELECT gp_vec_hash_chk('101','oid',true);
SELECT gp_vec_hash_chk('101','oid',false);

-- float 
SELECT gp_vec_hash_chk('1.0','float',true);
SELECT gp_vec_hash_chk('1.0','float',false);
SELECT gp_vec_hash_chk('0.01','float',true);
SELECT gp_vec_hash_chk('0.01','float',false);
SELECT gp_vec_hash_chk('-0.01','float',true);
SELECT gp_vec_hash_chk('-0.01','float',false);

-- double
SELECT gp_vec_hash_chk('1.0001','double',true);
SELECT gp_vec_hash_chk('1.0001','double',false);
SELECT gp_vec_hash_chk('0.01','double',true);
SELECT gp_vec_hash_chk('0.01','double',false);
SELECT gp_vec_hash_chk('-0.01','double',true);
SELECT gp_vec_hash_chk('-0.01','double',false);

-- text
SELECT gp_vec_hash_chk('a','text',true);
SELECT gp_vec_hash_chk('a','text',false);
SELECT gp_vec_hash_chk('ab','text',true);       
SELECT gp_vec_hash_chk('ab','text',false);
SELECT gp_vec_hash_chk('abc','text',true);       
SELECT gp_vec_hash_chk('abc','text',false);
SELECT gp_vec_hash_chk('abcd','text',true);       
SELECT gp_vec_hash_chk('abcd','text',false);
SELECT gp_vec_hash_chk('abcde','text',true);       
SELECT gp_vec_hash_chk('abcde','text',false);
SELECT gp_vec_hash_chk('abcdef','text',true);       
SELECT gp_vec_hash_chk('abcdef','text',false);
SELECT gp_vec_hash_chk('abcdefg','text',true);       
SELECT gp_vec_hash_chk('abcdefg','text',false);
SELECT gp_vec_hash_chk('abcdefgh','text',true);       
SELECT gp_vec_hash_chk('abcdefgh','text',false);
SELECT gp_vec_hash_chk('abcdefghi','text',true);       
SELECT gp_vec_hash_chk('abcdefghi','text',false);
SELECT gp_vec_hash_chk('abcdefghij','text',true);       
SELECT gp_vec_hash_chk('abcdefghij','text',false);
SELECT gp_vec_hash_chk('abcdefghijk','text',true);       
SELECT gp_vec_hash_chk('abcdefghijk','text',false);
SELECT gp_vec_hash_chk('abcdefghijkl','text',true);       
SELECT gp_vec_hash_chk('abcdefghijkl','text',false);
SELECT gp_vec_hash_chk('abcdefghijklm','text',true);
SELECT gp_vec_hash_chk('abcdefghijklm','text',false);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfed','text',true);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfed','text',false);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss','text',true);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss','text',false);
SELECT gp_vec_hash_chk('a|bcdefghijklsaf|asdfdsfed','text',true);
SELECT gp_vec_hash_chk('a','text',false);
SELECT gp_vec_hash_chk('bcdefghijklsaf','text',false);
SELECT gp_vec_hash_chk('asdfdsfed','text',false);

-- null
SELECT gp_vec_hash_chk('100','null',true);
SELECT gp_vec_hash_chk('100','null',false);


-- more than one

-- int64 int64
SELECT gp_vec_hash_chk('100,101','int64,int64',true);
SELECT gp_vec_hash_chk('100,101','int64,int64',false);
SELECT gp_vec_hash_chk('101,102','int64,int64',true);
SELECT gp_vec_hash_chk('101,102','int64,int64',false);
SELECT gp_vec_hash_chk('-10,-11','int64,int64',true);
SELECT gp_vec_hash_chk('-10,-11','int64,int64',false);


-- int64 int32
SELECT gp_vec_hash_chk('100,101','int64,int32',true);
SELECT gp_vec_hash_chk('100,101','int64,int32',false);
SELECT gp_vec_hash_chk('101,102','int64,int32',true);
SELECT gp_vec_hash_chk('101,102','int64,int32',false);
SELECT gp_vec_hash_chk('-10,-11','int64,int32',true);
SELECT gp_vec_hash_chk('-10,-11','int64,int32',false);


-- int64 int16
SELECT gp_vec_hash_chk('100,101','int64,int16',true);
SELECT gp_vec_hash_chk('100,101','int64,int16',false);
SELECT gp_vec_hash_chk('101,102','int64,int16',true);
SELECT gp_vec_hash_chk('101,102','int64,int16',false);
SELECT gp_vec_hash_chk('-10,-11','int64,int16',true);
SELECT gp_vec_hash_chk('-10,-11','int64,int16',false);

-- int64 int8
SELECT gp_vec_hash_chk('100,101','int64,int8',true);
SELECT gp_vec_hash_chk('100,101','int64,int8',false);
SELECT gp_vec_hash_chk('101,102','int64,int8',true);
SELECT gp_vec_hash_chk('101,102','int64,int8',false);
SELECT gp_vec_hash_chk('-10,-11','int64,int8',true);
SELECT gp_vec_hash_chk('-10,-11','int64,int8',false);


-- int64 oid
SELECT gp_vec_hash_chk('100,101','int64,oid',true);
SELECT gp_vec_hash_chk('100,101','int64,oid',false);
SELECT gp_vec_hash_chk('101,102','int64,oid',true);
SELECT gp_vec_hash_chk('101,102','int64,oid',false);
SELECT gp_vec_hash_chk('-10,-11','int64,oid',true);
SELECT gp_vec_hash_chk('-10,-11','int64,oid',false);

-- int64 float
SELECT gp_vec_hash_chk('100,101.1','int64,float',true);
SELECT gp_vec_hash_chk('100,101.1','int64,float',false);
SELECT gp_vec_hash_chk('101,102.1','int64,float',true);
SELECT gp_vec_hash_chk('101,102.1','int64,float',false);
SELECT gp_vec_hash_chk('-10,-11.0','int64,float',true);
SELECT gp_vec_hash_chk('-10,-11.0','int64,float',false);

-- int64 float
SELECT gp_vec_hash_chk('100,101.1','int64,double',true);
SELECT gp_vec_hash_chk('100,101.1','int64,double',false);
SELECT gp_vec_hash_chk('101,102.1','int64,double',true);
SELECT gp_vec_hash_chk('101,102.1','int64,double',false);
SELECT gp_vec_hash_chk('-10,-11.0','int64,double',true);
SELECT gp_vec_hash_chk('-10,-11.0','int64,double',false);

-- int64 null
SELECT gp_vec_hash_chk('100,101','int64,null',true);
SELECT gp_vec_hash_chk('100,101','int64,null',false);
SELECT gp_vec_hash_chk('101,102','int64,null',true);
SELECT gp_vec_hash_chk('101,102','int64,null',false);
SELECT gp_vec_hash_chk('-10,-11','int64,null',true);
SELECT gp_vec_hash_chk('-10,-11','int64,null',false);


-- mixed
SELECT gp_vec_hash_chk('100,101,102,103,104','int64,int32,int16,int8,oid',true);
SELECT gp_vec_hash_chk('100,101,102,103,104','int64,int32,int16,int8,oid',false);
SELECT gp_vec_hash_chk('-100,-101,-102,-103,104','int64,int32,int16,int8,oid',true);
SELECT gp_vec_hash_chk('-100,-101,-102,-103,104','int64,int32,int16,int8,oid',false);

SELECT gp_vec_hash_chk('-10,-11,-12','null,int64,null',true);
SELECT gp_vec_hash_chk('-10,-11,-12','null,int64,null',false);

SELECT gp_vec_hash_chk('-10,-11,-12','null,null,null',true);
SELECT gp_vec_hash_chk('-10,-11,-12','null,null,null',false);

SELECT gp_vec_hash_chk('-10,-11,-12','null,null,int64',true);
SELECT gp_vec_hash_chk('-10,-11,-12','null,null,int64',false);

SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,1.109','text,double',true);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,1.109','text,double',false);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,1','text,null',true);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,1','text,null',false);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,10101','text,text',true);
SELECT gp_vec_hash_chk('abcdefghijklsafasdfdsfedasdddffdsfdsdsfdsfdskasjdfdasjkfjdsafkdsffddassss,10101','text,text',false);

-- drop function
DROP FUNCTION gp_vec_hash_chk(data_string text, data_type text, is_vec boolean);







