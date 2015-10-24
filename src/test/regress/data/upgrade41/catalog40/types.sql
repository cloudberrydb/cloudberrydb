-- Fill in type shells for types defined in the 4.0 Catalog:

-- Handle small number of composite types by hand
CREATE TYPE nb_classification AS (classes text[], accum float8[], apriori bigint[]);

-- Create dummy functions to handle refcursor:
--  This deals with the problem that the normal io functions for refcursor are not actually over
--  the correct datatypes.
CREATE FUNCTION dummy_cast_functions.textin(cstring) returns upg_catalog.refcursor 
    LANGUAGE internal as 'textout' STRICT IMMUTABLE;
CREATE FUNCTION dummy_cast_functions.textout(upg_catalog.refcursor) returns cstring 
    LANGUAGE internal as 'textout' STRICT IMMUTABLE;
CREATE FUNCTION dummy_cast_functions.textrecv(internal) returns upg_catalog.refcursor 
    LANGUAGE internal as 'textrecv' STRICT IMMUTABLE;
CREATE FUNCTION dummy_cast_functions.textsend(upg_catalog.refcursor) returns bytea
    LANGUAGE internal as 'textsend' STRICT IMMUTABLE;

-- Derived via the following SQL run within a 4.0 catalog
/*
\o /tmp/types.sql

SELECT 'CREATE TYPE upg_catalog.' || quote_ident(t.typname)
    || '('
    || E'\n  INPUT = ' 
    ||    case when pin.prorettype = t.oid 
               then 'upg_catalog.'
               else 'dummy_cast_functions.' 
          end || pin.proname 
    || E',\n  OUTPUT = '
    ||    case when pout.proargtypes[0] = t.oid 
               then 'upg_catalog.'
               else 'dummy_cast_functions.' 
          end || pout.proname 
    || case when precv.proname is not null 
            then E',\n  RECEIVE = ' ||
            case when precv.prorettype = t.oid 
                 then 'upg_catalog.'
                 else 'dummy_cast_functions.' 
            end || precv.proname 
            else '' end
    || case when psend.proname is not null 
            then E',\n  SEND = ' ||
            case when psend.proargtypes[0] = t.oid 
                 then 'upg_catalog.'
                 else 'dummy_cast_functions.' 
            end || psend.proname 
            else '' end
    || case when panalyze.proname is not null 
            then E',\n  ANALYZE = upg_catalog.' || panalyze.proname
            else '' end
    || case when t.typlen = -1 
            then E',\n  INTERNALLENGTH = VARIABLE'
            else E',\n  INTERNALLENGTH = ' || t.typlen end
    || case when t.typbyval 
            then E',\n  PASSEDBYVALUE' else '' end
    || E',\n  STORAGE = ' 
    || case when t.typstorage = 'p' then 'plain'
            when t.typstorage = 'x' then 'extended'
            when t.typstorage = 'm' then 'main'
            when t.typstorage = 'e' then 'external'
            else 'BROKEN' end
    || case when t.typdefault is not null 
            then E',\n  DEFAULT = ' || t.typdefault else '' end
    || case when telement.typname is not null
            then E',\n  ELEMENT = ' || telement.typname else '' end
    || E',\n  DELIMITER = ''' || t.typdelim  || ''''
    || E',\n  ALIGNMENT = '
    || case when t.typalign = 'c' then 'char'
            when t.typalign = 's' then 'int2'
            when t.typalign = 'i' then 'int4'
            when t.typalign = 'd' then 'double'
            else 'BROKEN' end
    || E'\n);'
    || case when (t.typtype = 'p' or t.typname in ('smgr', 'unknown'))
            then E'\nDROP TYPE upg_catalog._' || t.typname || ';' 
            else '' end
FROM pg_type t
     join pg_namespace n on (t.typnamespace = n.oid)
     join pg_proc pin on (t.typinput = pin.oid)
     join pg_proc pout on (t.typoutput = pout.oid)
     left join pg_proc precv on (t.typreceive = precv.oid)
     left join pg_proc psend on (t.typsend = psend.oid)
     left join pg_proc panalyze on (t.typanalyze = panalyze.oid)
     left join pg_type telement on (t.typelem = telement.oid)
WHERE t.typtype in ('b', 'p') and t.typname !~ '^_' and n.nspname = 'pg_catalog'
order by 1
;
*/
 CREATE TYPE upg_catalog."any"(               
   INPUT = upg_catalog.any_in,                
   OUTPUT = upg_catalog.any_out,              
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._any;
 CREATE TYPE upg_catalog."bit"(               
   INPUT = upg_catalog.bit_in,                
   OUTPUT = upg_catalog.bit_out,              
   RECEIVE = upg_catalog.bit_recv,            
   SEND = upg_catalog.bit_send,               
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog."char"(              
   INPUT = upg_catalog.charin,                
   OUTPUT = upg_catalog.charout,              
   RECEIVE = upg_catalog.charrecv,            
   SEND = upg_catalog.charsend,               
   INTERNALLENGTH = 1,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = char                           
 );
 CREATE TYPE upg_catalog."interval"(          
   INPUT = upg_catalog.interval_in,           
   OUTPUT = upg_catalog.interval_out,         
   RECEIVE = upg_catalog.interval_recv,       
   SEND = upg_catalog.interval_send,          
   INTERNALLENGTH = 16,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog."numeric"(           
   INPUT = upg_catalog.numeric_in,            
   OUTPUT = upg_catalog.numeric_out,          
   RECEIVE = upg_catalog.numeric_recv,        
   SEND = upg_catalog.numeric_send,           
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = main,                            
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog."time"(              
   INPUT = upg_catalog.time_in,               
   OUTPUT = upg_catalog.time_out,             
   RECEIVE = upg_catalog.time_recv,           
   SEND = upg_catalog.time_send,              
   INTERNALLENGTH = 8,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog."timestamp"(         
   INPUT = upg_catalog.timestamp_in,          
   OUTPUT = upg_catalog.timestamp_out,        
   RECEIVE = upg_catalog.timestamp_recv,      
   SEND = upg_catalog.timestamp_send,         
   INTERNALLENGTH = 8,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog."varchar"(           
   INPUT = upg_catalog.varcharin,             
   OUTPUT = upg_catalog.varcharout,           
   RECEIVE = upg_catalog.varcharrecv,         
   SEND = upg_catalog.varcharsend,            
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.abstime(             
   INPUT = upg_catalog.abstimein,             
   OUTPUT = upg_catalog.abstimeout,           
   RECEIVE = upg_catalog.abstimerecv,         
   SEND = upg_catalog.abstimesend,            
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.aclitem(             
   INPUT = upg_catalog.aclitemin,             
   OUTPUT = upg_catalog.aclitemout,           
   INTERNALLENGTH = 12,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.anyarray(            
   INPUT = upg_catalog.anyarray_in,           
   OUTPUT = upg_catalog.anyarray_out,         
   RECEIVE = upg_catalog.anyarray_recv,       
   SEND = upg_catalog.anyarray_send,          
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );                                           
 DROP TYPE upg_catalog._anyarray;
 CREATE TYPE upg_catalog.anyelement(          
   INPUT = upg_catalog.anyelement_in,         
   OUTPUT = upg_catalog.anyelement_out,       
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._anyelement;
 CREATE TYPE upg_catalog.bool(                
   INPUT = upg_catalog.boolin,                
   OUTPUT = upg_catalog.boolout,              
   RECEIVE = upg_catalog.boolrecv,            
   SEND = upg_catalog.boolsend,               
   INTERNALLENGTH = 1,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = char                           
 );
 CREATE TYPE upg_catalog.box(                 
   INPUT = upg_catalog.box_in,                
   OUTPUT = upg_catalog.box_out,              
   RECEIVE = upg_catalog.box_recv,            
   SEND = upg_catalog.box_send,               
   INTERNALLENGTH = 32,                       
   STORAGE = plain,                           
   ELEMENT = point,                           
   DELIMITER = ';',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.bpchar(              
   INPUT = upg_catalog.bpcharin,              
   OUTPUT = upg_catalog.bpcharout,            
   RECEIVE = upg_catalog.bpcharrecv,          
   SEND = upg_catalog.bpcharsend,             
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.bytea(               
   INPUT = upg_catalog.byteain,               
   OUTPUT = upg_catalog.byteaout,             
   RECEIVE = upg_catalog.bytearecv,           
   SEND = upg_catalog.byteasend,              
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.cid(                 
   INPUT = upg_catalog.cidin,                 
   OUTPUT = upg_catalog.cidout,               
   RECEIVE = upg_catalog.cidrecv,             
   SEND = upg_catalog.cidsend,                
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.cidr(                
   INPUT = upg_catalog.cidr_in,               
   OUTPUT = upg_catalog.cidr_out,             
   RECEIVE = upg_catalog.cidr_recv,           
   SEND = upg_catalog.cidr_send,              
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.circle(              
   INPUT = upg_catalog.circle_in,             
   OUTPUT = upg_catalog.circle_out,           
   RECEIVE = upg_catalog.circle_recv,         
   SEND = upg_catalog.circle_send,            
   INTERNALLENGTH = 24,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.cstring(             
   INPUT = upg_catalog.cstring_in,            
   OUTPUT = upg_catalog.cstring_out,          
   RECEIVE = upg_catalog.cstring_recv,        
   SEND = upg_catalog.cstring_send,           
   INTERNALLENGTH = -2,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = char                           
 );                                           
 DROP TYPE upg_catalog._cstring;
 CREATE TYPE upg_catalog.date(                
   INPUT = upg_catalog.date_in,               
   OUTPUT = upg_catalog.date_out,             
   RECEIVE = upg_catalog.date_recv,           
   SEND = upg_catalog.date_send,              
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.float4(              
   INPUT = upg_catalog.float4in,              
   OUTPUT = upg_catalog.float4out,            
   RECEIVE = upg_catalog.float4recv,          
   SEND = upg_catalog.float4send,             
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.float8(              
   INPUT = upg_catalog.float8in,              
   OUTPUT = upg_catalog.float8out,            
   RECEIVE = upg_catalog.float8recv,          
   SEND = upg_catalog.float8send,             
   INTERNALLENGTH = 8,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.gpaotid(             
   INPUT = upg_catalog.gpaotidin,             
   OUTPUT = upg_catalog.gpaotidout,           
   RECEIVE = upg_catalog.gpaotidrecv,         
   SEND = upg_catalog.gpaotidsend,            
   INTERNALLENGTH = 6,                        
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int2                           
 );
 CREATE TYPE upg_catalog.gpxlogloc(           
   INPUT = upg_catalog.gpxloglocin,           
   OUTPUT = upg_catalog.gpxloglocout,         
   RECEIVE = upg_catalog.gpxloglocrecv,       
   SEND = upg_catalog.gpxloglocsend,          
   INTERNALLENGTH = 8,                        
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.inet(                
   INPUT = upg_catalog.inet_in,               
   OUTPUT = upg_catalog.inet_out,             
   RECEIVE = upg_catalog.inet_recv,           
   SEND = upg_catalog.inet_send,              
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.int2(                
   INPUT = upg_catalog.int2in,                
   OUTPUT = upg_catalog.int2out,              
   RECEIVE = upg_catalog.int2recv,            
   SEND = upg_catalog.int2send,               
   INTERNALLENGTH = 2,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int2                           
 );
 CREATE TYPE upg_catalog.int2vector(          
   INPUT = upg_catalog.int2vectorin,          
   OUTPUT = upg_catalog.int2vectorout,        
   RECEIVE = upg_catalog.int2vectorrecv,      
   SEND = upg_catalog.int2vectorsend,         
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = plain,                           
   ELEMENT = int2,                            
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.int4(                
   INPUT = upg_catalog.int4in,                
   OUTPUT = upg_catalog.int4out,              
   RECEIVE = upg_catalog.int4recv,            
   SEND = upg_catalog.int4send,               
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.int8(                
   INPUT = upg_catalog.int8in,                
   OUTPUT = upg_catalog.int8out,              
   RECEIVE = upg_catalog.int8recv,            
   SEND = upg_catalog.int8send,               
   INTERNALLENGTH = 8,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.internal(            
   INPUT = upg_catalog.internal_in,           
   OUTPUT = upg_catalog.internal_out,         
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._internal;
 CREATE TYPE upg_catalog.language_handler(    
   INPUT = upg_catalog.language_handler_in,   
   OUTPUT = upg_catalog.language_handler_out, 
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._language_handler;
 CREATE TYPE upg_catalog.line(                
   INPUT = upg_catalog.line_in,               
   OUTPUT = upg_catalog.line_out,             
   RECEIVE = upg_catalog.line_recv,           
   SEND = upg_catalog.line_send,              
   INTERNALLENGTH = 32,                       
   STORAGE = plain,                           
   ELEMENT = float8,                          
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.lseg(                
   INPUT = upg_catalog.lseg_in,               
   OUTPUT = upg_catalog.lseg_out,             
   RECEIVE = upg_catalog.lseg_recv,           
   SEND = upg_catalog.lseg_send,              
   INTERNALLENGTH = 32,                       
   STORAGE = plain,                           
   ELEMENT = point,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.macaddr(             
   INPUT = upg_catalog.macaddr_in,            
   OUTPUT = upg_catalog.macaddr_out,          
   RECEIVE = upg_catalog.macaddr_recv,        
   SEND = upg_catalog.macaddr_send,           
   INTERNALLENGTH = 6,                        
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.money(               
   INPUT = upg_catalog.cash_in,               
   OUTPUT = upg_catalog.cash_out,             
   RECEIVE = upg_catalog.cash_recv,           
   SEND = upg_catalog.cash_send,              
   INTERNALLENGTH = 4,                        
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.name(                
   INPUT = upg_catalog.namein,                
   OUTPUT = upg_catalog.nameout,              
   RECEIVE = upg_catalog.namerecv,            
   SEND = upg_catalog.namesend,               
   INTERNALLENGTH = 64,                       
   STORAGE = plain,                           
   ELEMENT = char,                            
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.oid(                 
   INPUT = upg_catalog.oidin,                 
   OUTPUT = upg_catalog.oidout,               
   RECEIVE = upg_catalog.oidrecv,             
   SEND = upg_catalog.oidsend,                
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.oidvector(           
   INPUT = upg_catalog.oidvectorin,           
   OUTPUT = upg_catalog.oidvectorout,         
   RECEIVE = upg_catalog.oidvectorrecv,       
   SEND = upg_catalog.oidvectorsend,          
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = plain,                           
   ELEMENT = oid,                             
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.opaque(              
   INPUT = upg_catalog.opaque_in,             
   OUTPUT = upg_catalog.opaque_out,           
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._opaque;
 CREATE TYPE upg_catalog.path(                
   INPUT = upg_catalog.path_in,               
   OUTPUT = upg_catalog.path_out,             
   RECEIVE = upg_catalog.path_recv,           
   SEND = upg_catalog.path_send,              
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.point(               
   INPUT = upg_catalog.point_in,              
   OUTPUT = upg_catalog.point_out,            
   RECEIVE = upg_catalog.point_recv,          
   SEND = upg_catalog.point_send,             
   INTERNALLENGTH = 16,                       
   STORAGE = plain,                           
   ELEMENT = float8,                          
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.polygon(             
   INPUT = upg_catalog.poly_in,               
   OUTPUT = upg_catalog.poly_out,             
   RECEIVE = upg_catalog.poly_recv,           
   SEND = upg_catalog.poly_send,              
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.record(              
   INPUT = upg_catalog.record_in,             
   OUTPUT = upg_catalog.record_out,           
   RECEIVE = upg_catalog.record_recv,         
   SEND = upg_catalog.record_send,            
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );                                           
 DROP TYPE upg_catalog._record;
 CREATE TYPE upg_catalog.refcursor(           
   INPUT = dummy_cast_functions.textin,       
   OUTPUT = dummy_cast_functions.textout,     
   RECEIVE = dummy_cast_functions.textrecv,   
   SEND = dummy_cast_functions.textsend,      
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regclass(            
   INPUT = upg_catalog.regclassin,            
   OUTPUT = upg_catalog.regclassout,          
   RECEIVE = upg_catalog.regclassrecv,        
   SEND = upg_catalog.regclasssend,           
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regoper(             
   INPUT = upg_catalog.regoperin,             
   OUTPUT = upg_catalog.regoperout,           
   RECEIVE = upg_catalog.regoperrecv,         
   SEND = upg_catalog.regopersend,            
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regoperator(         
   INPUT = upg_catalog.regoperatorin,         
   OUTPUT = upg_catalog.regoperatorout,       
   RECEIVE = upg_catalog.regoperatorrecv,     
   SEND = upg_catalog.regoperatorsend,        
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regproc(             
   INPUT = upg_catalog.regprocin,             
   OUTPUT = upg_catalog.regprocout,           
   RECEIVE = upg_catalog.regprocrecv,         
   SEND = upg_catalog.regprocsend,            
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regprocedure(        
   INPUT = upg_catalog.regprocedurein,        
   OUTPUT = upg_catalog.regprocedureout,      
   RECEIVE = upg_catalog.regprocedurerecv,    
   SEND = upg_catalog.regproceduresend,       
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.regtype(             
   INPUT = upg_catalog.regtypein,             
   OUTPUT = upg_catalog.regtypeout,           
   RECEIVE = upg_catalog.regtyperecv,         
   SEND = upg_catalog.regtypesend,            
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.reltime(             
   INPUT = upg_catalog.reltimein,             
   OUTPUT = upg_catalog.reltimeout,           
   RECEIVE = upg_catalog.reltimerecv,         
   SEND = upg_catalog.reltimesend,            
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.smgr(                
   INPUT = upg_catalog.smgrin,                
   OUTPUT = upg_catalog.smgrout,              
   INTERNALLENGTH = 2,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int2                           
 );                                           
 DROP TYPE upg_catalog._smgr;
 CREATE TYPE upg_catalog.text(                
   INPUT = upg_catalog.textin,                
   OUTPUT = upg_catalog.textout,              
   RECEIVE = upg_catalog.textrecv,            
   SEND = upg_catalog.textsend,               
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.tid(                 
   INPUT = upg_catalog.tidin,                 
   OUTPUT = upg_catalog.tidout,               
   RECEIVE = upg_catalog.tidrecv,             
   SEND = upg_catalog.tidsend,                
   INTERNALLENGTH = 6,                        
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int2                           
 );
 CREATE TYPE upg_catalog.timestamptz(         
   INPUT = upg_catalog.timestamptz_in,        
   OUTPUT = upg_catalog.timestamptz_out,      
   RECEIVE = upg_catalog.timestamptz_recv,    
   SEND = upg_catalog.timestamptz_send,       
   INTERNALLENGTH = 8,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.timetz(              
   INPUT = upg_catalog.timetz_in,             
   OUTPUT = upg_catalog.timetz_out,           
   RECEIVE = upg_catalog.timetz_recv,         
   SEND = upg_catalog.timetz_send,            
   INTERNALLENGTH = 12,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = double                         
 );
 CREATE TYPE upg_catalog.tinterval(           
   INPUT = upg_catalog.tintervalin,           
   OUTPUT = upg_catalog.tintervalout,         
   RECEIVE = upg_catalog.tintervalrecv,       
   SEND = upg_catalog.tintervalsend,          
   INTERNALLENGTH = 12,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.trigger(             
   INPUT = upg_catalog.trigger_in,            
   OUTPUT = upg_catalog.trigger_out,          
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._trigger;
 CREATE TYPE upg_catalog.unknown(             
   INPUT = upg_catalog.unknownin,             
   OUTPUT = upg_catalog.unknownout,           
   RECEIVE = upg_catalog.unknownrecv,         
   SEND = upg_catalog.unknownsend,            
   INTERNALLENGTH = -2,                       
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = char                           
 );                                           
 DROP TYPE upg_catalog._unknown;
 CREATE TYPE upg_catalog.varbit(              
   INPUT = upg_catalog.varbit_in,             
   OUTPUT = upg_catalog.varbit_out,           
   RECEIVE = upg_catalog.varbit_recv,         
   SEND = upg_catalog.varbit_send,            
   INTERNALLENGTH = VARIABLE,                 
   STORAGE = extended,                        
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
 CREATE TYPE upg_catalog.void(                
   INPUT = upg_catalog.void_in,               
   OUTPUT = upg_catalog.void_out,             
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );                                           
 DROP TYPE upg_catalog._void;
 CREATE TYPE upg_catalog.xid(                 
   INPUT = upg_catalog.xidin,                 
   OUTPUT = upg_catalog.xidout,               
   RECEIVE = upg_catalog.xidrecv,             
   SEND = upg_catalog.xidsend,                
   INTERNALLENGTH = 4,                        
   PASSEDBYVALUE,                             
   STORAGE = plain,                           
   DELIMITER = ',',                           
   ALIGNMENT = int4                           
 );
