SELECT catalog_name, schema_name, schema_owner FROM information_schema.schemata;

SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gptransfer_schema';
