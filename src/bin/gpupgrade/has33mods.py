import pg8000

def has33mods(masterdb):
    masterdb.execute('''select
        (select 1 from pg_class where relnamespace = 11 and 
         relname = 'pg_aocs') is not null as hasaocs,
        (select 1 from pg_attribute where attname = 'compresstype' and
         attrelid = 'pg_appendonly'::regclass) is not null as aocol,
        (select 1 from pg_class where relnamespace = 11 and
         relname = 'gp_configuration_history') is not null as hasgpconfhis;''')
    return masterdb.iterate_dict().next()

if __name__ == '__main__':
    conn = pg8000.Connection(user='swm', host='localhost', port=12000,
                             database='postgres')
    dets = has33mods(conn)
    print dets
