drop external table if exists rig_readable;
drop table if exists rig_errortable;

create readable external table rig_readable (like rig_xml)
  location ('gpfdist://127.0.0.1:8080/data/rig.xml#transform=rig_input') 
  --                   ^^^^^^^^^
  --                   replace this value
  --
  format 'text' (delimiter '|')
  log errors into rig_errortable segment reject limit 10;
