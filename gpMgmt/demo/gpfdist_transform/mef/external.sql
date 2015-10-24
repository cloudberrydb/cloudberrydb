drop external table if exists mef_readable;
drop table if exists mef_errortable;

create readable external table mef_readable (like mef_xml)
  location ('gpfdist://127.0.0.1:8080/data/RET990EZ_2006.xml#transform=mef_input') 
  --                   ^^^^^^^^^
  --                   replace this value
  --
  format 'text' (delimiter '|')
  log errors into mef_errortable segment reject limit 10;
