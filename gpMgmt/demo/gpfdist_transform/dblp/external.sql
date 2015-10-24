drop external table if exists dblp_thesis_readable;
drop external table if exists dblp_thesis_writable;
drop table if exists dblp_errortable;

create readable external table dblp_thesis_readable (like dblp_thesis)
  location ('gpfdist://127.0.0.1:8080/data/dblp.xml#transform=dblp_input') 
  --                   ^^^^^^^^^
  --                   replace this value
  --
  format 'text' (delimiter '	')
  log errors into dblp_errortable segment reject limit 10;

create writable external table dblp_thesis_writable (like dblp_thesis)
  location ('gpfdist://127.0.0.1:8080/data/out.dblp_thesis.xml#transform=dblp_output') 
  --                   ^^^^^^^^^
  --                   replace this value
  --
  format 'text' (delimiter '	');
