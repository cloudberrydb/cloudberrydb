create table member_summary (member_id int, text text) distributed by (member_id);
create table keywords (keyword_id int, keyword text) distributed by (keyword_id);

insert into member_summary values
  (1, 'Interested in software development and software profiling');

insert into keywords values (100, 'software engineer');

insert into keywords values (101, 'software development');

