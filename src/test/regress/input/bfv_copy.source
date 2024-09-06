CREATE TABLE copy_converse_varify_error(a int, b text);
COPY copy_converse_varify_error FROM '@abs_srcdir@/data/copy_converse_varify_error.data'
WITH(FORMAT text, delimiter '|', "null" E'\\N', newline 'LF', escape 'OFF')
LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;
SELECT * FROM copy_converse_varify_error;
DROP TABLE copy_converse_varify_error;

CREATE TABLE copy_eol_on_nextrawpage(b text);
COPY copy_eol_on_nextrawpage FROM '@abs_srcdir@/data/eol_on_next_raw_page.data'
WITH(FORMAT text, delimiter '|', "null" E'\\N', newline 'LF', escape 'OFF')
LOG ERRORS SEGMENT REJECT LIMIT 10 ROWS;
SELECT count(*) FROM copy_eol_on_nextrawpage;
DROP TABLE copy_eol_on_nextrawpage;
