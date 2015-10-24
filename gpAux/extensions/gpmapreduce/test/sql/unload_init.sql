create table unload_test (f1 int, f2 text , f3 text) distributed by (f1);

copy unload_test from stdin;
1	one	uno
2	two	dos
3	three	treis
4	four	cuatro
5	\N	\N
6	null	null
7	text with space	"text" with 'quotes'
\.
