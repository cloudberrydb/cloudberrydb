drop table if exists dblp_thesis;

create table dblp_thesis (
    key        text, 	   -- e.g. ms/Brown92
    type       text, 	   -- e.g. masters
    author     text,  	   -- e.g. Kurt P. Brown
    title      text,  	   -- e.g. PRPL: A Database Workload Specification Language, v1.3.
    year       text,  	   -- e.g. 1992
    school     text  	   -- e.g. Univ. of Wisconsin-Madison
) distributed by (key);
