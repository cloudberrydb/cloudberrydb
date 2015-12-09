-- These queries depend on the tables created by the 'world' test.

with capitals as  (select country.code,id,city.name from city,country where city.countrycode = country.code AND city.id = country.capital) 
--start_ignore
select * from capitals,countrylanguage where capitals.code = countrylanguage.countrycode and isofficial='true' order by capitals.code,countrylanguage.language;
--end_ignore
--query2
--start_ignore
with lang_total as ( select count(*) as lang_count,country.code,countrylanguage.countrycode from country join countrylanguage on(country.code=countrylanguage.countrycode and governmentform='Federal Republic') group by country.code,countrylanguage.countrycode order by country.code)
--end_ignore 
--start_ignore
select lang_count,country.code,country.name,country.continent,country.region,country.population from country left outer join lang_total on (lang_total.code = country.code) where country.indepyear > 1970 and lang_total.lang_count >=1 and lang_total.lang_count >=1 order by lang_total.lang_count desc;
--end_ignore

-- Using CTE in the WHERE clause

--query3
with lang_total as ( select lang_count,countrycode from(
(
select count(*) as lang_count,country.code,countrylanguage.countrycode from country join countrylanguage on (country.code=countrylanguage.countrycode and governmentform='Federal Republic') group by country.code,countrylanguage.countrycode order by country.code)
 
   UNION ALL
 
  (
select count(*) as lang_count,country.code,countrylanguage.countrycode from country join countrylanguage on (country.code=countrylanguage.countrycode and governmentform='Monarchy') group by country.code,countrylanguage.countrycode order by country.code)
 
 ) FOO1
)
select * from(
select count(*) as cnt,country.code,country.name from country,countrylanguage where country.code=countrylanguage.countrycode group by country.code,country.name) AS FOO where foo.cnt = (select max(lang_count) from lang_total) order by foo.code;

--query4
with diversecountries as
(select country.code,country.name,country.capital,d.CNT from country, (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode HAVING count(*) > 6) d where d.countrycode = country.code and country.gnp > 100000)

select diversecountries.name,city.name,diversecountries.CNT from diversecountries,city where city.id = diversecountries.capital;


--query5 -Using a CTE in the select list
with bigcities as
(
select city.id,city.name,city.population,city.countrycode from city where city.population >= 0.5 * (select population from country where country.code=city.countrycode)
)
select
(select max(bigcities.population) from bigcities where bigcities.countrycode='QAT') as MAX_POP,(select avg(bigcities.population)from bigcities) AS WORLD_AVG, city.name,city.population from city where city.countrycode='QAT';

--query6 using CTE in the select list with a qual
with diversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country, 
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) > 6) d
 where d.countrycode = country.code and country.gnp > 100000)
select * from
(
select
(select max(CNT) from diversecountries where  diversecountries.code = country.code) CNT,country.name COUNTRY,city.name CAPITAL
from country,city where country.capital = city.id) FOO where FOO.CNT is not null;


--queries Using a CTE in the HAVING clause

with notdiversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country,
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) < 3) d
 where d.countrycode = country.code and country.gnp > 100000)

select country.name COUNTRY,city.name CAPITAL,count(*) LANGCNT from
country,city,countrylanguage
where country.code = countrylanguage.countrycode and country.capital = city.id
group by country.name,city.name
HAVING count(*) NOT IN (select CNT from notdiversecountries where notdiversecountries.name = country.name)
order by country.name
LIMIT 10;


with somecheapasiandiversecountries as
(
 select FOO.code,FOO.COUNTRY,FOO.CAPITAL,FOO.headofstate,count(*) ASIAN_COUNT from
 (
 select country.code,country.name COUNTRY,city.name CAPITAL,country.headofstate
 from country,city
 where country.capital = city.id 
 and country.gnp < 10000
 and country.region = 'Southeast Asia'
 and country.continent = 'Asia'
 
 UNION ALL

 select country.code,country.name COUNTRY,city.name CAPITAL,country.headofstate
 from country,city
 where country.capital = city.id 
 and country.gnp < 10000
 and country.region = 'Eastern Asia'
 and country.continent = 'Asia'

 UNION ALL

 select country.code,country.name COUNTRY,city.name CAPITAL,country.headofstate
 from country,city
 where country.capital = city.id 
 and country.gnp < 10000
 and country.region = 'Middle East'
 and country.continent = 'Asia'
 ) FOO, countrylanguage
 where FOO.code = countrylanguage.countrycode
 group by FOO.code,FOO.COUNTRY,FOO.CAPITAL,FOO.headofstate,countrylanguage.countrycode
 HAVING count(*) >=
 (select min(CNT) FROM
   (select count(*) CNT,country.code from countrylanguage,country
    where countrylanguage.countrycode=country.code
    and country.continent = 'Asia'
    and country.region = 'Southern and Central Asia'
    group by country.code
   ) FOO1
 )
)

select FOO.code,FOO.COUNTRY,FOO.CAPITAL,FOO.headofstate,count(*) COMPARED_WITH_CHEAP_ASIAN_CNT
from
(
select country.code,country.name COUNTRY,city.name CAPITAL,country.headofstate
from country,city
where country.capital = city.id 
and country.continent = 'North America'


UNION ALL

select country.code,country.name COUNTRY,city.name CAPITAL,country.headofstate
from country,city
where country.capital =	city.id	
and country.continent =	'South America'
) FOO,countrylanguage

where FOO.code = countrylanguage.countrycode
group by FOO.code,FOO.COUNTRY,FOO.CAPITAL,FOO.headofstate

HAVING count(*)  >=
 (select min(ASIAN_COUNT) FROM
   (select ASIAN_COUNT FROM somecheapasiandiversecountries,country
    where somecheapasiandiversecountries.code = country.code
    and country.gnp >= country.gnpold
   ) ASIANCOUNT
 )
order by COUNTRY;

--queries with a single CTE used more than once in different parts of the main query

-- query1
-- Similar to genetech query 'dxAcctMonthlyTrending_Avastin_WITH_Clause.sql'
-- This kind of query is their only use case for CTE. We dont error, we give correct results,use shared scan here and we are good ! 

select count(*) from
( select r.* from
  ( with fact as 
     (
      select country.name as COUNTRY,country.code,city.name as CAPITAL,S_POPULATION,S_GNP,AVG_LIFE,AGG1.region
      from      
         (select
         sum(case when (city.population >= 0.5 * country.population) then country.population else city.population end) as S_POPULATION,
         sum(case when (gnp >= gnpold) then gnp else gnpold end) as S_GNP,
         avg(case when (lifeexpectancy > 60) then 50 else lifeexpectancy end) as AVG_LIFE,country.region
         from country,city  
         where governmentform != 'Constitutional Monarchy'
         and country.capital = city.id
         and indepyear > 0
         group by country.region) AGG1
         ,country,city
         where country.capital = city.id
         and country.region = AGG1.region
      )
     
     select code,COUNTRY,CAPITAL,S_POPULATION,S_GNP,AVG_LIFE,language as OFFICIALLANGUAGE,region
     from fact,countrylanguage
     where fact.code = countrylanguage.countrycode and isofficial = 'True'
     and fact.region = 'South America'
     
     UNION ALL
     
     select code,COUNTRY,CAPITAL,S_POPULATION,S_GNP,AVG_LIFE,language as OFFICIALLANGUAGE,region
     from fact,countrylanguage
     where fact.code = countrylanguage.countrycode and isofficial = 'True'
     and fact.region = 'North America'
     
     UNION ALL
     
     select code,COUNTRY,CAPITAL,S_POPULATION,S_GNP,AVG_LIFE,language as OFFICIALLANGUAGE,region
     from fact,countrylanguage
     where fact.code = countrylanguage.countrycode and isofficial = 'True'
     and fact.region = 'Caribbean'
 ) as r
 left join
  (
   select 'ARG' as CODE UNION ALL
   select 'BOL' as CODE UNION ALL
   select 'BRA' as CODE UNION ALL
   select 'PER' as CODE UNION ALL
   select 'URY' as CODE UNION ALL
   select 'IND' as CODE  UNION ALL
   select 'LCA' as CODE UNION ALL
   select 'VCT' as CODE
   ) as r1
on r.code = r1.code) AS FOO;


-- query2
with alleuropeanlanguages as 
(select country.code,country.name COUNTRY, city.name CAPITAL, language, isofficial, percentage
 FROM country,city,countrylanguage
 WHERE country.code = countrylanguage.countrycode
 and country.capital = city.id
 and country.continent = 'Europe')

select * from
(select * from alleuropeanlanguages where isofficial='True') e1,
(select * from alleuropeanlanguages where percentage > 50) e2
where e1.code = e2.code order by e2.COUNTRY,e1.language;


-- query3

with allcountrystats as 
( select country.code,country.name,count(distinct city.id) CITY_CNT,
  count(distinct countrylanguage.language) LANG_CNT
  from country,city,countrylanguage
  where country.code = city.countrycode
  and country.code = countrylanguage.countrycode
  group by country.code,country.name
)

select sum(FOO.CITY_CNT) REGION_CITY_CNT,sum(FOO.LANG_CNT) REGION_LANG_CNT,FOO.region
FROM
(
select allcountrystats.code,allcountrystats.name COUNTRY,CITY_CNT,LANG_CNT,country.region,city.name CAPITAL
from allcountrystats,country,city
where allcountrystats.code = country.code
and country.capital = city.id
and CITY_CNT/LANG_CNT > 1
and country.continent = 'Asia'

UNION ALL

select allcountrystats.code,allcountrystats.name COUNTRY,CITY_CNT,LANG_CNT,country.region,city.name CAPITAL
from allcountrystats,country,city
where allcountrystats.code = country.code
and country.capital = city.id
and CITY_CNT/LANG_CNT > 1
and country.continent = 'North America'

UNION ALL

select allcountrystats.code,allcountrystats.name COUNTRY,CITY_CNT,LANG_CNT,country.region,city.name CAPITAL
from allcountrystats,country,city
where allcountrystats.code = country.code
and country.capital = city.id
and CITY_CNT/LANG_CNT > (select  max(CITY_CNT/LANG_CNT)  from allcountrystats,country where allcountrystats.code = country.code AND country.continent='Europe')
) FOO
,allcountrystats,country

WHERE allcountrystats.code = country.code
and FOO.region = country.region
group by FOO.region order by FOO.region;


--query 4
with diversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country,
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) > 6) d
 where d.countrycode = country.code and country.gnp > 100000)

select d1.code,d1.name,d1.capital,city.name CAPITAL_CITY,d1.CNT,d2.CNT
from
diversecountries d1 left join country
ON (d1.code = country.code AND d1.CNT < 8)
left join diversecountries d2
ON (country.code = d2.code AND d2.CNT > 8)
INNER JOIN city
ON(d1.capital = city.id)
ORDER BY d1.name;


--query 5 Use CTE more than once in select list , from clause and where clause without correlation
with official_languages as
(
 select country.code,country.name,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and isofficial = 'True'
 and country.governmentform NOT  IN (select 'Commonwealth of the US' UNION ALL select 'Monarchy (Sultanate)' UNION ALL select 'Monarchy')
 and country.gnp > (select min(gnpold) from country where country.region = 'Western Europe')
)

select 
( select max(CNT) from (select count(*) CNT,o1.name from official_languages o1, official_languages o2
  where o1.code = o2.code group by o1.name) FOO
),* from official_languages;


--query 6 Use CTE in the main query and subqueries within the main query

with bad_headofstates as 
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)
select OUTERMOST_FOO.*,bad_headofstates.headofstate from (
select avg(population),region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country 
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
order by OUTERMOST_FOO.region,bad_headofstates.headofstate LIMIT 40;


-- query 7 Use CTE in the main query, where clause and having clause
with district_population as 
(select sum(city.population) DISTRICT_POP,count(*) NUM_CITIES,district,countrycode,country.name COUNTRY
 from city,country
 where city.countrycode = country.code
 group by district,countrycode,country.name
 HAVING (sum(city.population)/count(*)) > ( select avg(population) from city where countrycode = 'CHN'))

select sum(FOO.DISTRICT_POP),sum(FOO.NUM_CITIES),COUNTRY,CAPITAL,CAPITAL_POP
from
(

(select district_population.*,city.name CAPITAL,city.population CAPITAL_POP from
district_population,country,city
where district_population.countrycode = country.code AND city.id = country.capital
AND DISTRICT_POP >= ( select avg(DISTRICT_POP) FROM district_population where district IN (select district from city where countrycode = 'USA') )
order by COUNTRY,district)
UNION ALL
(select district_population.*,city.name CAPITAL,city.population CAPITAL_POP from
district_population,country,city
where district_population.countrycode = country.code AND city.id = country.capital
AND DISTRICT_POP >= ( select avg(DISTRICT_POP) FROM district_population where district IN (select district from city where countrycode = 'IND') )
order by COUNTRY,district)
UNION ALL
(select district_population.*,city.name CAPITAL,city.population CAPITAL_POP from
district_population,country,city
where district_population.countrycode = country.code AND city.id = country.capital
AND DISTRICT_POP >= ( select avg(DISTRICT_POP) FROM district_population where district IN (select district from city where countrycode = 'CHN') )
order by COUNTRY,district)

) FOO

WHERE FOO.CAPITAL_POP > (select min(DISTRICT_POP) from district_population)

group by FOO.COUNTRY,FOO.CAPITAL,FOO.CAPITAL_POP

HAVING sum(FOO.DISTRICT_POP) >= ( select avg(DISTRICT_POP) FROM district_population where district IN (select district from city where countrycode = 'AUS') )
AND (sum(FOO.DISTRICT_POP)/sum(FOO.NUM_CITIES)) <= 
( select avg(DISTRICT_POP) FROM district_population where district IN (select district from city where countrycode = 'USA' or countrycode = 'IND' or countrycode = 'CHN'))
order by FOO.country;

-- query8 Use CTE in the select list and the from clause - MPP-13943
with official_languages as
(
 select country.code,country.name,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and isofficial = 'True'
)

select
( select max(CNT) from (select count(*) CNT from official_languages) FOO	
)
,* from official_languages order by official_languages.code,official_languages.language;

 
-- Queries using multiple CTEs

-- query1 - all CTEs being used once in the main query
with capitals as 
(select country.code,id,city.name from city,country 
 where city.countrycode = country.code AND city.id = country.capital),
lang_total as
( select count(*) as lang_count,country.code,countrylanguage.countrycode
  from country join countrylanguage on (country.code=countrylanguage.countrycode and governmentform='Federal Republic')
  group by country.code,countrylanguage.countrycode order by country.code),
alleuropeanlanguages as 
(select country.code,country.name COUNTRY, city.name CAPITAL, language, isofficial, percentage
 FROM country,city,countrylanguage
 WHERE country.code = countrylanguage.countrycode
 and country.capital = city.id
 and country.continent = 'Europe'),
diversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country,
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) > 6) d
 where d.countrycode = country.code and country.gnp > 100000)

select capitals.name CAPITAL,lang_total.lang_count COUNTRY_LANG_COUNT,capitals.code,country.name
from capitals,lang_total,country
where capitals.code = lang_total.countrycode
and capitals.code = country.code
and country.code NOT IN (select diversecountries.code from diversecountries,alleuropeanlanguages
                         where diversecountries.code = alleuropeanlanguages.code)
order by capitals.code;

-- query 2 multiple CTEs being used multiple times through joins
with longlivingregions as 
(
select FOO.*,count(distinct language) as "lang_count"
from(
     select
       sum(population) as "REGION_POP",
       sum(gnp) as "REGION_GNP",
       avg(lifeexpectancy) as "REGION_LIFETIME",region
     from
      country
     group by region
    ) FOO,countrylanguage,country
where
   country.code = countrylanguage.countrycode
   and FOO.region = country.region
group by
FOO.region,foo."REGION_POP",foo."REGION_GNP",foo."REGION_LIFETIME"),

denseregions as 
(
select FOO.*,count(distinct language) as "lang_count",
       sum(surfacearea) as "REGION_SURFACE_AREA"
from(
     select
       sum(population) as "REGION_POP",
       sum(gnp) as "REGION_GNP",
       region
     from
      country
     group by region
    ) FOO,countrylanguage,country
where
   country.code = countrylanguage.countrycode
   and FOO.region = country.region
   and FOO."REGION_POP" != 0
group by
FOO.region,foo."REGION_POP",foo."REGION_GNP"
order by sum(surfacearea)/foo."REGION_POP" desc),

allcountrystats as 
( select country.code,country.name,count(distinct city.id) CITY_CNT,
  count(distinct countrylanguage.language) LANG_CNT
  from country,city,countrylanguage
  where country.code = city.countrycode
  and country.code = countrylanguage.countrycode
  group by country.code,country.name
)

select allcountrystats.CITY_CNT,allcountrystats.LANG_CNT,allcountrystats.name,
       "REGION_SURFACE_AREA","REGION_LIFETIME",longlivingregions."REGION_POP",longlivingregions.lang_count,longlivingregions."REGION_GNP",longlivingregions.region
from longlivingregions,denseregions,allcountrystats,country
where longlivingregions.region = denseregions.region and allcountrystats.code = country.code and country.region = longlivingregions.region
and country.indepyear between 1800 and 1850

UNION ALL

select allcountrystats.CITY_CNT,allcountrystats.LANG_CNT,allcountrystats.name,
       "REGION_SURFACE_AREA","REGION_LIFETIME",longlivingregions."REGION_POP",longlivingregions.lang_count,longlivingregions."REGION_GNP",longlivingregions.region
from longlivingregions,denseregions,allcountrystats,country
where longlivingregions.region = denseregions.region and allcountrystats.code = country.code and country.region = longlivingregions.region
and country.indepyear between 1850 and 1900

UNION ALL

select allcountrystats.CITY_CNT,allcountrystats.LANG_CNT,allcountrystats.name,
       "REGION_SURFACE_AREA","REGION_LIFETIME",longlivingregions."REGION_POP",longlivingregions.lang_count,longlivingregions."REGION_GNP",longlivingregions.region
from longlivingregions,denseregions,allcountrystats,country
where longlivingregions.region = denseregions.region and allcountrystats.code = country.code and country.region = longlivingregions.region
and country.indepyear > 1900
order by name
LIMIT 50;

-- Queries with multiple CTEs referencing one another

-- query1 
with city_official_language as
(select city.id,city.name CITY,country.name COUNTRY,countrylanguage.language,city.population,country.capital 
 from city,countrylanguage,country
 where city.countrycode = country.code
 and country.code = countrylanguage.countrycode
 and countrylanguage.isofficial = 'True'
),
capital_official_language as 
(select c2.CITY, c1.COUNTRY,c1.language OFFICIAL_LANGUAGE,c1.CITY CAPITAL
 from city_official_language c1 , city_official_language c2 
 where c1.id = c2.capital
 and c1.id != c2.id
),
alleuropeanlanguages as 
(select c.COUNTRY,c.CITY,c.CAPITAL,c.OFFICIAL_LANGUAGE
 from capital_official_language c, country
 where c.COUNTRY = country.name 
 and country.continent = 'Europe'
)

select code,COUNTRY,CITY,alleuropeanlanguages.CAPITAL,OFFICIAL_LANGUAGE from alleuropeanlanguages,country
where alleuropeanlanguages.COUNTRY = country.name
and alleuropeanlanguages.city in (select CITY from city_official_language)
and alleuropeanlanguages.OFFICIAL_LANGUAGE IN (select OFFICIAL_LANGUAGE from capital_official_language)
order by code,country,city,official_language
limit 100;

--query2
--start_ignore
with allcitystats as 
( select city.name CITY,city.id,country.name COUNTRY,city.district,city.population as CITY_POP
  from
  city,country
  where city.countrycode = country.code
),
alldistrictstats as 
( select allcitystats.district,allcitystats.COUNTRY,sum(CITY_POP) DISTRICT_POP,
  count(CITY) as D_CITY_CNT
  from allcitystats
  group by allcitystats.district,allcitystats.COUNTRY
  order by district,COUNTRY
),
allcountrystats as 
( select alldistrictstats.COUNTRY,country.code,sum(D_CITY_CNT) C_CITY_CNT,
  count(distinct countrylanguage.language) C_LANG_CNT
  from alldistrictstats,country,countrylanguage
  where alldistrictstats.COUNTRY = country.name
  and country.code = countrylanguage.countrycode
  group by COUNTRY,code
),
asian_region_stats as 
(
select sum(FOO.C_CITY_CNT) REGION_CITY_CNT,sum(FOO.C_LANG_CNT) REGION_LANG_CNT,FOO.region
FROM
(
select allcountrystats.code,allcountrystats.COUNTRY,C_CITY_CNT,C_LANG_CNT,country.region,city.name CAPITAL
from allcountrystats,country,city
where allcountrystats.code = country.code
and country.capital = city.id
and C_CITY_CNT/C_LANG_CNT > 1
and country.continent = 'Asia') FOO
,allcountrystats,country

WHERE allcountrystats.code = country.code
and FOO.region = country.region
group by FOO.region order by FOO.region
)

select * from
(
select REGION_CITY_CNT as CITY_CNT,REGION_LANG_CNT as LANG_CNT, region as IDENTIFIER from asian_region_stats
UNION ALL
(
select sum(FOO.C_CITY_CNT) CITY_CNT,sum(FOO.C_LANG_CNT) LANG_CNT,FOO.region as IDENTIFIER
FROM
(
select allcountrystats.code,allcountrystats.COUNTRY,C_CITY_CNT,C_LANG_CNT,country.region,allcitystats.CITY CAPITAL
from allcountrystats,country,allcitystats
where allcountrystats.code = country.code
and country.capital = allcitystats.id
and C_CITY_CNT/C_LANG_CNT > 1
and country.continent = 'Europe') FOO
,allcountrystats,country

WHERE allcountrystats.code = country.code
and FOO.region = country.region
group by FOO.region order by FOO.region
)
) FOO1
order by FOO1.identifier,FOO1.city_cnt;
--end_ignore      

-- queries using column lists for CTEs

-- query 1 use column list despite having no duplicate names
with capitals("C","ID","CAP") as 
(select country.code,id,city.name from city,country 
 where city.countrycode = country.code AND city.id = country.capital),

lang_total(LC,CC,CLC) as
( select count(*) as lang_count,country.code,countrylanguage.countrycode
  from country join countrylanguage on (country.code=countrylanguage.countrycode and governmentform='Federal Republic')
  group by country.code,countrylanguage.countrycode order by country.code) 

select country.name,"C","ID","CAP",lang_total.lc
from capitals,lang_total,country
where capitals."C" = country.code
and country.code = lang_total.cc;



-- query 3 use column list when there are duplicate names within the CTE
with capitals("CO_C","C_ID","CAPITAL",country) as 
(select country.code,id,city.name,country.name from city,country 
 where city.countrycode = country.code AND city.id = country.capital)

select "CO_C","C_ID","CAPITAL",COUNTRY from capitals where "CO_C"='SMR';


-- query4 use column list within another CTE
with capitals("CO_C","C_ID","CAPITAL",country) as 
(select country.code,id,city.name,country.name from city,country 
 where city.countrycode = country.code AND city.id = country.capital),
south_asian_capitals(A_CO_C,"C_ID",A_C_NAME,A_CO_NAME) as 
(select "CO_C","C_ID","CAPITAL",country from capitals,country where capitals."CO_C"=country.code and country.region = 'Southern and Central Asia')
select "a_co_c","C_ID",A_c_NaMe,"a_co_name" from south_asian_capitals order by A_CO_C;

-- queries using CTEs in initplans and main plan 

--query1 using CTE in the select list(initplan) . One CTE using another CTE in it's initplan
with gnpstats as 
(
select REG_GNP/REG_OLD_GNP as GNP_INDEX,region from
(select sum(gnpold) as REG_OLD_GNP,
 sum(gnp) as REG_GNP,region
 from country
 group by region) FOO
where (case when (REG_GNP/REG_OLD_GNP) > 0 then 1.2 * (REG_GNP/REG_OLD_GNP) else null end) between 0.5 and 1.5
order by region
),
gnp_index_compare as 
(
select (select max(GNP_INDEX) from gnpstats) M_GNP_IDX,(gnp/gnpold) as GNP_IDX,country.name from
country where  country.continent = 'Asia'
)
select (select min(GNP_IDX) from gnp_index_compare) MIN_COUNTRY_GNP_IDX, (select max(GNP_INDEX) from gnpstats) MAX_REG_GNP_IDX,city.name CAPITAL,country.name COUNTRY
 from city,country where city.id = country.capital and country.continent='Europe';


--query2 using the CTE in the where clause(initplan) of the main query. One CTE using another CTE in it's where clause as it's initplan
with diversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country, 
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) > 6) d
 where d.countrycode = country.code and country.gnp > 100000),

notdiversecountries as
(select country.code,country.name,country.capital,d.CNT
 from country,
 (select countrylanguage.countrycode,count(*) as CNT from countrylanguage group by countrycode
  HAVING count(*) < 3) d
 where d.countrycode = country.code and country.gnp > 100000
 and d.CNT < (select max(CNT) from diversecountries))
select LANG_CNT,name from
(
select count(*) LANG_CNT,country.name,country.code from country,countrylanguage
where country.code = countrylanguage.countrycode and country.continent = 'North America'
group by country.name,country.code
) FOO
where FOO.LANG_CNT between (select min(CNT) from notdiversecountries) AND (select max(CNT) from diversecountries);



--query3 using CTE more than once in the same initplan and also more than once in the main query
with alleuropeanlanguages as 
(select country.code,country.name COUNTRY, city.name CAPITAL, language, isofficial, percentage
 FROM country,city,countrylanguage
 WHERE country.code = countrylanguage.countrycode
 and country.capital = city.id
 and country.continent = 'Europe'),
all_official_languages as
(
 select country.code,country.name,alleuropeanlanguages.language,alleuropeanlanguages.percentage
 from
 country,alleuropeanlanguages
 where country.code = alleuropeanlanguages.code and isofficial = 'True'
 and country.governmentform NOT  IN (select 'Commonwealth of the US' UNION ALL select 'Monarchy (Sultanate)' UNION ALL select 'Monarchy')
 and country.gnp > (select min(gnpold) from country where country.region = 'Western Europe')
)

SELECT avg(percentage),language
FROM
(
select country.name,city.name,percentage,language
from country,city,all_official_languages
where country.code = all_official_languages.code and country.capital = city.id and country.continent = 'Europe'

UNION ALL

select country.name,city.name,percentage,language
from country,city,all_official_languages
where country.code = all_official_languages.code and country.capital = city.id and country.continent = 'Asia'
) FOO 
group by FOO.language
HAVING avg(percentage) >=
(select avg(percentage) from 
( select percentage from all_official_languages,country where country.code = all_official_languages.code and country.region = 'British Islands'
  UNION
  select percentage from all_official_languages,country where country.code = all_official_languages.code and country.region = 'Western Europe'
) FOO )
order by FOO.language;  



-- sanity tests with queries using CTEs in insert,update,delete and create

-- query 1 CTAS using CTE
create table bad_headofstates as 
(
with bad_headofstates as 
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)
select OUTERMOST_FOO.*,bad_headofstates.headofstate from (
select avg(population),region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country 
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
order by OUTERMOST_FOO.region,bad_headofstates.headofstate LIMIT 40
);

select * from bad_headofstates order by region,headofstate;

--query 2 insert using CTE
insert into bad_headofstates
(
with bad_headofstates as 
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)
select OUTERMOST_FOO.*,bad_headofstates.headofstate from (
select avg(population),region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country 
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
order by OUTERMOST_FOO.region,bad_headofstates.headofstate LIMIT 40
);

select * from bad_headofstates order by region,headofstate;

--query3 update using CTE
update bad_headofstates set region = cm.region FROM
(

with bad_headofstates as
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)

select avg(OUTERMOST_FOO.AVG),OUTERMOST_FOO.region from (
select avg(population) AVG,region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
and OUTERMOST_FOO.region = 'Central America'
group by OUTERMOST_FOO.region
order by OUTERMOST_FOO.region
) cm
where bad_headofstates.region = 'Caribbean';
 
select * from bad_headofstates order by avg,region,headofstate;

--query4 delete using CTE
delete from bad_headofstates USING
(
with bad_headofstates as
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)

select avg(OUTERMOST_FOO.AVG),OUTERMOST_FOO.region from (
select avg(population) AVG,region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
and OUTERMOST_FOO.region = 'Central America'
group by OUTERMOST_FOO.region
order by OUTERMOST_FOO.region
) as  cm
where bad_headofstates.region = cm.region;

select * from bad_headofstates order by region,headofstate;

-- delete using CTE in its subplan
delete from bad_headofstates where bad_headofstates.avg NOT IN
(
with bad_headofstates as
(
 select country.code,country.name,country.headofstate,countrylanguage.language
 from
 country,countrylanguage
 where country.code = countrylanguage.countrycode and countrylanguage.isofficial=true
 and (country.gnp < country.gnpold or country.gnp < 3000)
)

select OUTERMOST_FOO.AVG from (
select avg(population) AVG,region from
(
select FOO.*,bad_headofstates.headofstate,city.name
from
(select bad_headofstates.code,country.capital,country.region,country.population from
bad_headofstates,country where bad_headofstates.code = country.code) FOO, bad_headofstates,city
where FOO.code = bad_headofstates.code and FOO.capital = city.id) OUTER_FOO
group by region ) OUTERMOST_FOO,bad_headofstates,country
where country.code = bad_headofstates.code and country.region = OUTERMOST_FOO.region
and OUTERMOST_FOO.region = 'Australia and New Zealand' or OUTERMOST_FOO.region = 'Eastern Asia'
order by OUTERMOST_FOO.region
);

select * from bad_headofstates order by region,headofstate;

drop table bad_headofstates;
