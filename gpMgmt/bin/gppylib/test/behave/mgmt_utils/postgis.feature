# Sample data is from http://workshops.opengeo.org/postgis-intro
@postgis
Feature: postgis regression

    Scenario: check PostGIS 2.0 is installed
        Then verify that file "postgis.sql" exists under "${GPHOME}/share/postgresql/contrib/postgis-2.0"
        Then verify that file "postgis_restore.pl" exists under "${GPHOME}/share/postgresql/contrib/postgis-2.0"

    Scenario: check GPDB and PostGIS version info
        When all rows from table "version()" db "opengeo" are stored in the context
        Then validate that "Greenplum Database" is in the stored rows
        When all rows from table "postgis_full_version()" db "opengeo" are stored in the context
        Then validate that "POSTGIS="2.0.3" is in the stored rows
        And validate that "GEOS" is in the stored rows

    # Basic geometries operations and functions

    Scenario: create and check various geometry types
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE geometries (name varchar, geom geometry) DISTRIBUTED BY (name)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                ('Point', 'POINT(0 0)'),
                ('Linestring', 'LINESTRING(0 0, 1 1, 2 1, 2 2)'),
                ('Polygon', 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
                ('PolygonWithHole', 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(1 1, 1 2, 2 2, 2 1, 1 1))'), 
                ('Collection', 'GEOMETRYCOLLECTION(POINT(2 0),POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))')
            """
        And all rows from table "(SELECT name, ST_AsText(geom) FROM geometries) AS foo" db "opengeo" are stored in the context
        Then validate that stored rows has "5" lines of output

        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name, ST_GeometryType(geom), ST_NDims(geom), ST_SRID(geom) FROM geometries
            """
        Then validate that "Collection|ST_GeometryCollection|2|0" "string|string|int|int" seperated by "|" is in the stored rows 
        When execute sql "SELECT ST_AsText(geom) FROM geometries WHERE name = 'Point'" in db "opengeo" and store result in the context
        Then validate that "POINT(0 0)" is in the stored rows
        When execute sql "SELECT ST_X(geom), ST_Y(geom) FROM geometries WHERE name = 'Point'" in db "opengeo" and store result in the context
        Then validate that "0|0" "int|int" seperated by "|" is in the stored rows 
        When execute sql "SELECT ST_Length(geom)::text FROM geometries WHERE name = 'Linestring'" in db "opengeo" and store result in the context
        Then validate that "3.41421356237309" is in the stored rows
        When execute sql "SELECT name, ST_Area(geom) FROM geometries WHERE name LIKE 'Polygon%'" in db "opengeo" and store result in the context
        Then validate that "Polygon|1" "string|int" seperated by "|" is in the stored rows 
        Then validate that "PolygonWithHole|99" "string|int" seperated by "|" is in the stored rows 
        When execute sql "SELECT name, ST_AsText(geom) FROM geometries WHERE name = 'Collection'" in db "opengeo" and store result in the context
        Then validate that "Collection|GEOMETRYCOLLECTION(POINT(2 0),POLYGON((0 0,1 0,1 1,0 1,0 0)))" "string|string" seperated by "|" is in the stored rows 
        When execute sql "SELECT ST_AsEWKT(ST_GeometryFromText('LINESTRING(0 0 0,1 0 0,1 1 2)'))" in db "opengeo" and store result in the context
        Then validate that "LINESTRING(0 0 0,1 0 0,1 1 2)" is in the stored rows
        When execute sql "SELECT 'SRID=4326;POINT(0 0)'::geometry::text" in db "opengeo" and store result in the context
        Then validate that "0101000020E610000000000000000000000000000000000000" is in the stored rows

    Scenario: what is the area of the 'West Village' neighborhood
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Area(geom)::text FROM nyc_neighborhoods WHERE name = 'West Village'
            """
        Then validate that "1044614" is in the stored rows

    Scenario: what is the area of Manhattan in acres
        When execute sql "SELECT Sum(ST_Area(geom))::text FROM nyc_neighborhoods WHERE boroname = 'Manhattan'" in db "opengeo" and store result in the context
        Then validate that "56517650" is in the stored rows

    Scenario: how many census blocks in New York City have a hole in them
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Count(*)::text FROM nyc_census_blocks WHERE ST_NumInteriorRings(ST_GeometryN(geom,1)) > 0
            """
        Then validate that "45" is in the stored rows

    Scenario: what is the total length of streets (in km) in New York City
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT (Sum(ST_Length(geom)) / 1000)::text FROM nyc_streets
            """
        Then validate that "10418" is in the stored rows

    Scenario: how long is 'Columbus Cir'
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Length(geom)::text FROM nyc_streets WHERE name = 'Columbus Cir'
            """
        Then validate that "308" is in the stored rows

    Scenario: how many polygons are in the 'West Village' multipolygon
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_NumGeometries(geom)::text FROM nyc_neighborhoods WHERE name = 'West Village'
            """
        Then validate that "1" is in the stored rows

    Scenario: how many polygons are in the 'West Village' multipolygon
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT type, Round(Sum(ST_Length(geom)))::int AS length FROM nyc_streets GROUP BY type ORDER BY length DESC
            """
        Then validate that following rows are in the stored rows
            | type                       |  length | 
            | residential                | 8629870 |
            | motorway                   |  403622 |
            | tertiary                   |  360395 |
            | motorway_link              |  294261 |
            | secondary                  |  276264 |

    # Spatial relationships 

    Scenario: ST_Equals should work
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name FROM nyc_subway_stations 
            WHERE ST_Equals(geom, '0101000020266900000EEBD4CF27CF2141BC17D69516315141')
            """
        Then validate that "Broad St" is in the stored rows

    Scenario: ST_Intersects should work
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name, boroname FROM nyc_neighborhoods
            WHERE ST_Intersects(geom, ST_GeomFromText('POINT(583571 4506714)',26918))
            """
        Then validate that "Manhattan" is in the stored rows

    Scenario: ST_Distance should work
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Distance(ST_GeometryFromText('POINT(0 5)'), ST_GeometryFromText('LINESTRING(-2 2, 2 2)'))::text;
            """
        Then validate that "3" is in the stored rows

    Scenario: What streets are nearby 'Broad Street' subway station
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name FROM nyc_streets
            WHERE ST_DWithin(geom, ST_GeomFromText('POINT(583571 4506714)',26918), 10);
            """
        Then validate that "Wall St" is in the stored rows
        And  validate that "Broad St" is in the stored rows
        And  validate that "Nassau St" is in the stored rows

    Scenario: What is the geometry value for street named 'Atlantic Commons'
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(geom) FROM nyc_streets WHERE name = 'Atlantic Commons';
            """
        Then validate that "MULTILINESTRING((586781" is in the stored rows

    Scenario: What neighborhood and borough is Atlantic Commons in
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name, boroname FROM nyc_neighborhoods
            WHERE ST_Intersects(geom, ST_GeomFromText('LINESTRING(586782 4504202,586864 4504216)', 26918))
            """
        Then validate that "Fort Green|Brooklyn" "string|string" seperated by "|" is in the stored rows 

    Scenario: What streets does Atlantic Commons join with
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT name FROM nyc_streets
            WHERE ST_DWithin(geom, ST_GeomFromText('LINESTRING(586782 4504202,586864 4504216)', 26918), 1)
            """
        Then validate that "S Oxford St" is in the stored rows
        And  validate that "Cumberland St" is in the stored rows
    
     Scenario: How many people live on (within 50 meters of) Atlantic commons
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(popn_total)::text FROM nyc_census_blocks
            WHERE ST_DWithin(geom, ST_GeomFromText('LINESTRING(586782 4504202,586864 4504216)', 26918), 50)
            """
        Then validate that "1186" is in the stored rows
   

    # Spatial joins

    Scenario: What neighborhood is the 'Broad St' station in
         When execute following sql in db "opengeo" and store result in the context
            """
            SELECT subways.name AS subway_name, neighborhoods.name AS neighborhood_name, neighborhoods.boroname AS borough
            FROM   nyc_neighborhoods AS neighborhoods
            JOIN   nyc_subway_stations AS subways ON ST_Contains(neighborhoods.geom, subways.geom)
            WHERE  subways.name = 'Broad St'
            """
        Then validate that "Broad St|Financial District|Manhattan" "string|string|string" seperated by "|" is in the stored rows 

    Scenario: What is the population and racial make-up of the neighborhoods of Manhattan
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT neighborhoods.name AS neighborhood_name, Sum(census.popn_total) AS population, 
                   Round(100.0 * Sum(census.popn_white) / Sum(census.popn_total),1) AS white_pct,
                   Round(100.0 * Sum(census.popn_black) / Sum(census.popn_total),1) AS black_pct
            FROM   nyc_neighborhoods AS neighborhoods
            JOIN   nyc_census_blocks AS census ON ST_Intersects(neighborhoods.geom, census.geom)
            WHERE  neighborhoods.boroname = 'Manhattan'
            GROUP  BY neighborhoods.name ORDER BY white_pct DESC
            """
        Then validate that following rows are in the stored rows
            | neighborhood_name   | population | white_pct | black_pct |
            | Carnegie Hill       |      19909 |      91.6 |       1.5 |
            | North Sutton Area   |      21413 |      90.3 |       1.2 |
            | West Village        |      27141 |      88.1 |       2.7 |
            | Upper East Side     |     201301 |      87.8 |       2.5 |

    Scenario: What is the total population and total racial made-up in nyc
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(100.0 * Sum(popn_white) / Sum(popn_total), 1) AS white_pct,
                   Round(100.0 * Sum(popn_black) / Sum(popn_total), 1) AS black_pct, 
                   Sum(popn_total) AS popn_total
            FROM nyc_census_blocks
            """
        Then validate that following rows are in the stored rows
            | white_pct | black_pct | popn_total |
            |      44.7 |      26.6 |    8008278 |

    Scenario: What is racial make-up along the A-train
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(100.0 * Sum(popn_white) / Sum(popn_total), 1) AS white_pct,
                   Round(100.0 * Sum(popn_black) / Sum(popn_total), 1) AS black_pct, 
                   Sum(popn_total) AS popn_total
            FROM   nyc_census_blocks AS census
            JOIN   nyc_subway_stations AS subways ON ST_DWithin(census.geom, subways.geom, 200)
            WHERE  strpos(subways.routes,'A') > 0
            """
        Then validate that following rows are in the stored rows
            | white_pct | black_pct | popn_total |
            |      42.1 |      23.1 |     185259 |

    Scenario: What subway station is in 'Little Italy', What subway route is it on
        When execute following sql in db "opengeo" and store result in the context
            """
                SELECT s.name, s.routes FROM nyc_subway_stations AS s
                JOIN nyc_neighborhoods AS n ON ST_Contains(n.geom, s.geom)
                WHERE n.name = 'Little Italy'
            """
        Then validate that following rows are in the stored rows
            |   name    | routes |
            | Spring St | 6      |

     Scenario: What are all the neighborhoods served by the 6-train
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT DISTINCT n.name, n.boroname FROM nyc_subway_stations AS s
            JOIN nyc_neighborhoods AS n ON ST_Contains(n.geom, s.geom)
            WHERE strpos(s.routes,'6') > 0
            """
        Then validate that following rows are in the stored rows
            |        name        | boroname  | 
            | Chinatown          | Manhattan |
            | East Harlem        | Manhattan |
            | Financial District | Manhattan |
            | Gramercy           | Manhattan |
            | Greenwich Village  | Manhattan |
            | Hunts Point        | The Bronx |
            | Little Italy       | Manhattan |

      Scenario: How many people had to be evacuated after 9/11 as 'Battery Park' was off limits for several days
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(popn_total) FROM nyc_neighborhoods AS n 
            JOIN nyc_census_blocks AS c ON ST_Intersects(n.geom, c.geom)
            WHERE n.name = 'Battery Park'
            """
        Then validate that following rows are in the stored rows
            | sum  |
            | 9928 |

      # Spatial index

      Scenario: What is the population of 'West Village' using bbox
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(popn_total) FROM nyc_neighborhoods neighborhoods 
            JOIN nyc_census_blocks blocks ON neighborhoods.geom && blocks.geom
            WHERE neighborhoods.name = 'West Village'
            """
        Then validate that following rows are in the stored rows
            | sum   |
            | 50325 |

      Scenario: What is the population of 'West Village' using ST_Intersects
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(popn_total) FROM nyc_neighborhoods neighborhoods 
            JOIN nyc_census_blocks blocks ON ST_Intersects(neighborhoods.geom, blocks.geom)
            WHERE neighborhoods.name = 'West Village'
            """
        Then validate that following rows are in the stored rows
            | sum   |
            | 27141 |


               
    # Projecting/Transforming
    Scenario: Convert the coordinates of the ‘Broad St’ subway station into geographics
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_Transform(geom,4326)) FROM nyc_subway_stations WHERE name = 'Broad St';
            """
        Then validate that following rows are in the stored rows
            |                  st_astext                |
            | POINT(-74.0106714688735 40.7071048155841) |

    Scenario: What is the length of all streets in New York, as measured in UTM 18
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(ST_Length(geom))::integer FROM nyc_streets
            """
        Then validate that following rows are in the stored rows
            |    sum   | 
            | 10418905 |

    Scenario: What is the length of all streets in New York, as measured in SRID 2831
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Sum(ST_Length(ST_Transform(geom,2831)))::integer FROM nyc_streets
            """
        Then validate that following rows are in the stored rows
            |    sum    |
            | 10421994  |

    Scenario: What is the KML representation of the point at ‘Broad St’ subway station
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsKML(geom) FROM nyc_subway_stations WHERE name = 'Broad St'
            """
        Then validate that following rows are in the stored rows
            |                                      st_askml                                    | 
            | <Point><coordinates>-74.010671468873468,40.707104815584088</coordinates></Point> |


    # Geography
    Scenario: What is the distance between Los Angeles and Paris
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(ST_Distance(
                    ST_GeometryFromText('POINT(-118.4079 33.9434)', 4326), -- Los Angeles (LAX)
                    ST_GeometryFromText('POINT(2.5559 49.0083)', 4326)     -- Paris (CDG)
                  )::numeric, 1)
            """
        Then validate that following rows are in the stored rows
            | round |
            | 121.9 |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(ST_Distance(
                    ST_GeographyFromText('POINT(-118.4079 33.9434)'), -- Los Angeles (LAX)
                    ST_GeographyFromText('POINT(2.5559 49.0083)')     -- Paris (CDG)
                  )::numeric, 1)
            """
        Then validate that following rows are in the stored rows
            | round     |
            | 9124665.3 |


    Scenario: Geography type basic test
        When below sql is executed in "opengeo" db 
            """
            CREATE TABLE airports (code VARCHAR(3), geog GEOGRAPHY)
            """
        And  below sql is executed in "opengeo" db
            """
            INSERT INTO airports VALUES
                ('LAX', 'POINT(-118.4079 33.9434)'),
                ('CDG', 'POINT(2.5559 49.0083)'),
                ('REK', 'POINT(-21.8628 64.1286)')
            """
        And execute following sql in db "opengeo" and store result in the context
            """
            select f_table_name, f_geography_column, srid, type from geography_columns
            """
        Then validate that following rows are in the stored rows
            | f_table_name | f_geography_column | srid |   type   |
            | airports     | geog               |    0 | Geometry |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT code, ST_X(geog::geometry) AS longitude FROM airports
            """
        Then validate that following rows are in the stored rows
            | code | longitude |
            | LAX  | -118.4079 |
            | CDG  |    2.5559 |
            | REK  |  -21.8628 |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT code, Round(ST_Distance(geog, 'POINT(-118.4079 33.9434)'))::integer AS distance FROM airports
            """
        Then validate that following rows are in the stored rows
             | code | distance  |
             | LAX  |        0  |
             | CDG  |  9124665  |
             | REK  |  6969661  |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT code, ST_DWithin(geog, 'POINT(-118.4079 33.9434)', 100) AS within FROM airports;
            """
        Then validate that following rows are in the stored rows
            | code | within |
            | CDG  | False  |
            | REK  | False  |
            | LAX  | True   |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT substr(ST_AsText(ST_Buffer(geog, 100)), 0, 20) AS buffer_snipet FROM airports
            """
        Then validate that following rows are in the stored rows
            |     buffer_snipet    |
            |  POLYGON((2.55726738 |
            |  POLYGON((-21.860746 |
            |  POLYGON((-118.40681 |

    # Geometry constructing functions
    Scenario: How many points for liberty island with 500m and -50m buffer zone
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE liberty_island_zone AS SELECT ST_Buffer(geom,500) AS geom
            FROM nyc_census_blocks WHERE blkid = '360610001009000'
            """
        And execute following sql in db "opengeo" and store result in the context
            """
            select st_npoints(geom) from liberty_island_zone
            """
        Then validate that following rows are in the stored rows
            | st_npoints |
            |         43 |
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE liberty_island_zone2 AS SELECT ST_Buffer(geom, -50) AS geom
            FROM nyc_census_blocks WHERE blkid = '360610001009000' 
            """
        And execute following sql in db "opengeo" and store result in the context
            """
            select st_npoints(geom) from liberty_island_zone2
            """
        Then validate that following rows are in the stored rows
            | st_npoints |
            |         11 | 
    
    Scenario: How many points for intersection of two buffer
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_NPoints(ST_AsText(ST_Intersection(ST_Buffer('POINT(0 0)', 2), ST_Buffer('POINT(3 0)', 2))))
            """
        Then validate that following rows are in the stored rows
            | st_npoints |
            |         17 | 
 
    Scenario: How many points for union of two buffer
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_NPoints(ST_AsText(ST_Union(ST_Buffer('POINT(0 0)', 2), ST_Buffer('POINT(3 0)', 2))))
            """
        Then validate that following rows are in the stored rows
            | st_npoints |
            |         53 | 
        
    Scenario: merging census blocks using union
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE nyc_census_counties AS
            SELECT ST_Union(geom) AS geom, SubStr(blkid,1,5) AS countyid FROM nyc_census_blocks GROUP BY countyid
            """
        And execute following sql in db "opengeo" and store result in the context
            """
            SELECT SubStr(blkid,1,5) AS countyid, Round(Sum(ST_Area(geom)))::integer AS area
            FROM nyc_census_blocks
            GROUP BY countyid ORDER BY countyid
            """
        Then validate that following rows are in the stored rows
             | countyid |   area    |
             | 36005    | 109807440 |
             | 36047    | 184906576 |
             | 36061    |  58973522 |
             | 36081    | 283764734 |
             | 36085    | 149806078 |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT countyid, Round(ST_Area(geom))::integer AS area
            FROM nyc_census_counties ORDER BY countyid
            """
        Then validate that following rows are in the stored rows
             | countyid |   area    |
             | 36005    | 109807440 |
             | 36047    | 184906576 |
             | 36061    |  58973522 |
             | 36081    | 283764734 |
             | 36085    | 149806078 |

    # More spatial joins
    Scenario: nyc census data with tract geometry: List top 10 New York neighborhoods ordered by the proportion of people who have graduate degrees
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE nyc_census_tract_geoms AS
            SELECT ST_Union(geom) AS geom, SubStr(blkid,1,11) AS tractid FROM nyc_census_blocks GROUP BY tractid
            """
        And  below sql is executed in "opengeo" db
            """
            CREATE TABLE nyc_census_tracts AS
            SELECT g.geom, a.* FROM nyc_census_tract_geoms g JOIN nyc_census_sociodata a ON g.tractid = a.tractid;
            """
        And  execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(100.0 * Sum(t.edu_graduate_dipl) / Sum(t.edu_total), 1) AS graduate_pct, n.name, n.boroname
            FROM nyc_neighborhoods n JOIN nyc_census_tracts t ON ST_Intersects(n.geom, t.geom) 
            WHERE t.edu_total > 0 GROUP BY n.name, n.boroname ORDER BY graduate_pct DESC LIMIT 10;
            """
        Then validate that following rows are in the stored rows
             | graduate_pct |       name        | boroname  |
             |         40.4 | Carnegie Hill     | Manhattan |
             |         40.2 | Flatbush          | Brooklyn  |
             |         34.8 | Battery Park      | Manhattan |
             |         33.9 | North Sutton Area | Manhattan |


    Scenario: avoiding double count issues for ST_Intersects
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(100.0 * Sum(t.edu_graduate_dipl) / Sum(t.edu_total), 1) AS graduate_pct, n.name, n.boroname
            FROM nyc_neighborhoods n JOIN nyc_census_tracts t ON ST_Contains(n.geom, ST_Centroid(t.geom))
            WHERE t.edu_total > 0 GROUP BY n.name, n.boroname ORDER BY graduate_pct DESC LIMIT 10;
            """
        Then validate that following rows are in the stored rows
            | graduate_pct |       name        | boroname   |
            |         49.2 | Carnegie Hill     | Manhattan  |
            |         39.5 | Battery Park      | Manhattan  |
            |         34.3 | Upper East Side   | Manhattan  |
            |         33.6 | Upper West Side   | Manhattan  |
            |         32.5 | Greenwich Village | Manhattan  |
            |         32.2 | Tribeca           | Manhattan  |
            |         31.3 | North Sutton Area | Manhattan  |
            |         30.8 | West Village      | Manhattan  |
            |         30.1 | Downtown          | Brooklyn   |
            |         28.4 | Cobble Hill       | Brooklyn   |

    # 3D
    Scenario: 3D distance
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(ST_3DDistance(
              'POLYHEDRALSURFACE Z (
                  ((0 0 0, 0 1 0, 1 1 0, 1 0 0, 0 0 0)),
                  ((0 0 0, 0 1 0, 0 1 1, 0 0 1, 0 0 0)),
                  ((0 0 0, 1 0 0, 1 0 1, 0 0 1, 0 0 0)),
                  ((1 1 1, 1 0 1, 0 0 1, 0 1 1, 1 1 1)),
                  ((1 1 1, 1 0 1, 1 0 0, 1 1 0, 1 1 1)),
                  ((1 1 1, 1 1 0, 0 1 0, 0 1 1, 1 1 1))
               )'::geometry,
               'POINT Z (2 2 2)'::geometry
            )::numeric, 5)
            """
        Then validate that following rows are in the stored rows
            | st_3ddistance    |
            | 1.73205 |

        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES ('3DDistancePointA', 'POINT Z (1 1 1)'),
                   ('3DDistancePointB', 'POINT Z (2 2 2)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Round(ST_3DDistance(foo.geom, bar.geom)::numeric, 5) FROM (SELECT geom FROM geometries WHERE name='3DDistancePointB') AS foo, (SELECT geom FROM geometries WHERE name='3DDistancePointA') AS bar
            """
        Then validate that following rows are in the stored rows
            | st_3ddistance    |
            | 1.73205 |

    Scenario: N-D index operator
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT gid, name FROM nyc_streets
            WHERE geom &&& ST_SetSRID('LINESTRING(586785 4492901,587561 4493037)',26918)
            """
        Then validate that following rows are in the stored rows
            | gid  |       name     |
            |    1 | Shore Pky S    |
            |   57 | W 5th St       |
            |   69 | Colby Ct       |


    # Index-based KNN (K nearest neighbours)
    Scenario: Closest streets to Broad Street station using KNN
        When execute following sql in db "opengeo" and store result in the context
            """
            WITH closest_candidates AS (
              SELECT
                streets.gid,
                streets.name,
                streets.geom
              FROM
                nyc_streets streets
              ORDER BY
                streets.geom <->
                'SRID=26918;POINT(583571.905921312 4506714.34119218)'::geometry
              LIMIT 100
            )
            SELECT gid, name
            FROM closest_candidates
            ORDER BY
              ST_Distance(
                geom,
                'SRID=26918;POINT(583571.905921312 4506714.34119218)'::geometry
                )
            LIMIT 1;
            """
        Then validate that following rows are in the stored rows
            | gid   |  name   |
            | 17385 | Wall St |

    Scenario: 10 nearest stations to Broad St station
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT gid, name
            FROM nyc_subway_stations
            ORDER BY geom <-> 'SRID=26918;POINT(583571.905921312
            4506714.34119218)'::geometry
            LIMIT 10;
            """
        Then validate that following rows are in the stored rows
            | gid | name        |
            | 332 | Broad St    |
            | 373 | Wall St     |
            | 374 | Wall St     |
            | 366 | Rector St   |
            |   2 | Rector St   |
            |   1 | Cortlandt St|

    Scenario: ST_AsGML should work with in-memory lookup of spatial_ref_sys
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT substr(ST_AsX3D(geom), 1400, 100) FROM nyc_census_blocks
            WHERE blkid = '360050001009000';
            """
        Then validate that following rows are in the stored rows
            | substr                                                                                               |
            | 370 371 372 373 374 375 376 377'>595206.589571490767412 4516105.184799655340612 595214.8743579722940 |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT substr(ST_AsGML(geom), 0, 200) FROM nyc_census_blocks WHERE blkid = '360050001009000'
            """
        Then validate that following rows are in the stored rows
            | substr                                                                                                | 
            | <gml:MultiPolygon srsName="EPSG:26918"><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>595206.589571490767412,4516105.184799655340612 595214.874357972294092,4516 |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsGML(geog) FROM airports;
            """
        Then validate that following rows are in the stored rows
            |                                                        st_asgml                                           |       
            | <gml:Point srsName="EPSG:4326"><gml:coordinates>2.5559,49.008299999999998</gml:coordinates></gml:Point>   |
            | <gml:Point srsName="EPSG:4326"><gml:coordinates>-21.8628,64.128600000000006</gml:coordinates></gml:Point> |
            | <gml:Point srsName="EPSG:4326"><gml:coordinates>-118.407899999999998,33.943399999999997</gml:coordinates></gml:Point> |


    Scenario: Import/Export using different format
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_GeomFromGML('<gml:LineString srsName="EPSG:4269">
                <gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates>
                </gml:LineString>'))
            """
        Then validate that following rows are in the stored rows
            |                                 st_astext                                | 
            | LINESTRING(-71.16028 42.258729,-71.160837 42.259112,-71.161143 42.25932) |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsGML(3, ST_GeomFromText('POINT(5.234234233242 6.34534534534)',4326), 5, 17)
            """
        Then validate that following rows are in the stored rows
            | st_asgml |
            | <gml:Point srsName="urn:ogc:def:crs:EPSG::4326"><gml:pos srsDimension="2">6.34535 5.23423</gml:pos></gml:Point> |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT substr(ST_AsGeoJSON(geom), 0, 200) FROM nyc_census_blocks WHERE blkid = '360050001009000'
            """
        Then validate that following rows are in the stored rows
            | substr                                                                                                 | 
            | {"type":"MultiPolygon","coordinates":[[[[595206.589571491,4516105.18479966],[595214.874357972,4516094.08370369],[595221.417038022,4516085.47920627],[595222.952499512,4516083.21821789],[595224.5418065 |

    Scenario: Linear approximation of closed curve polygon
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_postgis_linear(name text, geom geometry)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_postgis_linear VALUES
                ('curve_polygon',
                 'CURVEPOLYGON(CIRCULARSTRING(-192.63 96.09,-236.72 -84.3,-262.33
                  -256.89,-267.71 -409.94,-252.5 -533,-217.73 -617.69,-165.78
                  -658.24,-100.19 -651.89, -25.42 -599.06,53.44 -503.37,130.99
                  -371.32,201.97 -211.92,261.53 -36.04,305.62 144.35,331.23
                  316.94,336.61 469.99,321.4 593.05,286.64 677.74,234.69 718.29,169.09
                  711.94,94.32 659.11,15.47 563.42,-62.09 431.37,-133.07
                  271.98,-192.63 96.09))')
            """
        When execute sql "SELECT st_isclosed(geom) as closed from t_postgis_linear where name='curve_polygon'" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
            | closed |
            | True   |
        When execute sql "SELECT st_isclosed(st_curvetoline(geom)) as closed from t_postgis_linear where name='curve_polygon'" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
            | closed |
            | True   |

    Scenario: create a linestring dataset for latter test
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE geom_linestrings (id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO geom_linestrings VALUES
                (1, 'LINESTRING(0 0 0, 1 1 1, 2 1 2, 2 2 2)'),
                (2, 'LINESTRING(0 0 0, 1 1 2, 2 1 2, 2 2 2)'),
                (3, 'LINESTRING(0 0 0, 1 1 3, 2 1 2, 2 2 2)'),
                (4, 'LINESTRING(0 0 0, 1 1 4, 2 1 2, 2 2 2)'),
                (5, 'LINESTRING(0 0 0, 1 1 5, 2 1 2, 2 2 2)'),
                (6, 'LINESTRING(0 0 0, 1 1 6, 2 1 2, 2 2 2)'),
                (7, 'LINESTRING(0 0 0, 1 1 7, 2 1 2, 2 2 2)'),
                (8, 'LINESTRING(0 0 0, 1 1 8, 2 1 2, 2 2 2)'),
                (9, 'LINESTRING(0 0 0, 1 1 9, 2 1 2, 2 2 2)'),
                (10, 'LINESTRING(0 0 0, 1 1 10, 2 1 2, 2 2 2)'),
                (11, 'LINESTRING(0 0 0, 1 1 11, 2 1 2, 2 2 2)'),
                (12, 'LINESTRING(0 0 0, 1 1 12, 2 1 2, 2 2 2)'),
                (13, 'LINESTRING(0 0 0, 1 1 13, 2 1 2, 2 2 2)'),
                (14, 'LINESTRING(0 0 0, 1 1 14, 2 1 2, 2 2 2)'),
                (15, 'LINESTRING(0 0 0, 1 1 15, 2 1 2, 2 2 2)'),
                (16, 'LINESTRING(0 0 0, 1 1 16, 2 1 2, 2 2 2)'),
                (17, 'LINESTRING(0 0 0, 1 1 17, 2 1 2, 2 2 2)'),
                (18, 'LINESTRING(0 0 0, 1 1 18, 2 1 2, 2 2 2)'),
                (19, 'LINESTRING(0 0 0, 1 1 19, 2 1 2, 2 2 2)'),
                (20, 'LINESTRING(0 0 0, 1 1 20, 2 1 2, 2 2 2)'),
                (21, 'LINESTRING(0 0 0, 1 1 21, 2 1 2, 2 2 2)'),
                (22, 'LINESTRING(0 0 0, 1 1 22, 2 1 2, 2 2 2)'),
                (23, 'LINESTRING(0 0 0, 1 1 23, 2 1 2, 2 2 2)'),
                (24, 'LINESTRING(0 0 0, 1 1 24, 2 1 2, 2 2 2)'),
                (25, 'LINESTRING(0 0 0, 1 1 25, 2 1 2, 2 2 2)'),
                (26, 'LINESTRING(0 0 0, 1 1 26, 2 1 2, 2 2 2)')
            """

    Scenario: create a 2d-linestring dataset for latter test
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE geom_linestrings_2d (id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO geom_linestrings_2d VALUES
                (1, 'LINESTRING(0 0, 1 1, 2 1, 2 2)'),
                (2, 'LINESTRING(0 0, 1 2, 2 1, 2 2)'),
                (3, 'LINESTRING(0 0, 1 3, 2 1, 2 2)'),
                (4, 'LINESTRING(0 0, 1 4, 2 1, 2 2)'),
                (5, 'LINESTRING(0 0, 1 5, 2 1, 2 2)'),
                (6, 'LINESTRING(0 0, 1 6, 2 1, 2 2)'),
                (7, 'LINESTRING(0 0, 1 7, 2 1, 2 2)'),
                (8, 'LINESTRING(0 0, 1 8, 2 1, 2 2)'),
                (9, 'LINESTRING(0 0, 1 9, 2 1, 2 2)'),
                (10, 'LINESTRING(0 0, 1 10, 2 1, 2 2)'),
                (11, 'LINESTRING(0 0, 1 11, 2 1, 2 2)'),
                (12, 'LINESTRING(0 0, 1 12, 2 1, 2 2)'),
                (13, 'LINESTRING(0 0, 1 13, 2 1, 2 2)'),
                (14, 'LINESTRING(0 0, 1 14, 2 1, 2 2)'),
                (15, 'LINESTRING(0 0, 1 15, 2 1, 2 2)'),
                (16, 'LINESTRING(0 0, 1 16, 2 1, 2 2)'),
                (17, 'LINESTRING(0 0, 1 17, 2 1, 2 2)'),
                (18, 'LINESTRING(0 0, 1 18, 2 1, 2 2)'),
                (19, 'LINESTRING(0 0, 1 19, 2 1, 2 2)'),
                (20, 'LINESTRING(0 0, 1 20, 2 1, 2 2)'),
                (21, 'LINESTRING(0 0, 1 21, 2 1, 2 2)'),
                (22, 'LINESTRING(0 0, 1 22, 2 1, 2 2)'),
                (23, 'LINESTRING(0 0, 1 23, 2 1, 2 2)'),
                (24, 'LINESTRING(0 0, 1 24, 2 1, 2 2)'),
                (25, 'LINESTRING(0 0, 1 25, 2 1, 2 2)'),
                (26, 'LINESTRING(0 0, 1 26, 2 1, 2 2)')
            """

    Scenario: Applies a 3d affine transformation to the geometry to do things like translate, rotate, scale in one step
        When execute sql "SELECT ST_AsEWKT(ST_Affine(the_geom, cos(pi()), -sin(pi()), 0, sin(pi()), cos(pi()), 0,  0, 0, 1,  0, 0, 0)) As using_affine, ST_AsEWKT(ST_RotateZ(the_geom, pi())) As using_rotatez FROM (SELECT ST_GeomFromEWKT('LINESTRING(1 2 3, 1 4 3)') As the_geom) As foo" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | using_affine | using_rotatez |
             | LINESTRING(-1 -2 3,-1 -4 3) | LINESTRING(-1 -2 3,-1 -4 3) |

    Scenario: Returns Z minima/maxima of a bounding box 2d or 3d or a geometry
        When execute sql "SELECT ST_ZMin('BOX3D(1 2 3, 4 5 6)')" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_zmin |
             | 3.0 |
        When execute sql "SELECT ST_ZMax('BOX3D(1 2 3, 4 5 6)')" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_zmax |
             | 6.0 |

    Scenario: Returns bounding box expanded in all directions from the bounding box of the input geometry
        When execute sql "SELECT CAST(ST_Expand(foo.geom, 10) As box2d) FROM (SELECT geom FROM geom_linestrings WHERE id=1) AS foo" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_expand |
             | BOX(-10 -10,12 12) |
        When execute sql "SELECT ST_Expand(CAST('BOX3D(778783 2951741 1,794875 2970042.61545891 10)' As box3d),10)" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_expand |
             | BOX3D(778773 2951731 -9,794885 2970052.61545891 20) |
        When execute sql " SELECT ST_AsEWKT(ST_Expand(ST_GeomFromEWKT('SRID=2163;POINT(2312980 110676)'),10))" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | SRID=2163;POLYGON((2312970 110666,2312970 110686,2312990 110686,2312990 110666,2312970 110666)) |

    Scenario: Return a BBox which covers the two input polygon
        When execute sql "SELECT count(ST_Combine_BBox(Box2D(foo.geom), bar.geom)) FROM (SELECT geom FROM geom_linestrings_2d) AS foo, (SELECT geom FROM geom_linestrings_2d) AS bar" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | count |
             | 676 |

    Scenario: Return a BBox which covers all of the geometry of the specified column
       When execute sql "SELECT ST_find_extent('geom_linestrings_2d', 'geom')" in db "opengeo" and store result in the context
        Then validate that following rows are in the stored rows
             | st_find_extent |
             | BOX(0 0,2 26) |

    Scenario: Calculates the 2D or 3D length of a linestring/multilinestring on an ellipsoid
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(ST_Length_Spheroid(foo.the_geom, bar.sph_m )) As tot_len,
                   round(ST_Length_Spheroid(ST_GeometryN(foo.the_geom,1), bar.sph_m)) As len_line1,
                   round(ST_Length_Spheroid(ST_GeometryN(foo.the_geom,2), bar.sph_m)) As len_line2
                   FROM (SELECT geom AS the_geom FROM nyc_streets WHERE gid=3) AS foo,
                   (SELECT CAST('SPHEROID["GRS_1980",6378137,298.257222101]' AS spheroid) AS sph_m) AS bar
            """
        Then validate that following rows are in the stored rows
             | tot_len | len_line1 | len_line2 |
             | 162112223.0 | 162112223.0 | None |

    Scenario: Calculates the 2D length of a linestring/multilinestring on an ellipsoid
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(ST_Length2D_Spheroid(foo.the_geom, bar.sph_m )) As tot_len,
                   round(ST_Length2D_Spheroid(ST_GeometryN(foo.the_geom,1), bar.sph_m)) As len_line1,
                   round(ST_Length2D_Spheroid(ST_GeometryN(foo.the_geom,2), bar.sph_m)) As len_line2
                   FROM (SELECT geom AS the_geom FROM nyc_streets WHERE gid=3) AS foo,
                   (SELECT CAST('SPHEROID["GRS_1980",6378137,298.257222101]' As spheroid) As sph_m) as bar
            """
        Then validate that following rows are in the stored rows
             | tot_len | len_line1 | len_line2 |
             | 162112223.0 | 162112223.0 | None |

    Scenario: Is the point geometry inside circle defined by center_x, center_y , radius
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Point_Inside_Circle(the_geom.geom, 0.5, 2, 3) FROM (SELECT geom FROM nyc_subway_stations WHERE gid=3) AS the_geom
            """
        Then validate that following rows are in the stored rows
             | st_point_inside_circle |
             | False |

    Scenario: Forces the geometries into XYZ mode
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_Force_3D(geom)) FROM geometries WHERE name='PolygonWithHole'
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | POLYGON((0 0 0,10 0 0,10 10 0,0 10 0,0 0 0),(1 1 0,1 2 0,2 2 0,2 1 0,1 1 0)) |

    Scenario: Given a geometry collection, returns the "simplest" representation of the contents
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_CollectionHomogenize(geom)) FROM geometries WHERE name='Collection'
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | GEOMETRYCOLLECTION(POINT(2 0),POLYGON((0 0,1 0,1 1,0 1,0 0))) |

    Scenario: Returns the geometry with vertex order reversed
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(geom) as line, ST_AsText(ST_Reverse(geom)) As reverseline
                   FROM geom_linestrings_2d WHERE id=1
            """
        Then validate that following rows are in the stored rows
             | line | reverseline |
             | LINESTRING(0 0,1 1,2 1,2 2) | LINESTRING(2 2,2 1,1 1,0 0) |

    Scenario: Forces the orientation of the vertices in a polygon to follow the Right-Hand-Rule
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_ForceRHR(geom)) FROM geometries WHERE name='PolygonWithHole'
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,2 1,2 2,1 2,1 1)) |

    Scenario: Returns a Geometry in HEXEWKB format (as text) using either little-endian (NDR) or big-endian (XDR) encoding
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsHEXEWKB(geom) FROM geometries WHERE name='PolygonWithHole'
            """
        Then validate that following rows are in the stored rows
             | st_ashexewkb |
             | 01030000000200000005000000000000000000000000000000000000000000000000002440000000000000000000000000000024400000000000002440000000000000000000000000000024400000000000000000000000000000000005000000000000000000F03F000000000000F03F000000000000F03F0000000000000040000000000000004000000000000000400000000000000040000000000000F03F000000000000F03F000000000000F03F |

    Scenario: Return the Degrees, Minutes, Seconds representation of the given point
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES ('LatlonPoint', 'POINT(-302.2342342 -792.32498)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT (ST_AsLatLonText(geom, 'D degrees, M minutes, S seconds to the C')) FROM geometries WHERE name='LatlonPoint'
            """
        Then validate that following rows are in the stored rows
             | st_aslatlontext |
             | 72 degrees, 19 minutes, 30 seconds to the S 57 degrees, 45 minutes, 57 seconds to the E |

    Scenario: Creates a LineString from a MultiPoint geometry
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                ('MultiPoint', 'MULTIPOINT(1 2 3, 4 5 6, 7 8 9)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_LineFromMultiPoint(geom)) FROM geometries WHERE name='MultiPoint'
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | LINESTRING(1 2 3,4 5 6,7 8 9) |

    Scenario: Returns a set of geometry_dump rows, representing the exterior and interior rings of a polygon
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText((ST_DumpRings(geom)).geom) AS the_geom FROM geometries WHERE name='PolygonWithHole'
            """
        Then validate that following rows are in the stored rows
             | the_geom |
             | POLYGON((0 0,10 0,10 10,0 10,0 0)) |
             | POLYGON((1 1,1 2,2 2,2 1,1 1)) |

    Scenario: Return a derived geometry collection value with elements that match the specified range of measures inclusively
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                   ('MultiLinestring', 'MULTILINESTRINGM((1 2 3, 3 4 2, 9 4 3), (1 2 3, 5 4 5))')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_Locate_Between_Measures(geom, 1.5, 3)) FROM geometries WHERE name='MultiLinestring'
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | GEOMETRYCOLLECTIONM(LINESTRINGM(1 2 3,3 4 2,9 4 3),POINTM(1 2 3)) |

    Scenario: Return a derived geometry with measure elements linearly interpolated between the start and end points
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                   ('MultiLineStringAdd', 'MULTILINESTRINGM((1 0 4, 2 0 4, 4 0 4),(1 0 4, 2 0 4, 4 0 4))')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_AddMeasure(geom,10,70)) AS ewelev FROM geometries WHERE name='MultiLineStringAdd'
            """
        Then validate that following rows are in the stored rows
             | ewelev |
             | MULTILINESTRINGM((1 0 10,2 0 20,4 0 40),(1 0 40,2 0 50,4 0 70)) |

    Scenario: Returns a "simplified" version of the given geometry using the Douglas-Peucker algorithm
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Npoints(the_geom) As np_before, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,0.1)) As np01_notbadcircle, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,0.5)) As np05_notquitecircle, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,1)) As np1_octagon, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,10)) As np10_square, ST_NPoints(ST_SimplifyPreserveTopology(the_geom,100)) As np100_stillsquare
            FROM (SELECT ST_Buffer(geom,10,12) AS the_geom FROM geometries WHERE name='Point') As foo
            """
        Then validate that following rows are in the stored rows
             | np_before | np01_notbadcircle | np05_notquitecircle | np1_octagon | np10_square | np100_stillsquare |
             |         49|                 33|                   17|            9|            5|                  5|

    Scenario: Returns a geometry that represents the portions of A and B that do not intersect
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT count(ST_SymmetricDifference(foo.geom, bar.geom)) FROM (SELECT geom FROM geom_linestrings_2d) AS foo, (SELECT geom FROM geom_linestrings_2d) As bar
            """
        Then validate that following rows are in the stored rows
             | count |
             | 676 |

    Scenario: ST_CleanGeometry, a wrapper for ST_MakeValid
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                   ('CleanGeometry', 'POLYGON((-1 -1, -1 0, 1 0, 1 1, 0 1, 0 -1, -1 -1))')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_CleanGeometry(geom)) FROM geometries WHERE name='CleanGeometry'
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | MULTIPOLYGON(((-1 -1,-1 0,0 0,0 -1,-1 -1)),((0 0,0 1,1 1,1 0,0 0))) |

    Scenario: Takes as input GML representation of geometry and outputs a PostGIS geometry object
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_GMLToSQL('
                        <gml:LineString srsName="EPSG:4269">
                            <gml:coordinates>
                                -71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932
                            </gml:coordinates>
                        </gml:LineString>'))
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | LINESTRING(-71.16028 42.258729,-71.160837 42.259112,-71.161143 42.25932) |

    Scenario: Return the number of interior rings of the first polygon in the geometry
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_NumInteriorRing(geom) AS num FROM geometries WHERE name='PolygonWithHole'
            """
        Then validate that following rows are in the stored rows
             | num |
             | 1 |

    Scenario: Makes a point Geometry from WKT with the given SRID
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_pt_from_txt (id int, geom_txt text) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_pt_from_txt VALUES
                (1, 'POINT(-71.064544 42.28787)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_PointFromText(geom_txt, 4326) FROM t_pt_from_txt WHERE id=1
            """
        Then validate that following rows are in the stored rows
             | t_pointfromtext |
             | 0101000020E6100000CB49287D21C451C0F0BF95ECD8244540 |

    Scenario: Creates a geometry instance from a Well-Known Binary geometry representation (WKB) and optional SRID
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(
                   ST_GeomFromWKB(E'\\001\\002\\000\\000\\000\\002\\000\\000\\000\\037\\205\\353Q\\270~\\\\\\300\\323Mb\\020X\\231C@\\020X9\\264\\310~\\\\\\300)\\\\\\217\\302\\365\\230C@',4326))
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | SRID=4326;LINESTRING(-113.98 39.198,-113.981 39.195) |

    Scenario: Returns the 2-dimensional point on g1 that is closest to g2
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_ClosestPoint(pt.geom,line.geom)) AS cp_pt_line, ST_AsText(ST_ClosestPoint(line.geom,pt.geom)) As cp_line_pt
            FROM (SELECT geom FROM geometries WHERE name='Point') As pt, (SELECT geom FROM geometries WHERE name='Linestring') As line
            """
        Then validate that following rows are in the stored rows
             | cp_pt_line | cp_line_pt |
             | POINT(0 0) | POINT(0 0) |

    Scenario: Returns a version of the given geometry with X and Y axis flipped
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_FlipCoordinates(GeomFromEWKT(geom))) FROM geometries WHERE name='Point'
            """
        Then validate that following rows are in the stored rows
             | st_asewkt |
             | POINT(0 0) |

    Scenario: Creates a geography instance from a Well-Known Binary geometry representation (WKB)
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(
                   ST_GeogFromWKB(E'\\001\\002\\000\\000\\000\\002\\000\\000\\000\\037\\205\\353Q\\270~\\\\\\300\\323Mb\\020X\\231C@\\020X9\\264\\310~\\\\\\300)\\\\\\217\\302\\365\\230C@'))
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | LINESTRING(-113.98 39.198,-113.981 39.195) |

    Scenario: Returns linear distance in meters between two lon/lat points
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(CAST(ST_Distance_Sphere(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)',4326)) As numeric)) As dist_meters,
                   round(CAST(ST_Distance(ST_Transform(ST_Centroid(the_geom),32611), ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric)) As dist_utm11_meters,
                   round(CAST(ST_Distance(ST_Centroid(the_geom), ST_GeomFromText('POINT(-118 38)', 4326)) As numeric)) As dist_degrees,
                   round(CAST(ST_Distance(ST_Transform(the_geom,32611), ST_Transform(ST_GeomFromText('POINT(-118 38)', 4326),32611)) As numeric)) As min_dist_line_point_meters
            FROM
                   (SELECT ST_GeomFromText('LINESTRING(-118.584 38.374,-118.583 38.5)', 4326) As the_geom) as foo
            """
        Then validate that following rows are in the stored rows
             | dist_meters | dist_utm11_meters | dist_degrees | min_dist_line_point_meters |
             |       70425 |             70438 |            1 |                      65871 |

    Scenario: Returns the 3-dimensional cartesian maximum distance between two geometries in projected units
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(ST_3DMaxDistance(
                           ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
                           ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
                        )) As dist_3d,
                   round(ST_MaxDistance(
                           ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 10000)'),2163),
                           ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163)
                        )) As dist_2d
            """
        Then validate that following rows are in the stored rows
             | dist_3d | dist_2d |
             | 24384.0 | 22248.0 |

    Scenario: For 3d (z) geometry type Returns true if two geometries 3d distance is within number of units
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_3DDWithin(
                     ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
                     ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
                     126.8) As within_dist_3d,
                   ST_DWithin(
                     ST_Transform(ST_GeomFromEWKT('SRID=4326;POINT(-72.1235 42.3521 4)'),2163),
                     ST_Transform(ST_GeomFromEWKT('SRID=4326;LINESTRING(-72.1260 42.45 15, -72.123 42.1546 20)'),2163),
                     126.8) As within_dist_2d
            """
        Then validate that following rows are in the stored rows
             | within_dist_3d | within_dist_2d |
             | False | True |

    Scenario: Returns true if all of the 3D geometries are within the specified distance of one another
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES('3dWithinPoint', 'POINT(1 1 2)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT
                ST_3DDFullyWithin(foo.geom_a, bar.geom_b, 10) as D3DFullyWithin10,
                ST_3DDWithin(foo.geom_a, bar.geom_b, 10) as D3DWithin10,
                ST_DFullyWithin(foo.geom_a, bar.geom_b, 20) as D2DFullyWithin20,
                ST_3DDFullyWithin(foo.geom_a, bar.geom_b, 20) as D3DFullyWithin20
            FROM
                (SELECT geom AS geom_a FROM geometries WHERE name='3dWithinPoint') foo,
                (SELECT geom As geom_b FROM geom_linestrings WHERE id=1) bar
            """
        Then validate that following rows are in the stored rows
             | d3dfullywithin10 | d3dwithin10 | d2dfullywithin20 | d3dfullywithin20 |
             | True | True | True | True |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT
                Round(ST_3DMaxDistance(foo.geom, bar.geom)) AS max,
                ST_ASEWKT(ST_3DShortestLine(foo.geom, bar.geom)) AS shortest_line,
                ST_ASEWKT(ST_3DClosestPoint(foo.geom, bar.geom)) AS closest_point,
                ST_ASEWKT(ST_3DLongestLine(foo.geom, bar.geom)) AS longest_line
            FROM
                (SELECT geom AS geom FROM geom_linestrings WHERE id=2) foo,
                (SELECT geom As geom FROM geom_linestrings WHERE id=1) bar
            """
        Then validate that following rows are in the stored rows
             | max | shortest_line | closest_point | longest_line |
             | 3.0 | LINESTRING(0 0 0,0 0 0) | POINT(0 0 0) | LINESTRING(0 0 0,2 2 2) |

    Scenario: Returns TRUE if the Geometries "spatially intersect" in 3d - only for points and linestrings
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_3DIntersects(pt, line), ST_Intersects(pt,line)
            FROM (SELECT 'POINT(0 0 2)'::geometry As pt, geom As line FROM geom_linestrings WHERE id=1) As foo
            """
        Then validate that following rows are in the stored rows
             | st_3dintersects | st_intersects |
             | False | True |

    Scenario: Return the coordinate dimension of the ST_Geometry value
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_CoordDim(geom) FROM geom_linestrings WHERE id=1
            """
        Then validate that following rows are in the stored rows
             | st_coorddim |
             | 3 |

    Scenario: Return a derived geometry (collection) value with elements that intersect the specified range of elevations inclusively
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsEWKT(ST_LocateBetweenElevations(geom,2,4)) AS ewelev FROM geom_linestrings WHERE id=1
            """
        Then validate that following rows are in the stored rows
             | ewelev |
             | MULTILINESTRING((2 1 2,2 2 2)) |

    Scenario: Return the value of the measure dimension of a geometry at the point closed to the provided point
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES('InterpolatePointLine', 'LINESTRING M (0 0 0, 10 0 20)'),
                        ('InterpolatePointPoint', 'POINT(5 5)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_InterpolatePoint(foo.geom, bar.geom)
            FROM (SELECT geom FROM geometries WHERE name='InterpolatePointLine') AS foo,
                 (SELECT geom FROM geometries WHERE name='InterpolatePointPoint') AS bar
            """
        Then validate that following rows are in the stored rows
             | st_interpolatepoint |
             | 10.0 |

    Scenario: Returns a BOX3D representing the maximum extents of the geometry
       When execute following sql in db "opengeo" and store result in the context
            """
            SELECT Box3D(geom) FROM geom_linestrings WHERE id=1
            """
        Then validate that following rows are in the stored rows
             | box3d |
             | BOX3D(0 0 0,2 2 2) |

    Scenario: Returns a valid_detail (valid,reason,location) row stating if a geometry is valid or not and if not valid, a reason why and a location where
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT * FROM (SELECT ST_IsValidDetail(geom) FROM geom_linestrings WHERE id=1) AS foo
            """
        Then validate that following rows are in the stored rows
             | st_isvaliddetail|
             | (t,,) |

    Scenario: Return a specified ST_Geometry value from a collection of other geometries
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_st_memcollect(id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_memcollect VALUES
                (1, 'POINT(1 1)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_memcollect VALUES
                (2, 'POINT(2 2)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_memcollect VALUES
                (3, 'POINT(1 2)')
            """
        #When execute following sql in db "opengeo" and store result in the context
        #    """
        #    SELECT ST_AsText(ST_MemCollect(geom)) FROM t_st_memcollect
        #    """
        #Then validate that following rows are in the stored rows
        #     | st_astext |
        #     | GEOMETRYCOLLECTION(MULTIPOINT(1 1,2 2),POINT(1 2)) |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_Collect(geom)) FROM t_st_memcollect
            """
        Then validate that "1 1" is in the stored rows
        And validate that "2 2" is in the stored rows
        And validate that "1 2" is in the stored rows
        And validate that "MULTIPOINT(" is in the stored rows

        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_Union(geom)) FROM t_st_memcollect
            """
        Then validate that "1 1" is in the stored rows
        And validate that "2 2" is in the stored rows
        And validate that "1 2" is in the stored rows
        And validate that "MULTIPOINT(" is in the stored rows

        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_MemUnion(geom)) FROM t_st_memcollect
            """
        Then validate that "1 1" is in the stored rows
        And validate that "2 2" is in the stored rows
        And validate that "1 2" is in the stored rows
        And validate that "MULTIPOINT(" is in the stored rows

        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT (ST_Accum(geom)) FROM t_st_memcollect
            """
        Then validate that "0101000000000000000000F03F000000000000F03F" is in the stored rows
        And validate that "010100000000000000000000400000000000000040" is in the stored rows
        And validate that "0101000000000000000000F03F0000000000000040" is in the stored rows

        #When execute following sql in db "opengeo" and store result in the context
        #    """
        #    SELECT ST_AsText(ST_MakeLine(geom)) FROM t_st_memcollect
        #    """
        #Then validate that "1 1" is in the stored rows
        #And validate that "2 2" is in the stored rows
        #And validate that "1 2" is in the stored rows
        #And validate that "LINESTRING(" is in the stored rows
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_Extent(geom)) FROM t_st_memcollect
            """
        Then validate that following rows are in the stored rows
            | st_astext |
            | POLYGON((1 1,1 2,2 2,2 1,1 1)) |
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_3DExtent(geom)) FROM t_st_memcollect
            """
        Then validate that following rows are in the stored rows
            | st_astext |
            | POLYGON((1 1,1 2,2 2,2 1,1 1)) |

    Scenario: Creates a GeometryCollection made up of Polygons from a set of Geometries which contain lines that represents the Polygons edges
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_st_polygonize(id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_polygonize VALUES
                (1, 'LINESTRING(4 11, 4 4, 11 4, 11 11, 4 11)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_polygonize VALUES
                (2, 'LINESTRING(1 9, 1 5, 5 5, 5 9, 1 9)')
            """
       When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_Polygonize(geom)) FROM t_st_polygonize
            """
       Then validate that "POLYGON((4 11,11 11,11 4,4 4,4 11))" is in the stored rows
       And validate that "POLYGON((1 9,5 9,5 5,1 5,1 9))" is in the stored rows

    Scenario: Spatial indexing using default gist_geometry_ops_2d, and box2df for storage
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_st_box2df(id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_box2df VALUES
                (1, 'POINT(1 1)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_box2df VALUES
                (2, 'POINT(2 2)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_box2df VALUES
                (3, 'POINT(3 3)')
            """
        And below sql is executed in "opengeo" db
            """
            CREATE INDEX idx_box2df on t_st_box2df USING gist(geom)
            """
        And below sql is executed in "opengeo" db
            """
            SET enable_seqscan=off
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(geom) FROM t_st_box2df WHERE ST_X(geom) = 3
            """
        And below sql is executed in "opengeo" db
            """
            SET enable_seqscan=on
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | POINT(3 3) |

    Scenario: Spatial indexing using gist_geometry_ops_nd, and gidx for storage
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_st_gidx(id int, geom geometry) DISTRIBUTED BY (id)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_gidx VALUES
                (1, 'POINT(1 1 1)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_gidx VALUES
                (2, 'POINT(2 2 2)')
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_st_gidx VALUES
                (3, 'POINT(3 3 3)')
            """
        And below sql is executed in "opengeo" db
            """
            CREATE INDEX idx_gidx on t_st_gidx USING gist(geom gist_geometry_ops_nd)
            """
        And below sql is executed in "opengeo" db
            """
            SET enable_seqscan=off
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(geom) FROM t_st_gidx WHERE ST_X(geom) = 3
            """
        And below sql is executed in "opengeo" db
            """
            SET enable_seqscan=on
            """
        Then validate that following rows are in the stored rows
             | st_astext |
             | POINT Z (3 3 3) |

    Scenario: Relationship between two geometries
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_Touches(foo.geom, bar.geom),
                   ST_Relate(foo.geom, bar.geom),
                   ST_Crosses(foo.geom, bar.geom),
                   ST_Covers(foo.geom, bar.geom),
                   ST_CoveredBy(bar.geom, foo.geom),
                   ST_ContainsProperly(foo.geom, bar.geom),
                   ST_Disjoint(foo.geom, bar.geom),
                   ST_Within(foo.geom, bar.geom),
                   round(ST_Distance(foo.geom, bar.geom)),
                   ST_ASEWKT(ST_Difference(foo.geom, bar.geom)) AS difference,
                   ST_ASEWKT(ST_Intersection(foo.geom, bar.geom)) AS intersection,
                   ST_Overlaps(foo.geom, bar.geom),
                   ST_ASEWKT(ST_SymDifference(foo.geom, bar.geom)) AS symdiff,
                   ST_OrderingEquals(foo.geom, bar.geom),
                   ST_Equals(foo.geom, bar.geom),
                   round(ST_HausdorffDistance(foo.geom, bar.geom)),
                   ST_LineCrossingDirection(foo.geom, bar.geom) AS crossing
            FROM (SELECT geom FROM geom_linestrings WHERE id=1) AS foo,
                 (SELECT geom FROM geom_linestrings WHERE id=2) AS bar
            """
        Then validate that following rows are in the stored rows
             | st_touches | st_relate | st_crosses | st_covers | st_coveredby | st_containsproperly | st_disjoint | st_within | st_distance | difference | intersection | st_overlaps | symdiff | st_orderingequals | st_equals | st_hausdorffdistance  | crossing |
             | False | 1FFF0FFF2 | False | True | True | False | False | True | 0.0 | GEOMETRYCOLLECTION EMPTY | MULTILINESTRING((0 0 0,1 1 1.5),(1 1 1.5,2 1 2),(2 1 2,2 2 2)) | False | GEOMETRYCOLLECTION EMPTY | False | True | 0.0 | 0 |

    Scenario: Geography computation
        When below sql is executed in "opengeo" db
            """
            CREATE TABLE t_geography (name varchar, geog geography) DISTRIBUTED BY (name)
            """
        And below sql is executed in "opengeo" db
            """
            INSERT INTO t_geography VALUES
                        ('PointA', 'POINT(-118.4079 33.9434)'),
                        ('PointB', 'POINT(2.5559 49.0083)'),
                        ('LineStringA', 'LINESTRING(-118.4079 33.9434, 2.5559 49.0083)'),
                        ('LineStringB', 'LINESTRING(-119.4079 32.9434, 3.5559 46.0083)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(ST_Azimuth(foo.geog, bar.geog)) AS degaz,
                   round(ST_Azimuth(bar.geog, foo.geog)) AS degazrev,
                   round(ST_Distance(foo.geog, bar.geog)) AS distance,
                   ST_ASEWKT(ST_Intersection(line_a.geog, line_b.geog)) AS intersection
            FROM (SELECT geog FROM t_geography WHERE name='PointA') AS foo,
                 (SELECT geog FROM t_geography WHERE name='PointB') AS bar,
                 (SELECT geog FROM t_geography WHERE name='LineStringA') AS line_a,
                 (SELECT geog FROM t_geography WHERE name='LineStringB') AS line_b
            """
        Then validate that following rows are in the stored rows
             | degaz | degazrev | distance | intersection |
             | 1.0 | 5.0  | 9124665.0 | SRID=4326;GEOMETRYCOLLECTION EMPTY |

    Scenario: Returns a float between 0 and 1 representing the location of the closest point on LineString to the given Point, as a fraction of total 2d line length
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT round(ST_Line_Locate_Point(bar.geom, foo.geom)) AS location
            FROM (SELECT geom FROM geometries WHERE name='Point') AS foo,
                 (SELECT geom FROM geometries WHERE name='Linestring') AS bar
            """
        Then validate that following rows are in the stored rows
             | location |
             | 0.0 |

    Scenario: Creates a BOX2D defined by the given point geometries
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_ASEWKT(ST_MakeBox2D(foo.geom, bar.geom)) AS box2d
            FROM (SELECT geom FROM geometries WHERE name='Point') AS foo,
                 (SELECT geom FROM geometries WHERE name='InterpolatePointPoint') AS bar
            """
        Then validate that following rows are in the stored rows
             | box2d |
             | POLYGON((0 0,0 5,5 5,5 0,0 0)) |

    Scenario: Creates a BOX3D defined by the given point geometries
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                        ('Box3DPointA', 'POINT(-989502.1875 528439.5625 10)'),
                        ('Box3DPointB', 'POINT(-987121.375 529933.1875 10)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_3DMakeBox(foo.geom, bar.geom) AS box3d
            FROM (SELECT geom FROM geometries WHERE name='Box3DPointA') AS foo,
                 (SELECT geom FROM geometries WHERE name='Box3DPointB') AS bar
            """
        Then validate that following rows are in the stored rows
             | box3d |
             | BOX3D(-989502.1875 528439.5625 10,-987121.375 529933.1875 10) |

    Scenario: Snap all points of the input geometry to a regular grid
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                        ('SnapToGridLine', 'LINESTRING(-1.1115678 2.123 2.3456 1.11111,4.111111 3.2374897 3.1234 1.1111, -1.11111112 2.123 2.3456 1.1111112)'),
                        ('SnapToGridPoint', 'POINT(1.12 2.22 3.2 4.4444)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_ASEWKT(ST_SnapToGrid(foo.geom, bar.geom, 0.1, 0.1, 0.1, 0.01)) AS snap
            FROM (SELECT geom FROM geometries WHERE name='SnapToGridLine') AS foo,
                 (SELECT geom FROM geometries WHERE name='SnapToGridPoint') AS bar
            """
        Then validate that following rows are in the stored rows
             | snap |
             | LINESTRING(-1.08 2.12 2.3 1.1144,4.12 3.22 3.1 1.1144,-1.08 2.12 2.3 1.1144) |

    Scenario: Snap segments and vertices of input geometry to vertices of a reference geometry
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                        ('SnapPoly', 'MULTIPOLYGON(((26 125, 26 200, 126 200, 126 125, 26 125),(51 150, 101 150, 76 175, 51 150 )),((151 100, 151 200, 176 175, 151 100)))'),
                        ('SnapLine', 'LINESTRING (5 107, 54 84, 101 100)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_ASEWKT(ST_Snap(line.geom, poly.geom, ST_Distance(poly.geom, line.geom)*1.01)) AS snap
            FROM (SELECT geom FROM geometries WHERE name='SnapLine') AS line,
                 (SELECT geom FROM geometries WHERE name='SnapPoly') AS poly
            """
        Then validate that following rows are in the stored rows
             | snap |
             | LINESTRING(5 107,26 125,54 84,101 100) |

    Scenario: Returns a collection of geometries resulting by splitting a geometry
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                        ('SplitCircle', 'POINT(100 90)'),
                        ('SplitLine', 'LINESTRING(10 10,190 190)')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText((ST_Dump(ST_Split(circle.buffer, line.geom))).geom) AS split
            FROM (SELECT geom FROM geometries WHERE name='SplitLine') AS line,
                 (SELECT ST_Buffer(geom, 50) AS buffer FROM geometries WHERE name='SplitCircle') AS circle
            """
        Then validate that following rows are in the stored rows
             | split |
             | POLYGON((150 90,149.039264020162 80.2454838991936,146.193976625564 70.8658283817455,141.573480615127 62.2214883490199,135.355339059327 54.6446609406727,127.77851165098 48.4265193848728,119.134171618255 43.8060233744357,109.754516100806 40.9607359798385,100 40,90.2454838991937 40.9607359798385,80.8658283817456 43.8060233744356,72.22148834902 48.4265193848727,64.6446609406727 54.6446609406725,60.1371179574584 60.1371179574584,129.862882042542 129.862882042542,135.355339059327 125.355339059327,141.573480615127 117.77851165098,146.193976625564 109.134171618255,149.039264020162 99.7545161008065,150 90)) |
             | POLYGON((60.1371179574584 60.1371179574584,58.4265193848728 62.2214883490198,53.8060233744357 70.8658283817454,50.9607359798385 80.2454838991934,50 89.9999999999998,50.9607359798384 99.7545161008062,53.8060233744356 109.134171618254,58.4265193848726 117.77851165098,64.6446609406725 125.355339059327,72.2214883490197 131.573480615127,80.8658283817453 136.193976625564,90.2454838991934 139.039264020161,99.9999999999998 140,109.754516100806 139.039264020162,119.134171618254 136.193976625564,127.77851165098 131.573480615127,129.862882042542 129.862882042542,60.1371179574584 60.1371179574584)) |

    Scenario: Replace point N of linestring with given point. Index is 0-based
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_SetPoint(foo.geom, 1, bar.geom)) AS set_point
            FROM (SELECT geom FROM geometries WHERE name='Point') AS bar,
                 (SELECT geom FROM geom_linestrings_2d WHERE id=3) AS foo
            """
        Then validate that following rows are in the stored rows
             | set_point |
             | LINESTRING(0 0,0 0,2 1,2 2) |

    # Non-deterministic order of start point and end point of resulted
	# linestrings in multi-segment env.
    Scenario: Returns a collection containing paths shared by the two input linestrings/multilinestrings
        When below sql is executed in "opengeo" db
            """
            INSERT INTO geometries VALUES
                        ('SharedPathsLine', 'LINESTRING(76 175,90 161,126 125,126 156.25,151 100)'),
                        ('SharedPathsMultiLine', 'MULTILINESTRING((26 125,26 200,126 200,126 125,26 125),(51 150,101 150,76 175,51 150))')
            """
        When execute following sql in db "opengeo" and store result in the context
            """
            SELECT ST_AsText(ST_SharedPaths(foo.geom, bar.geom)) AS shared_paths
            FROM (SELECT geom FROM geometries WHERE name='SharedPathsLine') AS bar,
                 (SELECT geom FROM geometries WHERE name='SharedPathsMultiLine') AS foo
            """
        Then validate that "GEOMETRYCOLLECTION(" is in the stored rows
        Then validate that "MULTILINESTRING EMPTY" is in the stored rows
        Then validate that "GEOMETRYCOLLECTION" is in the stored rows
        Then validate that "(101 150,90 161)" is in the stored rows
        Then validate that "(90 161,76 175)" is in the stored rows
        Then validate that "(126 156.25,126 125)" is in the stored rows

    # TODO:
    #
    # 1) JSON-C: SELECT
    # ST_AsText(ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}')) As wkt;
