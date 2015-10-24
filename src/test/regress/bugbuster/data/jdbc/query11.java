import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class query11 {
  public static void main(String[] argv) {

  String pgport = System.getenv("PGPORT");
  String username = System.getenv("PGUSER");
  String hostname= System.getenv("PGHOST");
  System.out.println("Checking if Driver is registered with DriverManager.");

  if (username == null)
    username = System.getProperty("user.name");

  Connection c = null;
  
  try {
      String hostAndPort = null;

      if (pgport != null)
	  hostAndPort = hostname+":" + pgport;
      else
	  hostAndPort = hostname;

      c = DriverManager.getConnection("jdbc:postgresql://" + hostAndPort + "/template1",
				       username, "");
  } catch (SQLException se) {
    System.out.println("Couldn't connect: print out a stack trace and exit.");
    se.printStackTrace();
    System.exit(1);
  }
  Statement s = null;

    try {
      s = c.createStatement();
    } catch (SQLException se) {
      System.out.println("We got an exception while creating a statement:" +
                     "that probably means we're no longer connected.");
      se.printStackTrace();
      System.exit(1);
    }
   try {
    ResultSet rs = null;
    int m = 0;
      String path = new java.io.File(".").getCanonicalPath();
      Runtime run = Runtime.getRuntime();
      run.exec("rm -rf "+path+"/data/out.tbl");
      
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl1");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("CREATE TABLE tbl1 (s1 text, s2 text, s3 text, dt timestamp,n1 smallint, n2 integer, n3 bigint, n4 decimal, n5 numeric, n6 real, n7 double precision)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl1 VALUES('aaa','twoa','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789),('bbb','twob','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789),('ccc','twoc','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789 )");
      System.out.println("INSERTED ROWS "+m);
      m = s.executeUpdate("CREATE OR REPLACE FUNCTION fxwd_in() RETURNS record AS '$libdir/fixedwidth.so', 'fixedwidth_in' LANGUAGE C STABLE");
      m = s.executeUpdate("CREATE OR REPLACE FUNCTION fxwd_out(record) RETURNS bytea AS '$libdir/fixedwidth.so', 'fixedwidth_out' LANGUAGE C STABLE");
      System.out.println("CREATE FUNCTIONS");
      
      m = s.executeUpdate("DROP EXTERNAL TABLE IF EXISTS tbl33");
      System.out.println("DROP TABLE");
      String str1="CREATE WRITABLE EXTERNAL TABLE tbl33 (s1 char(10), s2 varchar(10), s3 text, dt timestamp, n1 smallint, n2 integer, n3 bigint, n4 decimal, n5 numeric, n6 real, n7 double precision) LOCATION ('gpfdist://";
      str1=str1+hostname;
      str1=str1+":45555/data/out.tbl') FORMAT 'CUSTOM' (formatter='fxwd_out', s1='10', s2='10', s3='10', dt='20', n1='5', n2='10', n3='10', n4='10', n5='10', n6='10', n7='15',line_delim=E'\n')";
      m = s.executeUpdate(str1);
      System.out.println("CREATE TABLE");
            
      run.exec("killall gpfdist");
      Thread.currentThread().sleep(1000);//sleep for 1000 ms
      //run.exec("ssh "+username+"@"+hostname+" && source "+System.getenv("GPHOME")+"/greenplum_path.sh");
      run.exec("gpfdist -p 45555 -d "+path);
      Thread.currentThread().sleep(3000);//sleep for 3000 ms
      
      m = s.executeUpdate("INSERT INTO tbl33 SELECT * FROM tbl1");
      System.out.println("INSERTED ROWS "+m);
      m = s.executeUpdate("DROP EXTERNAL TABLE IF EXISTS tbl44");
      System.out.println("DROP TABLE");
      str1="CREATE READABLE EXTERNAL TABLE tbl44 (s1 char(10), s2 varchar(10), s3 text, dt timestamp, n1 smallint, n2 integer, n3 bigint, n4 decimal, n5 numeric, n6 real, n7 double precision) LOCATION ('gpfdist://";
      str1=str1+hostname;
      str1=str1+":45555/data/out.tbl') FORMAT 'CUSTOM' (formatter='fxwd_in', s1='10', s2='10', s3='10', dt='20', n1='5', n2='10', n3='10', n4='10', n5='10', n6='10', n7='15',line_delim=E'\n')";
      m = s.executeUpdate(str1);
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl1");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("CREATE TABLE tbl1 (s1 text, s2 text, s3 text, dt timestamp,n1 smallint, n2 integer, n3 bigint, n4 decimal, n5 numeric, n6 real, n7 double precision)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl1 SELECT * FROM tbl44");
      System.out.println("INSERTED ROWS "+m);
      
      rs = s.executeQuery("SELECT * FROM tbl1 ORDER BY s1");
      System.out.println("QUERY TABLE");
	        
      ResultSetMetaData rsmd = rs.getMetaData();
      int numberOfColumns = rsmd.getColumnCount();
  
      while (rs.next()) {
        for (int i = 1; i <= numberOfColumns; i++) {
          if (i > 1) System.out.print(",  ");
          String columnValue = rs.getString(i);
          System.out.print(columnValue);
        }
        System.out.println("");  
      }

    if (c != null) c.close();
    rs.close ();
    s.close();
    run.exec("killall gpfdist");
    } catch (SQLException se) {
      System.out.println("We got an exception while executing our query:" +
                    "that probably means our SQL is invalid");
      se.printStackTrace();
      System.exit(1);
    } catch(Exception e) {
	  e.printStackTrace();
	}
 }     
}
