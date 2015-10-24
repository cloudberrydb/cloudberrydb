import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class query03 {
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
    ResultSet rs = null;
    int m = 0;

    try {
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl1");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("CREATE TABLE tbl1 (s1 text, s2 text, s3 text, dt timestamp,n1 smallint, n2 integer, n3 bigint, n4 decimal, n5 numeric, n6 real, n7 double precision)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl1 VALUES('aaa','twoa','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789),('bbb','twob','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789),('ccc','twoc','shpits','2011-06-01 12:30:30',23,732,834567,45.67,789.123,7.12345,123.456789 )");
      System.out.println("INSERTED ROWS "+m);
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl2");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("CREATE TABLE tbl2 (LIKE tbl1)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl2 SELECT * FROM tbl1");
      System.out.println("INSERTED ROWS "+m);
      rs = s.executeQuery("SELECT * FROM tbl2 order by s1");
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
    } catch (SQLException se) {
      System.out.println("We got an exception while executing our query:" +
                    "that probably means our SQL is invalid");
      se.printStackTrace();
      System.exit(1);
    }
  }     
}
