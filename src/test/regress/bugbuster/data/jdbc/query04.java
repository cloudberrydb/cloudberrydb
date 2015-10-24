import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class query04 {
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
      m = s.executeUpdate("create table tbl1(a int, b int) WITH (appendonly=true)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(1,2),(1,3),(1,4),(1,5),(1,6)");
      System.out.println("INSERTED ROWS "+m);
      try {
      		m = s.executeUpdate("DELETE FROM tbl1 WHERE a=7");
      		m = s.executeUpdate("DELETE FROM tbl1 WHERE a=b");
      }catch (SQLException se) {
             System.out.println("CAN'T DELETE BECAUSE THE TABLE IS APPEND-ONLY");
      }
      rs = s.executeQuery("SELECT * FROM tbl1 ORDER BY a,b");
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
