import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class query10{
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
      m = s.executeUpdate("drop external table IF EXISTS set1");
      System.out.println("DROP TABLE");
      String str1="create external web table set1(a1 text) execute E'(cd ";
      str1=str1+path;
      str1=str1+" && ls query0*.out)' ON MASTER FORMAT 'TEXT'";
      m = s.executeUpdate(str1);
      System.out.println("CREATE TABLE");
     
      rs = s.executeQuery("select * from set1");
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
      Runtime run = Runtime.getRuntime();
      if (c != null) c.close();
      rs.close ();
      s.close();
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
