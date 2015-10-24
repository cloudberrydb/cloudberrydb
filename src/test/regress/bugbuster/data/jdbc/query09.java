import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class query09 {
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
      
      m = s.executeUpdate("DROP EXTERNAL TABLE IF EXISTS tbl11");
      System.out.println("DROP TABLE");
      String str1="create writable EXTERNAL table tbl11(a int, b int) LOCATION ('gpfdist://";
      str1=str1+hostname;
      str1=str1+":45555/data/tbl2.tbl') FORMAT 'TEXT' (DELIMITER '|')";
      m = s.executeUpdate(str1);
      System.out.println("CREATE TABLE");
      
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl22");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("create table tbl22(a int, b int) DISTRIBUTED BY (b)");
      System.out.println("CREATE TABLE");
      m = s.executeUpdate("INSERT INTO tbl22 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(1,2),(1,3),(1,4),(1,5),(1,6)");
      System.out.println("INSERTED ROWS "+m);    
      
      Runtime run = Runtime.getRuntime();
            
	  run.exec("killall gpfdist");
	  String path = new java.io.File(".").getCanonicalPath();
      run.exec("rm -rf "+path+"/data/tbl2.tbl");
	  //run.exec("ssh "+username+"@"+hostname+" && source "+System.getenv("GPHOME")+"/greenplum_path.sh");
	  run.exec("gpfdist -p 45555 -d "+path);
      Thread.currentThread().sleep(2000);//sleep for 2000 ms
      
      m = s.executeUpdate("INSERT INTO tbl11 SELECT * FROM tbl22");
      System.out.println("INSERTED ROWS "+m);
      
      m = s.executeUpdate("DROP EXTERNAL TABLE IF EXISTS tbl11");
      System.out.println("DROP TABLE");
      str1="create EXTERNAL table tbl11(a int, b int) LOCATION ('gpfdist://";
      str1=str1+hostname;
      str1=str1+":45555/data/tbl2.tbl') FORMAT 'TEXT' (DELIMITER '|')";
      m = s.executeUpdate(str1);
      System.out.println("CREATE TABLE");
      
      m = s.executeUpdate("DROP TABLE IF EXISTS tbl22");
      System.out.println("DROP TABLE");
      m = s.executeUpdate("create table tbl22(a int, b int) DISTRIBUTED BY (b)");
      System.out.println("CREATE TABLE");
      
      m = s.executeUpdate("INSERT INTO tbl22 SELECT * FROM tbl11");
      System.out.println("INSERTED ROWS "+m);
      
      rs = s.executeQuery("SELECT * FROM tbl22 ORDER BY b, a");
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
