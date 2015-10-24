import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

public class SimplePostgresSQL {
  public static void main(String[] argv) {

  String pgport = System.getenv("PGPORT");
  String username = System.getenv("PGUSER");
  String hostname= System.getenv("PGHOST");
  System.out.println("Checking if Driver is registered with DriverManager.");

  if (username == null)
    username = System.getProperty("user.name");

  try {
    Class.forName("org.postgresql.Driver");
  } catch (ClassNotFoundException cnfe) {
    System.out.println("Couldn't find the driver!");
    System.out.println("Let's print a stack trace, and exit.");
    cnfe.printStackTrace();
    System.exit(1);
  }
  
  Connection c = null;
  
  try {
    // The second and third arguments are the username and password,
    // respectively. They should be whatever is necessary to connect
    // to the database.
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
  
  if (c != null) {
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
      m = s.executeUpdate("CREATE TABLE jdbc_table (a int)");
      System.out.println("CREATED TABLE");
      m = s.executeUpdate("INSERT INTO jdbc_table select i from generate_series(1, 100) i");
      System.out.println("INSERTED ROWS "+m);
      rs = s.executeQuery("SELECT a from jdbc_table order by 1");
      System.out.println("QUERY TABLE");

      int count = 0;
      while (rs.next ()) {
        int idVal = rs.getInt ("a");
        System.out.println ("col a = " + idVal);
        ++count;
      }
      rs.close ();

      m = s.executeUpdate("DROP TABLE jdbc_table");
      System.out.println("DROP TABLE");

      s.close();
      c.close();

    } catch (SQLException se) {
      System.out.println("We got an exception while executing our query:" +
                    "that probably means our SQL is invalid");
      se.printStackTrace();
      System.exit(1);
    }
  } else
    System.out.println("We should never get here.");
  }

}
