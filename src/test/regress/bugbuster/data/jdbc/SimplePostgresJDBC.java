import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

public class SimplePostgresJDBC {
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
  
  System.out.println("Registered the driver ok, so let's make a connection.");
  
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
      System.out.println(" Connection string = " + "jdbc:postgresql://localhost:" + pgport + "/template1");
      System.out.println("   username = " + username);
    System.out.println("Couldn't connect: print out a stack trace and exit.");
    se.printStackTrace();
    System.exit(1);
  }
  
  if (c != null)
    System.out.println("Hooray! We connected to the database!");
  else
    System.out.println("We should never get here.");
  }
}
