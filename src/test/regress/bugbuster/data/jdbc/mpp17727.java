import java.net.URL;
import java.sql.*;

class mpp17727 {

public static void main (String args[]) {

   String pgport = System.getenv("PGPORT");
   String username = System.getenv("PGUSER");
   String hostname= System.getenv("PGHOST");
   if (username == null)
      username = System.getProperty("user.name");

   if (hostname == null)
      hostname = "localhost";

   String url = "jdbc:postgresql://" +hostname+ ":" +pgport+ "/template1";

   Connection con = null;

   try {
      Class.forName ("org.postgresql.Driver");
      //con = DriverManager.getConnection (url, username, "");
      con = DriverManager.getConnection(url);

      con.setAutoCommit(false);

      String droptab = "DROP TABLE TABDOESNTEXIST";
      Statement stm =  con.createStatement(); 
      System.out.println("\nExecute statement: "+droptab);
      stm.executeUpdate(droptab);
   }
   catch (SQLException ex) {
      System.out.println ("\n*** SQLException caught ***\n");
      while (ex != null) {
         System.out.println ("SQLState: " + ex.getSQLState ()); System.out.println ("Message:  " + ex.getMessage ()); System.out.println ("Vendor:   " + ex.getErrorCode ()); 
         ex = ex.getNextException ();
         System.out.println ("");
      }
   }
   catch (java.lang.Exception ex) {
      ex.printStackTrace ();
   }

   try {
      con.rollback(); 
      System.out.println("\nRollback transaction: ");
   }
   catch (SQLException ex) {
      System.out.println ("\n*** SQLException caught ***\n");
      while (ex != null) {
         System.out.println ("SQLState: " + ex.getSQLState ()); System.out.println ("Message:  " + ex.getMessage ()); System.out.println ("Vendor:   " + ex.getErrorCode ());
         ex = ex.getNextException ();
         System.out.println ("");
      }
   }
   catch (java.lang.Exception ex) {
      ex.printStackTrace ();
   }

   try {
      con.close();
      System.out.println("\nclosing Connection, tabdoesntexist should still exist in database");
   }
   catch (SQLException ex) {
      System.out.println ("\n*** SQLException caught ***\n");
      while (ex != null) {
         System.out.println ("SQLState: " + ex.getSQLState ()); System.out.println ("Message:  " + ex.getMessage ()); System.out.println ("Vendor:   " + ex.getErrorCode ());
         ex = ex.getNextException ();
         System.out.println ("");
      }
   }
   catch (java.lang.Exception ex) {
      ex.printStackTrace ();
   }

}
}
