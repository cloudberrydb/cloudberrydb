import java.sql.*;

/**

    Test GreenPlum through JDBC.
    *
    User: zvi
    Date: 2/28/11
    Time: 6:57 PM
*/
    public class mpp12490
    {

        public static Connection getConnection(String driverClass, String connUrl, String userName, String userPass, boolean autoCommitOn) throws Exception
        {
            try {
                // Register the database jdbc class.
                DriverManager.registerDriver((Driver) Class.forName(driverClass).newInstance());
                // Establish a connection to the database.
                Connection con = DriverManager.getConnection(connUrl, userName, userPass);
                con.setAutoCommit(autoCommitOn);
                return con;
            } catch ( ClassNotFoundException cnfe ) {
                throw new Exception("ClassNotFoundException Class " + driverClass + " not found");
            } catch ( InstantiationException ie ) {
                throw new Exception("InstantiationException Failed to instantiate " + driverClass);
            } catch ( IllegalAccessException iae ) {
                throw new Exception("IllegalAccessException Illegal access " + driverClass);
            } catch ( SQLException sqle ) {
                throw new Exception("SQLException Cannot acquire a new connection to " + connUrl + " using driver '" + driverClass + "'", sqle);
            } catch ( Exception e ) {
                throw new Exception("Exception Cannot acquire a new connection to " + connUrl + " using driver '" + driverClass + "'", e);
            }
        }

        static final String dbPass = "";

        public static void main(String[] args) throws Exception
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;

            try {
                String pgport = System.getenv("PGPORT");
                String username = System.getenv("PGUSER");
                String hostname= System.getenv("PGHOST");
                conn = getConnection("org.postgresql.Driver", "jdbc:postgresql://"+hostname+":"+pgport+"/template1", username, dbPass, false);
                stmt = conn.createStatement();

                //stmt.execute("select 1;");
                boolean ans = stmt.execute("select f1()");

                rs = stmt.getResultSet();
                while ( rs.next() )
                    { System.out.println("Query Result: " + rs.getInt(1)); }

            } finally {
                if ( rs != null ) rs.close();
                if ( stmt != null ) stmt.close();
                if ( conn != null ) conn.close();
            }
        }

    }
