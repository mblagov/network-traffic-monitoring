import java.sql.*;

public class HiveController<T> {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String databaseName = "traffic_limits";
    private static String tableName = "limits_per_hour";

    public void create() throws SQLException{
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();
        stmt.execute("use " + databaseName);
        stmt.execute("create table " + tableName + " (limit_name string, limit_value int, effective_date int) row format delimited fields terminated by \",\"");
    }

    public void push(String limit_name, String limit_value, String effective_date) throws SQLException{
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();

        String sql = "insert into table" + tableName + String.format("values (%s,%s,%s)", limit_name, limit_value, effective_date);
        stmt.executeQuery(sql);
    }

    public void select() throws SQLException{
        Connection con= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();

        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getInt(2));
        }
    }
}
