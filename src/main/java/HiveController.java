import java.sql.*;

public class HiveController {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();

        String databaseName = "traffic_limits";
        String tableName = "limits_per_hour";

        stmt.execute("use " + databaseName);

        stmt.execute("create table " + tableName + " (limit_name string, limit_value int, effective_date int) row format delimited fields terminated by \",\"");

        //String sql = "load data local inpath '/home/cloudera/a.txt' overwrite into table " + tableName;
        //stmt.execute(sql);

        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getInt(2));
        }

    }
}
