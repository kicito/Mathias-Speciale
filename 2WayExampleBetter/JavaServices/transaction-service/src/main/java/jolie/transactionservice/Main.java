package jolie.transactionservice;

import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        String connectionString = "jdbc:postgresql:example-db";
        Connection con = DriverManager.getConnection(connectionString, "postgres", "example");

        PreparedStatement statement = con
                .prepareStatement("INSERT INTO numbers (username, number) VALUES('user8', 123);");
        ResultSet set = statement.executeQuery();
        con.close();
        System.out.println("Hello world!");
    }
}
