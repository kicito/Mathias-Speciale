package jolie.transactionservice;

import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        String connectionString = "jdbc:postgresql:example-db";
        Connection con = DriverManager.getConnection(connectionString, "postgres", "example");
        con.setAutoCommit(false);

        PreparedStatement statement = con
                .prepareStatement("UPDATE numbers SET number = number + 1 WHERE username != 'lol'");
        int i = statement.executeUpdate();
        con.commit();
        con.close();
        System.out.println("Hello world!");
    }
}
