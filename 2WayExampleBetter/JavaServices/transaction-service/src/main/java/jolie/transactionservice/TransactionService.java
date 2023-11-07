package jolie.transactionservice;

import java.util.concurrent.atomic.AtomicInteger;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class TransactionService extends JavaService {

    AtomicInteger ai = new AtomicInteger();

    public Value sayHello(Value input) {
        System.out.println("Java saying hello from process " + ProcessHandle.current().pid());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Value response = Value.create();
        System.out.println("Finished Java with int: " + ai.incrementAndGet());
        response.getFirstChild("response").setValue("finished");
        return response;
    }
}

/**
 * public class TransactionService2 extends JavaService {
 * 
 * ConcurrentHashMap<String, java.sql.Connection> connections = new
 * ConcurrentHashMap<String, java.sql.Connection>();
 * 
 * public String openTransaction() {
 * Connection con;
 * try {
 * con = DriverManager.getConnection("whatever connectioninfo");
 * con.setAutoCommit(false);
 * String uuid = UUID.randomUUID().toString();
 * connections.put(uuid, con);
 * return uuid;
 * } catch (SQLException e) {
 * return "Hey, something went wrong!";
 * }
 * }
 * 
 * public Value executeQueryInTransaction(Value input) {
 * String transactionId = input.getFirstChild("tId");
 * String query = input.getFirstChild("query");
 * 
 * try {
 * Connection con = connections.get(transactionId);
 * PreparedStatement statement = con.prepareStatement(query);
 * ResultSet rs = statement.executeQuery();
 * // Return changes, which I hope can be found from the RS
 * } catch (SQLException e) {
 * // return error
 * }
 * }
 * 
 * public Value commitTransaction(String tId) {
 * try {
 * Connection con = connections.get(tId);
 * con.commit();
 * // Return success
 * } catch (SQLException e) {
 * // return error
 * }
 * }
 * }
 * 
 */