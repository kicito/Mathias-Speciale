package jolie.transactionservice;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jolie.runtime.CanUseJars;
import jolie.runtime.FaultException;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;

/**
 * Represents a class which can connect to a single database, but then open
 * several connections to that database concurrently.
 * 
 * Some of the code has been 'inspired' by the Database service in the standard
 * library of Jolie:
 * 'https://github.com/jolie/jolie/blob/master/javaServices/coreJavaServices/src/main/java/joliex/db/DatabaseService.java'
 */

@CanUseJars({
        "jdbc-postgresql.jar", // PostgreSQL is the one I use for now.
        "jdbc-sqlite.jar" // SQLite
})
public class TransactionService extends JavaService {

    // TODO: Think about adding a timeout to openTransactions
    private HashMap<String, java.sql.Connection> openTransactions = new HashMap<String, java.sql.Connection>();
    private final Object m_transactionMutex = new Object();

    private String m_connectionString = null;
    private String m_username = null;
    private String m_password = null;
    private String m_driver = null;
    private String m_driverClass = null;

    /**
     * Sets or overwrites the current connection string, meaning which database any
     * writes are made to.
     * 
     * @param request
     * @throws FaultException
     */
    public void connect(Value request) throws FaultException {
        synchronized (m_transactionMutex) {
            // We need to lock here, since we want to ensure that nothing is added
            // toopenTransactions after the check below has returned true

            if (openTransactions.keySet().size() > 0) { // I don't want to deal with logic for handling this lol
                throw new FaultException("ConnectionError",
                        "Cannot connect to other database while some transactions are still incomplete");
            } else { // There are no open transactions, we can safely move the connection to a new db

                m_driver = request.getChildren("driver").first().strValue();
                if (request.getFirstChild("driver").hasChildren("class")) {
                    m_driverClass = request.getFirstChild("driver").getFirstChild("class").strValue();
                }
                String host = request.getChildren("host").first().strValue();
                String port = request.getChildren("port").first().strValue();
                String databaseName = request.getChildren("database").first().strValue();
                m_username = request.getChildren("username").first().strValue();
                m_password = request.getChildren("password").first().strValue();
                String attributes = request.getFirstChild("attributes").strValue();
                String separator = "/";
                boolean isEmbedded = false;
                Optional<String> encoding = Optional
                        .ofNullable(
                                request.hasChildren("encoding") ? request.getFirstChild("encoding").strValue() : null);

                try {
                    if (m_driverClass != null) {
                        Class.forName(m_driverClass);
                    } else {
                        // This should not be a switch statement in this case, since we only have a
                        // single allowed value
                        // but then again, in the original it's 11 else-if statements, so that probably
                        // should have been a switch
                        // Also, as far as I can see, this is only here to throw an exception if the
                        // correct driver is not in the 'lib' folder
                        switch (m_driver) {
                            case "postgresql":
                                Class.forName("org.postgresql.Driver");
                                break;
                            case "sqlite":
                                Class.forName("org.sqlite.JDBC");
                                isEmbedded = true;
                                break;
                            default:
                                throw new FaultException("InvalidDriver", "Unknown type of driver: " + m_driver);
                        }
                    }

                    if (isEmbedded) // Driver is SQLITE
                    {
                        m_connectionString = "jdbc:" + m_driver + ":" + databaseName;
                        if (!attributes.isEmpty()) {
                            m_connectionString += ";" + attributes;
                        }
                    } else // Driver is postgres
                    {
                        m_connectionString = "jdbc:" + m_driver + "://" + host + (port.isEmpty() ? "" : ":" + port)
                                + separator + databaseName;
                        if (encoding.isPresent()) {
                            m_connectionString += "?characterEncoding=" + encoding.get();
                        }
                    }
                } catch (ClassNotFoundException e) {
                    throw new FaultException("DriverClassNotFound", e);
                }
            }
        }
    }

    public String startTransaction(Value request) throws FaultException {
        Connection con;
        try {
            con = DriverManager.getConnection(
                    m_connectionString,
                    m_username,
                    m_password);
            con.setAutoCommit(false);
            String uuid = UUID.randomUUID().toString();
            synchronized (m_transactionMutex) {
                openTransactions.put(uuid, con);
            }
            return uuid;
        } catch (SQLException e) {
            throw new FaultException("SQLException", "Error while starting transaction.");
        }
    }

    public Value executeQueryInTransaction(Value input) {
        String transactionId = input.getFirstChild("handle").strValue();
        String query = input.getFirstChild("query").strValue();

        Value response = Value.create();

        try {
            Connection con = openTransactions.get(transactionId);
            PreparedStatement statement = con.prepareStatement(query);
            ResultSet rs = statement.executeQuery();

            // Return changes, which I hope can be found from the RS
            return response;
        } catch (SQLException e) {
            return response;
            // return error
        }
    }

    public Value commitTransaction(String tId) {
        Value response = Value.create();

        try {
            Connection con = openTransactions.get(tId);
            con.commit();
            // Return success
            return response;
        } catch (SQLException e) {
            // return error
            return response;
        }
    }
}
