package jolie.transactionservice;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jolie.runtime.ByteArray;
import jolie.runtime.CanUseJars;
import jolie.runtime.FaultException;

//  Jolie imports
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;

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

                // try {
                // if (m_driverClass != null) {
                // Class.forName(m_driverClass);
                // } else {
                // // This should not be a switch statement in this case, since we only have a
                // // single allowed value
                // // but then again, in the original it's 11 else-if statements, so that
                // probably
                // // should have been a switch
                // // This part might not even be needed. Will test without.
                // switch (m_driver) {
                // case "postgresql":
                // Class.forName("org.postgresql.Driver");
                // break;
                // case "sqlite":
                // Class.forName("org.sqlite.JDBC");
                // isEmbedded = true;
                // break;
                // default:
                // throw new FaultException("InvalidDriver", "Unknown type of driver: " +
                // m_driver);
                // }
                // }

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
                // } catch (ClassNotFoundException e) {
                // throw new FaultException("DriverClassNotFound", e);
                // }
            }
        }

    }

    public String initiate(Value request) throws FaultException {
        Connection con;
        try {
            con = DriverManager.getConnection(
                    m_connectionString,
                    m_username,
                    m_password);
            con.setAutoCommit(false); // This line is where the magic lies
            String uuid = UUID.randomUUID().toString();
            synchronized (m_transactionMutex) {
                openTransactions.put(uuid, con);
            }
            return uuid;
        } catch (SQLException e) {
            throw new FaultException("SQLException", "Error while starting transaction.");
        }
    }

    public Value executeQuery(Value input) throws FaultException {
        String transactionHandle = input.getFirstChild("handle").strValue();
        String query = input.getFirstChild("query").strValue();

        Value response = Value.create();

        try {
            Connection con = openTransactions.get(transactionHandle);
            PreparedStatement statement = con.prepareStatement(query);
            ResultSet result = statement.executeQuery();

            resultSetToValueVector(result, response.getChildren("row"));
            // Return changes, which I hope can be found from the RS
            return response;
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    public Value executeUpdate(Value input) throws FaultException {
        String transactionHandle = input.getFirstChild("handle").strValue();
        String query = input.getFirstChild("update").strValue();
        Value response = Value.create();
        try {
            Connection con = openTransactions.get(transactionHandle);
            PreparedStatement statement = con.prepareStatement(query);
            int numberRowsUpdated = statement.executeUpdate();

            response.setValue(numberRowsUpdated);
            return response;
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    public Value commit(String transactionHandle) throws FaultException {
        Value response = Value.create();

        try {
            Connection con = openTransactions.get(transactionHandle);
            con.commit();
            response.setValue("Transaction " + transactionHandle + " was commited sucessfully.");
            return response;
        } catch (SQLException e) {
            throw new FaultException("SQLException", e);
        }
    }

    // *-------------------------- Private fucntions -------------------------- */
    /**
     * Fills reach row in the matrix vector with data from corresponding entries in
     * result. Basically, copy the values from a ResultSet into a Jolie ValueVector.
     * 
     * @param result - The ResultSet to copy from
     * @param vector - The ValueVector to fill.
     * @throws SQLException - Thrown if any operation on result fails.
     */
    private static void resultSetToValueVector(ResultSet result, ValueVector vector)
            throws SQLException {
        Value rowValue, fieldValue;
        ResultSetMetaData metadata = result.getMetaData();
        int cols = metadata.getColumnCount();
        int i;
        int rowIndex = 0;

        // As opposed to the Database service, for this simple example, we don't support
        // ToUpper and ToLower.

        while (result.next()) {
            rowValue = vector.get(rowIndex);
            for (i = 1; i <= cols; i++) {
                fieldValue = rowValue.getFirstChild(metadata.getColumnLabel(i));
                setValue(fieldValue, result, metadata.getColumnType(i), i);
            }
            rowIndex++;
        }
    }

    /**
     * Sets the value of fieldValue to the corresponding field in result. This
     * functions purely as a sort of 'parsing', it seems.
     * I've removed support for non-integer values for the sake of
     * reading-simplicity. Also, don't ask me why they used a switch statement here,
     * but not in the 'connect' method.
     * 
     * So glad I didn't have to write this by hand - I yoinked it directly from the
     * Database Service of jolie
     * 
     * @throws SQLException - Thrown if any operation on result fails.
     */
    private static void setValue(Value fieldValue, ResultSet result, int columnType, int index)
            throws SQLException {
        switch (columnType) {
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                fieldValue.setValue(result.getInt(index));
                break;
            case java.sql.Types.BIGINT:
                fieldValue.setValue(result.getLong(index));
                break;
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.LONGNVARCHAR:
                String s = result.getNString(index);
                if (s == null) {
                    s = "";
                }
                fieldValue.setValue(s);
                break;
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
                fieldValue.setValue(result.getBoolean(index));
                break;
            case java.sql.Types.VARCHAR:
            default:
                String str = result.getString(index);
                if (str == null) {
                    str = "";
                }
                fieldValue.setValue(str);
                break;
        }
    }

}