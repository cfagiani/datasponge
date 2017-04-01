package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.DataRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Datawriter that is able to insert records into one or more JDBC tables.
 * NOTE: this assumes we're using JDBC4.0+ and does NOT use a connection pool.
 */
public abstract class JdbcDataWriter extends AbstractDataWriter {

    public static final String JDBC_URL_PROP = "jdbcUrl";
    public static final String DB_USER_PROP = "jdbcUser";
    public static final String DB_PASSWORD_PROP = "jdbcPassword";

    private String jdbcUrl;
    private String user;
    private String password;

    @Override
    public void init(Properties props) {
        this.jdbcUrl = props.getProperty(JDBC_URL_PROP);
        this.user = props.getProperty(DB_USER_PROP);
        this.password = props.getProperty(DB_PASSWORD_PROP);
    }


    @Override
    protected void writeItem(DataRecord record) {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            conn.setAutoCommit(false);
            try {
                processRecord(record, conn);
                conn.commit();
            } catch (Throwable ex) {
                //TODO: log
                conn.rollback();
            }
        } catch (SQLException e) {
            //TODO: log and continue
        }

    }

    /**
     * Subclasses should implement this method and use it to build up 1 or more statements using the DataRecord.
     * Implementors should NOT close the connection nor is there a need to commit. To trigger a rollback, throw
     * implementors can throw a runtime exception.
     * <p>
     * The writeItem method will take care of closing the connection and committing/rolling back as needed.
     *
     * @param record
     * @param connection
     */
    protected abstract void processRecord(DataRecord record, Connection connection);

    @Override
    public void finish() {
        //no-op
    }
}
