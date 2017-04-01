package org.cataractsoftware.datasponge.writer;

import org.cataractsoftware.datasponge.DataRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Creates a simple in-memory database and ensures the data writer processes the data.
 */
@RunWith(JUnit4.class)
public class JdbcDataWriterTest {

    private static final String JDBC_URL = "jdbc:h2:mem:test";
    private static final String DB_USER = "sa";
    private static final String TEST_TYPE = "test";
    private static final String FAULTY_ID = "FAULT!";
    private static final String CREATE_DDL = "CREATE TABLE test(uuid varchar(256) not null, type varchar(100))";
    private static final String INSERT_STMT = "INSERT INTO TEST VALUES(?,?)";
    private static final String GET_STMT = "SELECT * FROM TEST WHERE UUID = ?";
    private static final String TRUNC_STMT = "TRUNCATE TABLE TEST";

    @BeforeClass
    public static void setupDb() throws Exception {
        Connection conn = getConnection();
        conn.prepareStatement(CREATE_DDL).execute();
    }

    @Before
    public void cleanup() throws SQLException {
        try (Connection conn = getConnection(); PreparedStatement stmt = conn.prepareStatement(TRUNC_STMT)) {
            stmt.execute();
        }
    }

    @Test
    public void testCanInsert() throws SQLException {
        DataWriter writer = initializeWriter();
        int count = 50;
        Set<DataRecord> records = addRecords(count, false, writer);
        writer.flushBatch();
        //now validate that the data was created.
        try (Connection conn = getConnection()) {
            for (DataRecord rec : records) {
                validateDb(rec, conn, true);
            }
        }
    }

    @Test
    public void testSingleErrorDoesNotImpactOthers() throws SQLException {
        DataWriter writer = initializeWriter();
        int count = 10;
        int badCount = 5;
        Set<DataRecord> records = addRecords(count, false, writer);
        //now add some "bad" records
        Set<DataRecord> badRecs = addRecords(badCount, true, writer);
        //and some more "good" ones
        records.addAll(addRecords(count, false, writer));
        writer.flushBatch();
        //now validate that the data was created.
        try (Connection conn = getConnection()) {
            for (DataRecord rec : records) {
                validateDb(rec, conn, true);
            }
            for (DataRecord rec : badRecs) {
                validateDb(rec, conn, false);
            }
        }
    }

    private Set<DataRecord> addRecords(int count, boolean faulty, DataWriter writer) {
        Set<DataRecord> records = new HashSet<>();
        for (int i = 0; i < count; i++) {
            DataRecord rec = new DataRecord(faulty ? FAULTY_ID : UUID.randomUUID().toString(), TEST_TYPE);
            records.add(rec);
            writer.addItem(rec);
        }
        return records;
    }

    private DataWriter initializeWriter() {
        Properties props = new Properties();
        props.put(JdbcDataWriter.JDBC_URL_PROP, JDBC_URL);
        props.put(JdbcDataWriter.DB_USER_PROP, DB_USER);
        TestJdbcDataWriter writer = new TestJdbcDataWriter();
        writer.init(props);
        return writer;
    }

    private void validateDb(DataRecord record, Connection conn, boolean shouldBeFound) {

        try (PreparedStatement stmt = conn.prepareStatement(GET_STMT)) {
            stmt.setString(1, record.getIdentifier());
            try (ResultSet rs = stmt.executeQuery()) {
                assertEquals(shouldBeFound ? "Expected record to be found in db" : "Did not expect record in db",
                        rs.next(), shouldBeFound);
            }
        } catch (SQLException e) {
            assertTrue("Could not query db", false);
        }

    }

    /**
     * Gets a connection to the test database.
     *
     * @return
     */
    private static Connection getConnection() {
        try {
            return DriverManager.getConnection(JDBC_URL, DB_USER, null);
        } catch (SQLException e) {
            throw new RuntimeException("Could not get connection for test db", e);
        }
    }


    /**
     * Sample data writer that just creates a simple row in the database.
     */
    class TestJdbcDataWriter extends JdbcDataWriter {
        @Override
        protected void processRecord(DataRecord record, Connection connection) {
            if (FAULTY_ID.equals(record.getIdentifier())) {
                throw new RuntimeException("Exception!");
            }

            try {
                PreparedStatement stmt = connection.prepareStatement(INSERT_STMT);
                int idx = 1;
                stmt.setString(idx++, record.getIdentifier());
                stmt.setString(idx++, record.getType());
                if (stmt.executeUpdate() != 1) {
                    throw new RuntimeException("Could not insert data");
                }
            } catch (Exception ex) {
                throw new RuntimeException("Could not insert", ex);
            }
        }
    }
}
