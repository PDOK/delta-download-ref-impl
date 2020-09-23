package nl.pdok.delta.download.process.database;

import org.h2.jdbcx.JdbcDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;


public class MemoryProviderImpl implements ConnectionProvider {

    private final JdbcDataSource ds;

    public MemoryProviderImpl(String host, Integer port, String database, String user, String password) {
        try {
            File file = File.createTempFile("database_", ".db");

            System.out.println(file.getAbsolutePath());

            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:" + database + ";DB_CLOSE_DELAY=-1");// + file.getAbsolutePath());
            ds.setUser(user);
            ds.setPassword(password);

            this.ds = ds;

        } catch (IOException e) {
            throw new DatabaseFacade.DatabaseException(e);
        }

    }

    @Override
    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
