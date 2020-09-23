package nl.pdok.delta.download.process.database;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class PGConnectionProviderImpl implements ConnectionProvider {

    private final PGConnectionPoolDataSource ds;

    public PGConnectionProviderImpl(String host, Integer port, String database, String user, String password) {
        PGConnectionPoolDataSource ds = new PGConnectionPoolDataSource();
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setDatabaseName(database);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setDefaultAutoCommit(false);
        ds.setApplicationName("PDOK Mutatie verwerker");

        this.ds = ds;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.ds.getConnection();
    }
}
