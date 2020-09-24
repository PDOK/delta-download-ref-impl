package nl.pdok.delta.download.process.database;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import nl.pdok.delta.download.process.model.MutationGroup;
import nl.pdok.delta.download.process.model.MutationMessage;

import java.sql.*;
import java.util.List;

public class DatabaseFacade {

    private final ConnectionProvider connectionProvider;

    public DatabaseFacade(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public Single<MutationMessage> findLatestMutationMessage() {
        try {
            MutationMessage latestMutation = findLatestMutation();
            return Single.just(latestMutation);
        } catch (SQLException sqlex) {
            return Single.error(new DatabaseException(sqlex));
        }

    }

    MutationMessage findLatestMutation() throws SQLException {
        MutationMessage mutationMessage = new MutationMessage();
        mutationMessage.setId("initial");
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement ps = conn.prepareStatement("select id, \"type\", dataset, ts from mutation_message order by ts desc limit 1;");
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                return populate(rs, mutationMessage);
            }

        }

        return mutationMessage;
    }

    public Single<Integer> insertMutationMessage(MutationMessage mutationMessage) {
        try {
            int val = insert(mutationMessage);
            return Single.just(val);
        } catch (SQLException sqlex) {
            return Single.error(new DatabaseException(sqlex));
        }
    }

    int insert(MutationMessage mutationMessage) throws SQLException {

        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement ps = createInsert(conn, mutationMessage)) {
            conn.setAutoCommit(false);
            int i = ps.executeUpdate();
            conn.commit();
            return i;
        }
    }

    PreparedStatement createInsert(Connection conn, MutationMessage record) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("insert into mutation_message( id, \"type\", dataset, ts ) values ( ?, ?, ?, ? );");
        ps.setString(1, record.getId());
        ps.setString(2, record.getType());
        ps.setString(3, record.getDataset());
        ps.setTimestamp(4, record.getTs());
        return ps;
    }

    MutationMessage populate(ResultSet rs, MutationMessage mutationMessage) throws
            SQLException {
        mutationMessage.setId(rs.getString("id"));
        mutationMessage.setType(rs.getString("type"));
        mutationMessage.setDataset(rs.getString("dataset"));
        mutationMessage.setTs(rs.getTimestamp("ts"));
        return mutationMessage;
    }

    public Observable<Integer> insertMutationGroups(List<MutationGroup> mgrs) {
        try {
            Integer dataRecords = insertDataRecords(mgrs);
            return Observable.just(dataRecords);
        } catch (Exception sqlex) {
            return Observable.error(new DatabaseException(sqlex));
        }
    }

    Integer insertDataRecords(List<MutationGroup> mutationList) throws Exception {
        assert mutationList != null;

        int inserted = 0;
        if (!mutationList.isEmpty()) {
            try (Connection conn = this.connectionProvider.getConnection()) {
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {

                    addBatch(stmt, mutationList);
                    stmt.executeBatch();
                    conn.commit();

                    inserted = mutationList.size();

                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                }

            }
        }
        return inserted;
    }

    private void addBatch(Statement stmt, List<MutationGroup> mutationgroups) throws SQLException {
        for (MutationGroup mutationGroup : mutationgroups) {
            if (mutationGroup.was != null) {
                stmt.addBatch("DELETE FROM data WHERE id='" + mutationGroup.was.id + "';");
            }
            if (mutationGroup.wordt != null) {
                // make the processing idempotent add delete if message was replayed
                stmt.addBatch("DELETE FROM data WHERE id='" + mutationGroup.wordt.id + "';");
                stmt.addBatch("INSERT INTO data (id, feature, \"data\") VALUES('" + mutationGroup.wordt.id + "', '" + mutationGroup.objectType + "', '" + mutationGroup.wordt.data + "');");
            }
        }
    }


    static class DatabaseException extends RuntimeException {
        private final Exception original;

        public DatabaseException(Exception e) {
            original = e;
        }

        public String toString() {
            return original.getLocalizedMessage();
        }
    }
}
