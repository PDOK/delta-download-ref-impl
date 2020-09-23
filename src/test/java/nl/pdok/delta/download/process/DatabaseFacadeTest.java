package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import nl.pdok.delta.download.process.database.ConnectionProvider;
import nl.pdok.delta.download.process.database.DatabaseFacade;
import nl.pdok.delta.download.process.database.MemoryProviderImpl;
import nl.pdok.delta.download.process.model.MutationData;
import nl.pdok.delta.download.process.model.MutationGroup;
import nl.pdok.delta.download.process.model.MutationMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

class DatabaseFacadeTest {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    public void initDB(Connection conn) {
        try (
                Reader reader = new InputStreamReader(this.getClass().getResourceAsStream("/database.sql"))
        ) {

            LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            conn.setAutoCommit(false);
            String command = "";

            while ((line = lineReader.readLine()) != null) {
                if (line.endsWith(";")) {
                    command = command + line + "\n";
                    conn.createStatement().execute(command);
                    command = "";
                } else {
                    command = command + line + "\n";
                }

            }

            conn.commit();

        } catch (SQLException | IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e.getCause());
        }

    }


    @Test
    void connectPostgres() throws SQLException {

        //ConnectionProvider cp = new PGConnecti0onProviderImpl("localhost", 5432, "postgres", "pdok", "pdok");
        ConnectionProvider cp = new MemoryProviderImpl("localhost", 5432, "postgres" + System.currentTimeMillis(), "pdok", "pdok");
        DatabaseFacade mpr = new DatabaseFacade(cp);

        Connection connection = cp.getConnection();
        initDB(connection);

        MutationMessage mutationMessage = new MutationMessage();
        mutationMessage.setId(UUID.randomUUID().toString());
        mutationMessage.setDataset("kadastralekaartv4");
        mutationMessage.setType("initial");
        mutationMessage.setTs(new Timestamp(System.currentTimeMillis()));

        TestObserver<MutationMessage> observer = new TestObserver<>();
        Observable<MutationMessage> aLong = mpr.insertMutationMessage(mutationMessage)
                .flatMap(x -> mpr.findLatestMutationMessage()).toObservable();

        aLong.subscribe(observer);

        observer.assertComplete();
        observer.assertNoErrors();
        observer.assertValue(mdb -> mdb.getId().equals(mutationMessage.getId()));
        observer.assertValue(mdb -> mdb.getType().equals(mutationMessage.getType()));


        String data = "<data>inhoud</data>";
        List<MutationGroup> mgrs = new ArrayList<>();

        MutationGroup mgInitial = new MutationGroup();
        mgInitial.wordt = MutationData.MutatieDataBuilder.aMutatie().withId("0").withFeature("Perceel").withData(data).build();
        mgrs.add(mgInitial);

        for (int i = 1; i < 100; i++) {
            MutationGroup mg = new MutationGroup();
            mg.was = mgInitial.wordt;
            mg.wordt = MutationData.MutatieDataBuilder.aMutatie().withId("" + i).withFeature("Perceel").withData(data).build();
            mgrs.add(mg);
            mgInitial = mg;
        }

        // should result in one record
        TestObserver<Integer> mutationGroepObserver = new TestObserver<>();

        Observable<Integer> insertMutationGroups = mpr.insertMutationGroups(mgrs);
        insertMutationGroups.subscribe(mutationGroepObserver);

        mutationGroepObserver.assertValue(100);

    }
}