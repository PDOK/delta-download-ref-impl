package nl.pdok.delta.download;

import io.reactivex.rxjava3.observers.TestObserver;
import nl.pdok.delta.download.process.DeltaDownloadService;
import nl.pdok.delta.download.process.DownloadService;
import nl.pdok.delta.download.process.database.DatabaseFacade;
import nl.pdok.delta.download.process.database.MemoryProviderImpl;
import nl.pdok.delta.download.process.model.MutationMessage;
import nl.pdok.delta.download.process.database.ConnectionProvider;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

class SynchronizeDatabaseTest {

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    public void initDB(Connection conn) {
        try (
                Reader reader = new InputStreamReader(this.getClass().getResourceAsStream("/database.sql"))
        ) {

            LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            conn.setAutoCommit(false);
            String command = "";

            //conn.createStatement().execute("drop table public.mutation_message cascade;");
            //conn.createStatement().execute("drop table public.data cascade;");

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
    void main() throws IOException, SQLException {

        ConnectionProvider cp = new MemoryProviderImpl("localhost", 5432, "initialdb" + System.currentTimeMillis(), "pdok", "pdok");

        Connection connection = cp.getConnection();
        initDB(connection);

        DatabaseFacade df = new DatabaseFacade(cp);


        MockWebServer server = new MockWebServer();

        // call 1, initial does not call remote delta service
        File file = new File("src/test/resources/somedelta.zip");// we threat the delta as an initial
        Buffer buffer = new Buffer();
        server.enqueue(new MockResponse().setBody(buffer.readFrom(new FileInputStream(file))));

        // Start the server.
        server.start();


        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl deltaUrl = server.url("/kadastralekaart/api/v4_0/delta");
        DeltaDownloadService dds = new DeltaDownloadService(deltaUrl.toString());

        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl downloadUrl = server.url("/kadastralekaart/api/v4_0/delta/predefined/dkk-gml-nl.zip");
        DownloadService ds = new DownloadService(downloadUrl.toString());

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

        // execute
        SynchronizeDatabase.synchronize(df,dds,ds);

        TestObserver<MutationMessage> omm = new TestObserver<>();
        df.findLatestMutationMessage().subscribe(omm);

        omm.assertComplete();
        omm.assertNoErrors();
        omm.assertValue(mm-> "delta".equals(mm.getType()));

        server.shutdown();

    }
}