package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.observers.TestObserver;
import nl.pdok.delta.download.process.model.Delta;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

class DownloadServiceTest {


    @Test
    void downloadDelte_Ok() throws IOException {

        File file = new File("src/test/resources/somedelta.zip");

        MockWebServer server = new MockWebServer();
        Buffer buffer = new Buffer();
        server.enqueue(new MockResponse().setBody(buffer.readFrom(new FileInputStream(file))));


        // Start the server.
        server.start();

        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl deltaUrl = server.url("/kadastralekaart/api/v4_0/delta/predefined/dkk-gml-nl.zip");
        DownloadService ds = new DownloadService(deltaUrl.toString());

        // execute
        TestObserver<File> fileObserver = new TestObserver<>();
        ds.downloadFile(new Delta("initial", null, null))
                .subscribe(fileObserver);

        fileObserver.assertNoErrors();
        fileObserver.assertComplete();
        fileObserver.assertValue(tempDownloadedFile -> tempDownloadedFile.exists());

        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }

    @Test
    void downloadDelta_Nok() throws IOException {

        File file = new File("src/test/resources/somedelta.zip");

        MockWebServer server = new MockWebServer();
        Buffer buffer = new Buffer();
        server.enqueue(new MockResponse().setBody(buffer.readFrom(new FileInputStream(file))).setResponseCode(500));


        // Start the server.
        server.start();

        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl deltaUrl = server.url("/kadastralekaart/api/v4_0/delta/predefined/dkk-gml-nl.zip");
        DownloadService ds = new DownloadService(deltaUrl.toString());

        // execute
        TestObserver<File> fileObserver = new TestObserver<>();
        ds.downloadFile(new Delta("initial", null, null))
                .subscribe(fileObserver);

        fileObserver.assertError(DownloadService.DownloadServiceException.class);

        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }




}