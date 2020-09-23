package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.observers.TestObserver;
import nl.pdok.delta.download.process.model.Delta;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class DeltaServiceTest {


    @Test
    void findRemainingDeliveries() throws IOException {

        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setBody("{\n" +
                "    \"deltas\": [\n" +
                "        {\n" +
                "            \"id\": \"a87da165-9109-4b0b-bbb8-371e1d1ca1c0\",\n" +
                "            \"timeWindow\": {\n" +
                "                \"from\": \"2020-09-13T21:59:59Z\",\n" +
                "                \"to\": \"2020-09-14T21:59:59Z\"\n" +
                "            }\n" +
                "        }\n" +
                "    ],\n" +
                "    \"_links\": []\n" +
                "}"));


        //server.enqueue(new MockResponse().);

        // Start the server.
        server.start();

        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl deltaUrl = server.url("/kadastralekaart/api/v4_0/delta");
        DeltaDownloadService ds = new DeltaDownloadService(deltaUrl.toString());


        // execute
        TestObserver<Delta> fileObserver = new TestObserver<>();
        ds.findRemainingDeltas("random deliveryId").subscribe(fileObserver);

        fileObserver.assertNoErrors();
        fileObserver.assertComplete();
        fileObserver.assertValue(delta -> delta.getDeliveryId().equals("a87da165-9109-4b0b-bbb8-371e1d1ca1c0"));


        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }

    @Test
    void findRemainingDeliveries_error() throws IOException {

        MockWebServer server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(500));


        //server.enqueue(new MockResponse().);

        // Start the server.
        server.start();

        // Ask the server for its URL. You'll need this to make HTTP requests.
        HttpUrl deltaUrl = server.url("/kadastralekaart/api/v4_0/delta");
        DeltaDownloadService ds = new DeltaDownloadService(deltaUrl.toString());

        // execute
        TestObserver<Delta> fileObserver = new TestObserver<>();
        ds.findRemainingDeltas("random deliveryId").subscribe(fileObserver);

        fileObserver.assertError(DeltaDownloadService.DeltaServiceException.class);

        // Shut down the server. Instances cannot be reused.
        server.shutdown();
    }

}