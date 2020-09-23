package nl.pdok.delta.download.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Observable;
import nl.pdok.delta.download.process.model.Delta;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static io.reactivex.rxjava3.core.Observable.fromIterable;
import static io.reactivex.rxjava3.core.Observable.just;
import static nl.pdok.delta.download.util.HttpUtil.getUnsafeOkHttpClient;

public class DeltaDownloadService {
    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    public static final int UPDATE_INTERVAL_MILLIS = 1000;

    private final Logger logger = Logger.getLogger(this.getClass().getName());


    private final String downloadUrl;

    public DeltaDownloadService(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    // may not be used for Initial Download
    public Observable<Delta> findRemainingDeltas(String afterDeltaId) {

        assert afterDeltaId != null;

        if (afterDeltaId.equals("initial")) return just(new Delta(afterDeltaId, null,null));

        List<Delta> remainingDeltas = new ArrayList<>();

        HttpUrl deltaUrl = HttpUrl.parse(this.downloadUrl).newBuilder()
                .addQueryParameter("count", "1000")
                .addQueryParameter("after-delta-id", afterDeltaId).build();

        try (Response response = getUnsafeOkHttpClient()
                .newCall(new Request.Builder().url(deltaUrl).build()).execute()) {

            if (response.code() != 200) {
                throw new IOException("Remaining Delta's, Http Exception status : " + response.code());
            }

            ObjectMapper mapper = new ObjectMapper();
            JsonNode actualObj = mapper.readTree(response.body().string());
            JsonNode deltas = actualObj.get("deltas");
            if (deltas.isArray()) {
                logger.info(String.format("Database is %d downloads behind current state", deltas.size()));
                deltas.forEach(delta -> {
                    String deliveryId = delta.get("id").asText();
                    JsonNode timeWindow = delta.get("timeWindow");
                    String from = timeWindow.get("from").asText("--infinity");
                    String to = timeWindow.get("to").asText();
                    remainingDeltas.add(new Delta(deliveryId, from, to));
                });
            }

        } catch (Exception ex) {
            return Observable.error(new DeltaServiceException(ex));
        }
        return fromIterable(remainingDeltas);
    }

    static class DeltaServiceException extends RuntimeException {
        private final Exception original;

        public DeltaServiceException(Exception e) {
            original = e;
        }

        public String toString() {
            return original.toString();
        }
    }

}

