package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.core.Observable;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import nl.pdok.delta.download.process.model.Delta;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Response;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.reactivex.rxjava3.core.Observable.error;
import static io.reactivex.rxjava3.core.Observable.just;
import static nl.pdok.delta.download.util.HttpUtil.getUnsafeOkHttpClient;

public class DownloadService {
    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    public static final int UPDATE_INTERVAL_MILLIS = 1000;

    private final Logger logger = Logger.getLogger(this.getClass().getName());


    private final String downloadUrl;

    public DownloadService(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    public Observable<File> downloadFile(Delta delta) {

        String outputFile = "initial";

        HttpUrl.Builder deltaUrlBuilder = HttpUrl.parse(this.downloadUrl).newBuilder()
                .addQueryParameter("count", "1000");

        if (delta.getDeliveryId() != null) {
            outputFile = delta.getDeliveryId();
            deltaUrlBuilder.addQueryParameter("delta-id", delta.getDeliveryId());
        }

        try (Response response = getUnsafeOkHttpClient()
                .newCall(new Request.Builder().url(deltaUrlBuilder.build()).build()).execute()) {

            File file = File.createTempFile(outputFile + "_", ".zip");

            if (response.code() != 200) {
                throw new IOException("Download File, Http Exception status : " + response.code());
            }

            try (InputStream inputStream = response.body().byteStream();
                 ProgressBar pb = new ProgressBarBuilder()
                         .setStyle(ProgressBarStyle.COLORFUL_UNICODE_BLOCK)
                         .setTaskName("Downloading : " + outputFile + " ")
                         .showSpeed()
                         .setUnit("MB", MB)
                         .setUpdateIntervalMillis(UPDATE_INTERVAL_MILLIS)
                         .setInitialMax(response.body().contentLength())
                         .build()) {

                byte[] chunk = new byte[100 * KB];
                int read;
                try (FileOutputStream fos = new FileOutputStream(file, false)) {
                    while ((read = inputStream.read(chunk)) != -1) {
                        pb.stepBy(read);
                        fos.write(chunk, 0, read);
                    }
                }
            }

            return just(file);
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e.getCause());
            return error(new DownloadServiceException(e));
        }
    }

    static class DownloadServiceException extends RuntimeException {
        private final Exception original;

        public DownloadServiceException(Exception e) {
            original = e;
        }

        public String toString() {
           return original.toString();
        }
    }
}

