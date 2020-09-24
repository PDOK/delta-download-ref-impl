package nl.pdok.delta.download;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import nl.pdok.delta.download.process.DeltaDownloadService;
import nl.pdok.delta.download.process.DownloadService;
import nl.pdok.delta.download.process.Zipfile2Stream;
import nl.pdok.delta.download.process.database.ConnectionProvider;
import nl.pdok.delta.download.process.database.DatabaseFacade;
import nl.pdok.delta.download.process.database.PGConnectionProviderImpl;
import nl.pdok.delta.download.process.model.MutationMessage;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

public class SynchronizeDatabase {

    private static final Logger logger = Logger.getLogger("SyncDatabase");

    public static void main(String... args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

        ConnectionProvider cp = new PGConnectionProviderImpl("localhost", 5432, "postgres", "pdok", "pdok");
        DatabaseFacade df = new DatabaseFacade(cp);

        // example using the DKK Delta download Endpoints
        DeltaDownloadService deltas = new DeltaDownloadService("https://downloads.pdok.nl/kadastralekaart/api/v4_0/delta");
        DownloadService downloader = new DownloadService("https://downloads.pdok.nl/kadastralekaart/api/v4_0/delta/predefined/dkk-gml-nl.zip");

        synchronize(df, deltas, downloader);
    }

    protected static void synchronize(DatabaseFacade df, DeltaDownloadService deltas, DownloadService downloader) {
        logger.info("Starting Processing Delta's");

        Scheduler single = Schedulers.single();

        // Find Latest processed Mutation
        df.findLatestMutationMessage().subscribeOn(single)
                // need only deliveryId
                .map(MutationMessage::getId)
                // Find Delta's after last Update
                .flatMapObservable(deltas::findRemainingDeltas)
                // download missing file to tmp // shows progressbar
                .flatMap(downloader::downloadFile) //
                //  process mutations in database
                .flatMap((File infile) -> {
                    Zipfile2Stream z2s = new Zipfile2Stream();
                    return z2s.toZipFlowable(infile).toObservable()
                            // per zip Entry
                            .flatMap((ZipInputStream inputStream) -> z2s.toFlowableMutatie(inputStream).toObservable())
                            // 1000 mutation groups at a time (also commit)
                            .buffer(10000)
                            .flatMap(df::insertMutationGroups)
                            .doOnNext(result -> {
                                if (result != null) {
                                    logger.info(String.format("Processed : %d mutation groups ", result));
                                }
                            })
                            .collect(Collectors.summingInt(Integer::intValue))
                            .doOnSuccess(totalProcessed -> {
                                logger.info(String.format("Processed : %d mutation groups in total", totalProcessed));
                                if (infile.exists()) {
                                    infile.deleteOnExit();
                                }
                            })
                            .flatMap(x -> df.insertMutationMessage(z2s.getMutationMessage())).toObservable();
                })
                .doOnError(error -> {
                    logger.log(Level.SEVERE, "", error);
                    System.exit(1);
                })
                .doFinally(() -> {
                    logger.info("Finished Processing Delta's");
                    System.exit(0);
                })
                .blockingSubscribe();

    }
}
