package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

class ZipFile2StreamTest {

    @Test
    void readZipfile() {

        File file = new File("src/test/resources/somedelta.zip");

        Zipfile2Stream f2s = new Zipfile2Stream();

        TestObserver<Integer> zipObserver = new TestObserver<>();

        Single<Integer> collect = f2s.toZipFlowable(file)
                .flatMap(f2s::toFlowableMutatie)
                .buffer(10000)
                .map(List::size)
                .collect(Collectors.summingInt(Integer::intValue))
                .doOnSuccess(numberOfMutationGroups-> System.out.println("Mutation Records Processed : " + numberOfMutationGroups));

        collect.subscribe(zipObserver);

        zipObserver.assertNoErrors();
        zipObserver.assertComplete();
        zipObserver.assertValue(30411);
    }


}