package nl.pdok.delta.download.process;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.stream.Collectors;

class ZipFile2StreamTest {

    @Test
    void readZipfile() {

        File file = new File("src/test/resources/somedelta.zip");

        ZipFile2Stream f2s = new ZipFile2Stream();

        TestObserver<Integer> zipObserver = new TestObserver<>();

        Single<Integer> collect = f2s.toZipFlowable(file)
                .flatMap(f2s::toFlowableMutatie)
                .buffer(10000)
                .map(x -> x.size())
                .collect(Collectors.summingInt(Integer::intValue))
                .doOnSuccess(numberOfMutationGroups-> System.out.println("Mutation Records Processed : " + numberOfMutationGroups));

        collect.subscribe(zipObserver);

        zipObserver.assertNoErrors();
        zipObserver.assertComplete();
        zipObserver.assertValue(30411);
    }


}