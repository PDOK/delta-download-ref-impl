package nl.pdok.delta.download.process;


import io.reactivex.rxjava3.core.Emitter;
import io.reactivex.rxjava3.core.Flowable;
import nl.pdok.delta.download.process.model.MutationData;
import nl.pdok.delta.download.process.model.MutationGroup;
import nl.pdok.delta.download.process.model.MutationMessage;
import nl.pdok.delta.download.util.XmlReaderUtil;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.sql.Timestamp;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.reactivex.rxjava3.core.Flowable.generate;


public class ZipFile2Stream {
    // ZipFileStream has state
    // Mutation Message does not have counts, processing MutatieGroep in a streaming fashion with progress is not efficient if applied
    // XML MutationMessage contains both zipped entry feature ObjectType, which is not te case. 1 ZipEntry -> 1 ObjectType
    // XML MutationGroep has no generic information about object/feature type and cannot be used standalone in a generic fashion,
    // generic deduction of feature type becomes cumbersome and non stateless.
    private String zipentryName;
    private String currentObjectType;
    private MutationMessage mutationMessage;

    ZipInputStream zipInputStream(File zipFile) throws IOException {
        return new ZipInputStream(new BufferedInputStream(new FileInputStream(zipFile)));
    }

    public Flowable<ZipInputStream> toZipFlowable(File zipfile) {
        return generate(
                () -> zipInputStream(zipfile),
                this::pushNextInputStream,
                ZipInputStream::close);
    }

    public Flowable<MutationGroup> toFlowableMutatie(InputStream inputStream) {
        return generate(
                () -> xmlStreamReader(inputStream),
                this::pushNextMutatieGroep,
                XMLStreamReader::close);
    }

    private void pushNextInputStream(ZipInputStream zis, Emitter<ZipInputStream> emitter) throws IOException {
        ZipEntry ze = zis.getNextEntry();
        if (ze != null) {
            // keeping state to deduce feature type
            this.zipentryName = ze.getName();
            emitter.onNext(zis);

        } else {
            emitter.onComplete();
        }
    }

    private XMLStreamReader xmlStreamReader(InputStream inputStream) throws Exception {
        XMLStreamReader xmlStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
        // each zip entry must has a mutation message containing objectTypes
        this.mutationMessage = extractMutatieBericht(xmlStreamReader);
        assert mutationMessage != null;
        this.currentObjectType = mutationMessage.getCurrentObjectType(zipentryName);
        return xmlStreamReader;
    }


    private void pushNextMutatieGroep(XMLStreamReader streamReader, Emitter<MutationGroup> emitter) throws Exception {
        final MutationGroup mutationGroup = emitMutatieGroep(streamReader, currentObjectType);
        if (mutationGroup != null) {
            emitter.onNext(mutationGroup);
        } else {
            emitter.onComplete();
        }
    }

    private MutationMessage extractMutatieBericht(XMLStreamReader streamReader) throws Exception {
        while (streamReader.hasNext()) {
            int event = streamReader.next();
            if (event == XMLStreamReader.START_ELEMENT) {
                String localName = streamReader.getLocalName();
                if ("mutatieBericht".equals(localName)) {
                    return new MutationMessageVisitor().visit(streamReader);
                }
            }
        }
        return null;
    }

    private MutationGroup emitMutatieGroep(XMLStreamReader streamReader, String currentObjectType) throws Exception {

        MutationGroup mg = null;

        while (streamReader.hasNext()) {
            int event = streamReader.next();
            switch (event) {
                case XMLStreamReader.START_ELEMENT: {
                    String localName = streamReader.getLocalName();
                    if ("mutatieGroep".equals(localName)) {
                        mg = new MutationGroup();
                        mg.objectType = currentObjectType;
                        setMutatie(streamReader, mg);
                    }
                    break;
                }
                case XMLStreamReader.END_ELEMENT: {
                    String localName = streamReader.getLocalName();
                    if ("mutatieGroep".equals(localName)) {
                        return mg;
                    } else if ("mutatieBericht".equals(localName)) { // EOF
                        return null;
                    }

                    break;
                }
                default: // NOP
            }
        }
        return null;
    }

    private void setMutatie(XMLStreamReader streamReader, MutationGroup mg) throws Exception {

        MutationData mutationData = null;
        while (streamReader.hasNext()) {
            int event = streamReader.next();
            switch (event) {
                case XMLStreamReader.START_ELEMENT: {

                    String localName = streamReader.getLocalName();
                    if ("was".equals(localName)) {
                        mutationData = new MutationData();
                        mutationData.id = streamReader.getAttributeValue(0);
                        mutationData.data = XmlReaderUtil.getTagContentAsXML(streamReader);

                    } else if ("wordt".equals(localName)) {
                        mutationData = new MutationData();
                        mutationData.id = streamReader.getAttributeValue(0);
                        mutationData.data = XmlReaderUtil.getTagContentAsXML(streamReader);
                    }
                    break;
                }
                case XMLStreamReader.END_ELEMENT: {
                    String localName = streamReader.getLocalName();
                    if ("was".equals(localName)) {
                        mg.was = mutationData;
                    } else if ("wordt".equals(localName)) {
                        mg.wordt = mutationData;
                    } // end of mutations
                    else if ("toevoeging".equals(localName)) {
                        return;
                    } else if ("wijziging".equals(localName)) {
                        return;
                    } else if ("verwijdering".equals(localName)) {
                        return;
                    }
                    break;
                }
                default: // NOP
            }
        }
    }


    public MutationMessage getMutationMessage() {
        return this.mutationMessage;
    }

    public static class MutationMessageVisitor {

        private final MutationMessage mutationMessage;

        MutationMessageVisitor() {
            mutationMessage = new MutationMessage();
            mutationMessage.setTs(new Timestamp(System.currentTimeMillis()));
        }

        public MutationMessage visit(XMLStreamReader streamReader) throws Exception {
            while (streamReader.hasNext()) {
                int event = streamReader.next();
                switch (event) {
                    case XMLStreamReader.START_ELEMENT: {
                        String localName = streamReader.getLocalName();
                        if ("dataset".equals(localName)) {
                            this.mutationMessage.setDataset(streamReader.getElementText());
                        } else if ("mutatieType".equals(localName)) {
                            this.mutationMessage.setType(streamReader.getElementText());
                        } else if ("leveringsId".equals(localName)) {
                            this.mutationMessage.setId(streamReader.getElementText());
                        } else if ("objectType".equals(localName)) {
                            this.mutationMessage.add(streamReader.getElementText());
                        }
                        break;
                    }
                    case XMLStreamReader.END_ELEMENT: {
                        String localName = streamReader.getLocalName();
                        if ("inhoud".equals(localName)) {
                            return this.mutationMessage;
                        }
                        break;
                    }
                    default: // NOP
                }
            }

            return this.mutationMessage;

        }
    }


}
