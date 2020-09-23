package nl.pdok.delta.download.process.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class MutationMessage {

    private String id; // String // Id or Primary Key

    private String type;  // String

    private String dataset;  // String

    private Timestamp ts;  // Date

    private final transient List<String> objectTypen = new ArrayList<>();

    public void add(String objectType) {
        this.objectTypen.add(objectType.toLowerCase());
    }

    public String getCurrentObjectType(String gzipEntry) {
        for (String objectType : objectTypen) {
            if (gzipEntry.toLowerCase().contains(objectType)) {
                return objectType;
            }
        }
        return "unknown";
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getId() {
        return this.id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getDataset() {
        return this.dataset;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Timestamp getTs() {
        return this.ts;
    }

    @Override
    public String toString() {
        return id +
                "|" +
                type +
                "|" +
                dataset +
                "|" +
                ts;
    }
}
