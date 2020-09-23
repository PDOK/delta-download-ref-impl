package nl.pdok.delta.download.process.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class MutationMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String id; // String // Id or Primary Key

    private String type;  // String

    private String dataset;  // String

    private Timestamp ts;  // Date

    private transient List<String> objectTypen = new ArrayList<>();

    public MutationMessage() {
        super();
    }

    public MutationMessage(String id, String type, String dataset, Timestamp ts) {
        this.id = id;
        this.type = type;
        this.dataset = dataset;
        this.ts = ts;
    }

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

    //----------------------------------------------------------------------
    // GETTER(S) & SETTER(S) FOR ID OR PRIMARY KEY
    //----------------------------------------------------------------------

    /**
     * Set the "id" field value
     * This field is mapped on the database column "id" ( type "", NotNull : true )
     *
     * @param id
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get the "id" field value
     * This field is mapped on the database column "id" ( type "", NotNull : true )
     *
     * @return the field value
     */
    public String getId() {
        return this.id;
    }

    //----------------------------------------------------------------------
    // GETTER(S) & SETTER(S) FOR OTHER DATA FIELDS
    //----------------------------------------------------------------------

    /**
     * Set the "type" field value
     * This field is mapped on the database column "type" ( type "", NotNull : false )
     *
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Get the "type" field value
     * This field is mapped on the database column "type" ( type "", NotNull : false )
     *
     * @return the field value
     */
    public String getType() {
        return this.type;
    }

    /**
     * Set the "dataset" field value
     * This field is mapped on the database column "dataset" ( type "", NotNull : false )
     *
     * @param dataset
     */
    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    /**
     * Get the "dataset" field value
     * This field is mapped on the database column "dataset" ( type "", NotNull : false )
     *
     * @return the field value
     */
    public String getDataset() {
        return this.dataset;
    }

    /**
     * Set the "ts" field value
     * This field is mapped on the database column "ts" ( type "", NotNull : false )
     *
     * @param ts
     */
    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    /**
     * Get the "ts" field value
     * This field is mapped on the database column "ts" ( type "", NotNull : false )
     *
     * @return the field value
     */
    public Timestamp getTs() {
        return this.ts;
    }

    //----------------------------------------------------------------------
    // toString METHOD
    //----------------------------------------------------------------------
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(id);
        sb.append("|");
        sb.append(type);
        sb.append("|");
        sb.append(dataset);
        sb.append("|");
        sb.append(ts);
        return sb.toString();
    }

}
