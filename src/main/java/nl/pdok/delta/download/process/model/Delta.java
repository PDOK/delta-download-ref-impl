package nl.pdok.delta.download.process.model;

public class Delta {
    private final String deliveryId;
    private final String from;
    private final String to;

    public Delta(String deliveryId, String from, String to) {
        this.deliveryId = deliveryId;
        this.from = from;
        this.to = to;
    }

    public String getDeliveryId() {
        return deliveryId;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    @Override
    public String toString() {
        return "Delta{" +
                "deliveryId='" + deliveryId + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }

    public static Delta empty(){
        return new Delta(null,null,null);
    }
}
