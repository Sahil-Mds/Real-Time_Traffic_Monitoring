package traffic.model;

public class OverSpeedEventM {

    public int taxiId;
    public String timestamp;
    public double speedKmh;
    public String alertType;

    public OverSpeedEventM() {
    }

    public OverSpeedEventM(int taxiId, String timestamp, double speedKmh, String alertType) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.speedKmh = speedKmh;
        this.alertType = alertType;
    }

    @Override
    public String toString() {
        return "OverSpeedEventM{" +
                "taxiId=" + taxiId +
                ", speedKmh=" + String.format("%.2f", speedKmh) +
                ", timestamp='" + timestamp + '\'' +
                ", alertType='" + alertType + '\'' +
                '}';
    }
}