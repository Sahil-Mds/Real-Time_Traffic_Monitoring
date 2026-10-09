package traffic.model;

public class AreaViolationEventM {

    public int taxiId;
    public String timestamp;
    public double latitude;
    public double longitude;
    public String violationType;

    public AreaViolationEventM() {
    }

    public AreaViolationEventM(
            int taxiId,
            String timestamp,
            double latitude,
            double longitude,
            String violationType) {

        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.violationType = violationType;
    }

    @Override
    public String toString() {
        return "AreaViolationEventM{" +
                "taxiId=" + taxiId +
                ", violationType='" + violationType + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}