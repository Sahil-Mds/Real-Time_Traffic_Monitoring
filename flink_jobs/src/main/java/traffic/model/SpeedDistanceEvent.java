package traffic.model;

public class SpeedDistanceEvent {

    public int taxiId;
    public double speedKmh;
    public double totalDistanceKm;
    public String timestamp;

    public SpeedDistanceEvent() {}

    public SpeedDistanceEvent(int taxiId, double speedKmh, double totalDistanceKm, String timestamp) {
        this.taxiId = taxiId;
        this.speedKmh = speedKmh;
        this.totalDistanceKm = totalDistanceKm;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SpeedDistanceEvent{taxiId=" + taxiId
                + ", speed=" + String.format("%.2f", speedKmh) + " km/h"
                + ", totalDistance=" + String.format("%.3f", totalDistanceKm) + " km"
                + ", timestamp=" + timestamp + "}";
    }
}
