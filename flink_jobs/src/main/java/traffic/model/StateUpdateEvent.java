package traffic.model;

public class StateUpdateEvent {

    public int taxiId;
    public double avgSpeedKmh;
    public double totalDistanceKm;
    public String timestamp;

    public StateUpdateEvent() {}

    public StateUpdateEvent(int taxiId, double avgSpeedKmh, double totalDistanceKm, String timestamp) {
        this.taxiId = taxiId;
        this.avgSpeedKmh = avgSpeedKmh;
        this.totalDistanceKm = totalDistanceKm;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "StateUpdateEvent{taxiId=" + taxiId
                + ", avgSpeed=" + String.format("%.2f", avgSpeedKmh) + " km/h"
                + ", totalDistance=" + String.format("%.3f", totalDistanceKm) + " km"
                + ", timestamp=" + timestamp + "}";
    }
}
