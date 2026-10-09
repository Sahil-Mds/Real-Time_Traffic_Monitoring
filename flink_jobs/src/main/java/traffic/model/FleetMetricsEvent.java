package traffic.model;

public class FleetMetricsEvent {

    public int activeTaxis;
    public double totalDistanceKm;
    public String timestamp;

    public FleetMetricsEvent() {}

    public FleetMetricsEvent(int activeTaxis, double totalDistanceKm, String timestamp) {
        this.activeTaxis = activeTaxis;
        this.totalDistanceKm = totalDistanceKm;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "FleetMetricsEvent{activeTaxis=" + activeTaxis
                + ", totalDistance=" + String.format("%.3f", totalDistanceKm) + " km"
                + ", timestamp=" + timestamp + "}";
    }
}
