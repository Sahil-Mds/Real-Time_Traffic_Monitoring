package traffic.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaxiLocation {

    @JsonProperty("taxi_id")
    public int taxiId;
    public String timestamp;
    public double latitude;
    public double longitude;
    public String type;

    public TaxiLocation() {}

    public TaxiLocation(int taxiId, String timestamp, double latitude, double longitude, String type) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.type = type;
    }

    @Override
    public String toString() {
        return "TaxiLocation{taxiId=" + taxiId + ", timestamp=" + timestamp + ", lat=" + latitude + ", lon=" + longitude + "}";
    }
}
