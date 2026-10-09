package traffic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import traffic.model.AreaViolationEventM;
import traffic.model.FleetMetricsEvent;
import traffic.model.OverSpeedEventM;
import traffic.model.StateUpdateEvent;
import traffic.model.TaxiLocation;

import static org.junit.jupiter.api.Assertions.*;

public class OutputTopicSerializationTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testTaxiLocationToLocationsTopic() throws Exception {
        TaxiLocation location = new TaxiLocation(42, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");

        String json = mapper.writeValueAsString(location);
        JsonNode node = mapper.readTree(json);

        assertTrue(node.has("taxi_id"));
        assertFalse(node.has("taxiId"));
        assertEquals(42, node.get("taxi_id").asInt());
        assertEquals(39.9163, node.get("latitude").asDouble(), 0.0001);

        TaxiLocation roundTrip = mapper.readValue(json, TaxiLocation.class);
        assertEquals(42, roundTrip.taxiId);
    }

    @Test
    public void testStateUpdateToStateUpdatesTopic() throws Exception {
        String json = mapper.writeValueAsString(
            new StateUpdateEvent(7, 42.5, 18.3, "2008-02-02T10:00:00"));
        JsonNode node = mapper.readTree(json);

        assertEquals(7, node.get("taxiId").asInt());
        assertEquals(42.5, node.get("avgSpeedKmh").asDouble(), 0.01);
        assertEquals(18.3, node.get("totalDistanceKm").asDouble(), 0.01);
    }

    @Test
    public void testFleetMetricsToFleetMetricsTopic() throws Exception {
        String json = mapper.writeValueAsString(
            new FleetMetricsEvent(12, 540.0, "2008-02-02T10:00:00"));
        JsonNode node = mapper.readTree(json);

        assertEquals(12, node.get("activeTaxis").asInt());
        assertEquals(540.0, node.get("totalDistanceKm").asDouble(), 0.01);
    }

    @Test
    public void testAreaViolationToViolationEventsTopic() throws Exception {
        String json = mapper.writeValueAsString(
            new AreaViolationEventM(5, "2008-02-02T10:00:00", 39.95, 116.40, "DISCARD_15KM"));
        JsonNode node = mapper.readTree(json);

        assertEquals(5, node.get("taxiId").asInt());
        assertEquals("DISCARD_15KM", node.get("violationType").asText());
    }

    @Test
    public void testOverSpeedToViolationEventsTopic() throws Exception {
        String json = mapper.writeValueAsString(
            new OverSpeedEventM(5, "2008-02-02T10:00:00", 85.2, "OVERSPEED_50KMH"));
        JsonNode node = mapper.readTree(json);

        assertEquals(5, node.get("taxiId").asInt());
        assertEquals(85.2, node.get("speedKmh").asDouble(), 0.01);
        assertEquals("OVERSPEED_50KMH", node.get("alertType").asText());
    }
}
