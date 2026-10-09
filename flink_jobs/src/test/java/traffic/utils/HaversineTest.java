package traffic.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class HaversineTest {

    @Test
    public void testKnownDistance() {
        double distance = Haversine.distanceKm(39.9163, 116.3972, 39.9055, 116.3976);
        assertEquals(1.2, distance, 0.1);
    }

    @Test
    public void testSamePointIsZero() {
        double distance = Haversine.distanceKm(39.9163, 116.3972, 39.9163, 116.3972);
        assertEquals(0.0, distance, 0.0001);
    }
}
