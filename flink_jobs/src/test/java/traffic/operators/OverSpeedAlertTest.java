package traffic.operators;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import traffic.model.OverSpeedEventM;
import traffic.model.SpeedDistanceEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OverSpeedAlertTest {

    @Test
    public void doesNotEmitAlertBelowSpeedLimit() throws Exception {
        OverSpeedAlert operator = new OverSpeedAlert();
        List<OverSpeedEventM> output = new ArrayList<>();

        SpeedDistanceEvent input = new SpeedDistanceEvent(
                1,
                49.9,
                10.0,
                "2008-02-02T10:00:00"
        );

        operator.flatMap(input, collectTo(output));

        assertEquals(0, output.size());
    }

    @Test
    public void doesNotEmitAlertAtExactSpeedLimit() throws Exception {
        OverSpeedAlert operator = new OverSpeedAlert();
        List<OverSpeedEventM> output = new ArrayList<>();

        SpeedDistanceEvent input = new SpeedDistanceEvent(
                1,
                50.0,
                10.0,
                "2008-02-02T10:00:00"
        );

        operator.flatMap(input, collectTo(output));

        assertEquals(0, output.size());
    }

    @Test
    public void emitsAlertAboveSpeedLimit() throws Exception {
        OverSpeedAlert operator = new OverSpeedAlert();
        List<OverSpeedEventM> output = new ArrayList<>();

        SpeedDistanceEvent input = new SpeedDistanceEvent(
                7,
                85.2,
                18.3,
                "2008-02-02T10:05:00"
        );

        operator.flatMap(input, collectTo(output));

        assertEquals(1, output.size());
        assertEquals(7, output.get(0).taxiId);
        assertEquals("2008-02-02T10:05:00", output.get(0).timestamp);
        assertEquals(85.2, output.get(0).speedKmh, 0.01);
        assertEquals("OVERSPEED_50KMH", output.get(0).alertType);
    }

    private Collector<OverSpeedEventM> collectTo(List<OverSpeedEventM> output) {
        return new Collector<OverSpeedEventM>() {
            @Override
            public void collect(OverSpeedEventM record) {
                output.add(record);
            }

            @Override
            public void close() {
            }
        };
    }
}