package traffic.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import traffic.model.OverSpeedEventM;
import traffic.model.SpeedDistanceEvent;

public class OverSpeedAlert implements FlatMapFunction<SpeedDistanceEvent, OverSpeedEventM> {

    private static final double SPEED_LIMIT_KMH = 50.0;

    @Override
    public void flatMap(SpeedDistanceEvent event, Collector<OverSpeedEventM> out) {
        if (event.speedKmh > SPEED_LIMIT_KMH) {
            out.collect(new OverSpeedEventM(
                    event.taxiId,
                    event.timestamp,
                    event.speedKmh,
                    "OVERSPEED_50KMH"
            ));
        }
    }
}