package traffic.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import traffic.model.SpeedDistanceEvent;
import traffic.model.TaxiLocation;
import traffic.utils.Haversine;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ComputeSpeedAndDistance extends RichFlatMapFunction<TaxiLocation, SpeedDistanceEvent> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private transient ValueState<TaxiLocation> lastLocation;
    private transient ValueState<Double> totalDistance;

    @Override
    public void open(Configuration config) {
        lastLocation = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastLocation", TaxiLocation.class)
        );
        totalDistance = getRuntimeContext().getState(
            new ValueStateDescriptor<>("totalDistance", Double.class)
        );
    }

    @Override
    public void flatMap(TaxiLocation current, Collector<SpeedDistanceEvent> out) throws Exception {
        if ("END".equals(current.type)) {
            lastLocation.clear();
            totalDistance.clear();
            return;
        }

        TaxiLocation prev = lastLocation.value();
        lastLocation.update(current);

        if (prev == null) return;

        double segmentKm = Haversine.distanceKm(
            prev.latitude, prev.longitude,
            current.latitude, current.longitude
        );

        double total = totalDistance.value() == null ? 0.0 : totalDistance.value();
        total += segmentKm;
        totalDistance.update(total);

        double hours = Duration.between(
            LocalDateTime.parse(prev.timestamp, FORMATTER),
            LocalDateTime.parse(current.timestamp, FORMATTER)
        ).toSeconds() / 3600.0;

        if (hours <= 0) return;

        out.collect(new SpeedDistanceEvent(current.taxiId, segmentKm / hours, total, current.timestamp));
    }
}
