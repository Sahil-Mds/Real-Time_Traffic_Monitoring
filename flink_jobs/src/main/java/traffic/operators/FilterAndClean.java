package traffic.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import traffic.model.TaxiLocation;
import traffic.utils.Haversine;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FilterAndClean extends RichFlatMapFunction<TaxiLocation, TaxiLocation> {

    private static final double MAX_SPEED_KMH = 200.0;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private transient ValueState<TaxiLocation> lastClean;

    @Override
    public void open(Configuration config) {
        lastClean = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastClean", TaxiLocation.class)
        );
    }

    @Override
    public void flatMap(TaxiLocation current, Collector<TaxiLocation> out) throws Exception {
        if ("END".equals(current.type)) {
            lastClean.clear();
            out.collect(current);
            return;
        }

        if (current.latitude < -90 || current.latitude > 90
                || current.longitude < -180 || current.longitude > 180) {
            return;
        }

        TaxiLocation prev = lastClean.value();

        if (prev != null) {
            if (prev.timestamp.equals(current.timestamp)) {
                return;
            }

            double distanceKm = Haversine.distanceKm(
                prev.latitude, prev.longitude,
                current.latitude, current.longitude
            );

            if (distanceKm == 0.0) {
                return;
            }

            double hours = Duration.between(
                LocalDateTime.parse(prev.timestamp, FORMATTER),
                LocalDateTime.parse(current.timestamp, FORMATTER)
            ).toSeconds() / 3600.0;

            if (hours > 0 && distanceKm / hours > MAX_SPEED_KMH) {
                return;
            }
        }

        lastClean.update(current);
        out.collect(current);
    }
}
