package traffic.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import traffic.model.TaxiLocation;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Emits taxi location updates to the dashboard at most once every 5 seconds per taxi.
 *
 * This reduces dashboard and Kafka load because the live map does not need every GPS point.
 * END events are always forwarded so the dashboard can remove inactive taxis.
 */
public class PropagateLocationInfo extends RichFlatMapFunction<TaxiLocation, TaxiLocation> {

    private static final long EMIT_INTERVAL_SECONDS = 5L;

    private transient ValueState<Long> lastEmittedEpochSecond;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "lastEmittedEpochSecond",
                Types.LONG
        );

        lastEmittedEpochSecond = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(TaxiLocation location, Collector<TaxiLocation> out) throws Exception {
        if (location == null) {
            return;
        }

        if ("END".equalsIgnoreCase(location.type)) {
            out.collect(location);
            lastEmittedEpochSecond.clear();
            return;
        }

        Long currentEpochSecond = parseTimestamp(location.timestamp);

        if (currentEpochSecond == null) {
            out.collect(location);
            return;
        }

        Long lastEpochSecond = lastEmittedEpochSecond.value();

        if (lastEpochSecond == null || currentEpochSecond - lastEpochSecond >= EMIT_INTERVAL_SECONDS) {
            out.collect(location);
            lastEmittedEpochSecond.update(currentEpochSecond);
        }
    }

    private Long parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return null;
        }

        try {
            return LocalDateTime.parse(timestamp).toEpochSecond(ZoneOffset.UTC);
        } catch (Exception ignored) {
            return null;
        }
    }
}