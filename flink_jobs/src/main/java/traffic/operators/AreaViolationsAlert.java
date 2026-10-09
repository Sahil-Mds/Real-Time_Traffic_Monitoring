package traffic.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import traffic.model.AreaViolationEventM;
import traffic.model.TaxiLocation;
import traffic.utils.Haversine;

public class AreaViolationsAlert
        extends RichFlatMapFunction<TaxiLocation, AreaViolationEventM> {

    private static final double CENTER_LAT = 39.9163;
    private static final double CENTER_LON = 116.3972;

    private transient ValueState<Boolean> warningSent;
    private transient ValueState<Boolean> discardSent;

    @Override
    public void open(Configuration parameters) {

        warningSent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("warningSent", Boolean.class));

        discardSent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("discardSent", Boolean.class));
    }

    @Override
    public void flatMap(
            TaxiLocation location,
            Collector<AreaViolationEventM> out) throws Exception {

        if ("END".equals(location.type)) {
            warningSent.clear();
            discardSent.clear();
            return;
        }

        double distanceKm =
                Haversine.distanceKm(
                        location.latitude,
                        location.longitude,
                        CENTER_LAT,
                        CENTER_LON);

        Boolean warned = warningSent.value();
        Boolean discarded = discardSent.value();

        if (warned == null) warned = false;
        if (discarded == null) discarded = false;

        if (distanceKm > 10.0 && distanceKm <= 15.0 && !warned) {

            out.collect(new AreaViolationEventM(
                    location.taxiId,
                    location.timestamp,
                    location.latitude,
                    location.longitude,
                    "WARNING_10KM"));

            warningSent.update(true);
        }

        if (distanceKm > 15.0 && !discarded) {

            out.collect(new AreaViolationEventM(
                    location.taxiId,
                    location.timestamp,
                    location.latitude,
                    location.longitude,
                    "DISCARD_15KM"));

            discardSent.update(true);
        }
    }
}