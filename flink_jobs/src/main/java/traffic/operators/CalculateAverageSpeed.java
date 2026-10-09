package traffic.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import traffic.model.AverageSpeedEvent;
import traffic.model.SpeedDistanceEvent;

public class CalculateAverageSpeed extends RichFlatMapFunction<SpeedDistanceEvent, AverageSpeedEvent> {

    private transient ValueState<Double> speedSum;
    private transient ValueState<Integer> speedCount;

    @Override
    public void open(Configuration config) {
        speedSum = getRuntimeContext().getState(new ValueStateDescriptor<>("speedSum", Types.DOUBLE));
        speedCount = getRuntimeContext().getState(new ValueStateDescriptor<>("speedCount", Types.INT));
    }

    @Override
    public void flatMap(SpeedDistanceEvent event, Collector<AverageSpeedEvent> out) throws Exception {
        double sum = speedSum.value() == null ? 0.0 : speedSum.value();
        int count = speedCount.value() == null ? 0 : speedCount.value();

        sum += event.speedKmh;
        count += 1;

        speedSum.update(sum);
        speedCount.update(count);

        out.collect(new AverageSpeedEvent(event.taxiId, sum / count, event.totalDistanceKm, event.timestamp));
    }
}
