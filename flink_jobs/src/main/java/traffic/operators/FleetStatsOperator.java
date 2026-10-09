package traffic.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

import traffic.model.FleetMetricsEvent;
import traffic.model.SpeedDistanceEvent;

/**
 * Aggregates fleet-wide metrics (active taxi count, total distance)
 * and writes them to:
 *   - Downstream stream (→ fleet-metrics Kafka topic via Job sink)
 *   - Redis key "fleet:metrics" (→ read by REST API GET /api/fleet/stats)
 */
public class FleetStatsOperator extends RichFlatMapFunction<SpeedDistanceEvent, FleetMetricsEvent> {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis");
    private static final int    REDIS_PORT = Integer.parseInt(
            System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static final int    REDIS_DB   = Integer.parseInt(
            System.getenv().getOrDefault("REDIS_DB", "1"));

    private transient MapState<Integer, Double> lastTotalByTaxi;
    private transient ValueState<Integer>       activeCount;
    private transient ValueState<Double>        fleetTotal;

    private transient Jedis        jedis;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration config) throws Exception {
        lastTotalByTaxi = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("lastTotalByTaxi", Integer.class, Double.class)
        );
        activeCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>("activeCount", Integer.class)
        );
        fleetTotal = getRuntimeContext().getState(
            new ValueStateDescriptor<>("fleetTotal", Double.class)
        );

        try {
            jedis  = new Jedis(REDIS_HOST, REDIS_PORT);
            jedis.select(REDIS_DB);
        } catch (Exception e) {
            jedis = null; // Redis not available (e.g. during unit tests)
        }
        mapper = new ObjectMapper();
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }

    @Override
    public void flatMap(SpeedDistanceEvent event, Collector<FleetMetricsEvent> out) throws Exception {
        Double previous = lastTotalByTaxi.get(event.taxiId);

        int    count = activeCount.value() == null ? 0 : activeCount.value();
        double total = fleetTotal.value()  == null ? 0.0 : fleetTotal.value();

        if (previous == null) {
            count += 1;
            activeCount.update(count);
        }

        total += event.totalDistanceKm - (previous == null ? 0.0 : previous);
        fleetTotal.update(total);
        lastTotalByTaxi.put(event.taxiId, event.totalDistanceKm);

        FleetMetricsEvent metrics = new FleetMetricsEvent(count, total, event.timestamp);

        // Write to Redis so REST API can serve it immediately
        if (jedis != null) {
            try {
                jedis.set("fleet:metrics", mapper.writeValueAsString(metrics));
            } catch (Exception e) {
                // Ignore Redis connection errors during test or transient failures
            }
        }

        out.collect(metrics);
    }
}
