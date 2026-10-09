package traffic.operators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

import traffic.model.AverageSpeedEvent;
import traffic.model.StateUpdateEvent;

/**
 * Maps AverageSpeedEvent -> StateUpdateEvent AND writes the taxi state
 * to Redis as a Hash under the key "taxi:{taxiId}".
 *
 * Redis key:  taxi:{taxiId}
 * Redis type: String (JSON value)
 *
 * This is read by the REST API at GET /api/taxis.
 */
public class StoreTaxiInfo extends RichMapFunction<AverageSpeedEvent, StateUpdateEvent> {

    // Redis connection details – resolved from env vars at runtime
    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "redis");
    private static final int    REDIS_PORT = Integer.parseInt(
            System.getenv().getOrDefault("REDIS_PORT", "6379"));
    private static final int    REDIS_DB   = Integer.parseInt(
            System.getenv().getOrDefault("REDIS_DB", "1"));

    private transient Jedis        jedis;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        jedis  = new Jedis(REDIS_HOST, REDIS_PORT);
        jedis.select(REDIS_DB);
        mapper = new ObjectMapper();
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }

    @Override
    public StateUpdateEvent map(AverageSpeedEvent event) throws Exception {
        StateUpdateEvent state = new StateUpdateEvent(
            event.taxiId,
            event.avgSpeedKmh,
            event.totalDistanceKm,
            event.timestamp
        );

        // Write to Redis: key = "taxi:{taxiId}", value = JSON
        String key   = "taxi:" + event.taxiId;
        String value = mapper.writeValueAsString(state);
        jedis.set(key, value);

        return state;
    }
}
