package traffic.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import traffic.model.TaxiLocation;

/**
 * Basic DataCleaner operator to filter out inconsistencies
 * immediately after deserialization from Kafka.
 */
public class DataCleaner implements FilterFunction<TaxiLocation> {

    @Override
    public boolean filter(TaxiLocation location) {
        if (location == null) {
            return false;
        }

        // Allow END tokens to pass through
        if ("END".equals(location.type)) {
            return true;
        }

        // Drop invalid taxi IDs
        if (location.taxiId <= 0) {
            return false;
        }

        // Drop missing timestamps
        if (location.timestamp == null || location.timestamp.trim().isEmpty()) {
            return false;
        }

        // Drop exactly (0.0, 0.0) coordinates which usually mean missing GPS lock
        if (location.latitude == 0.0 && location.longitude == 0.0) {
            return false;
        }

        return true;
    }
}
