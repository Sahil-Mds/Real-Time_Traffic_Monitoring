package traffic;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.Types;


import com.fasterxml.jackson.databind.ObjectMapper;

import traffic.model.AreaViolationEventM;
import traffic.model.AverageSpeedEvent;
import traffic.model.FleetMetricsEvent;
import traffic.model.OverSpeedEventM;
import traffic.model.SpeedDistanceEvent;
import traffic.model.StateUpdateEvent;
import traffic.model.TaxiLocation;
import traffic.operators.AreaViolationsAlert;
import traffic.operators.CalculateAverageSpeed;
import traffic.operators.ComputeSpeedAndDistance;
import traffic.operators.DataCleaner;
import traffic.operators.FilterAndClean;
import traffic.operators.FleetStatsOperator;
import traffic.operators.OverSpeedAlert;
import traffic.operators.StoreTaxiInfo;

import traffic.operators.PropagateLocationInfo;

public class TrafficMonitoringJob {

     private static final String BOOTSTRAP_SERVERS = "kafka:29092";

    /**
     * Generic helper: builds a KafkaSink<String> for the given topic.
     */
private static KafkaSink<String> buildSink(String topic) {
    return KafkaSink.<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
}

// Key-aware sink: sets Kafka message key so Redis connector knows what key to write to
private static KafkaSink<String[]> buildKeyedSink(String topic) {
    return KafkaSink.<String[]>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.<String[]>builder()
                            .setTopic(topic)
                            .setKeySerializationSchema(
                                    element -> element[0].getBytes())
                            .setValueSerializationSchema(
                                    element -> element[1].getBytes())
                            .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("taxi-raw-data")
                .setGroupId("flink-traffic-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        ObjectMapper mapper = new ObjectMapper();

        DataStream<TaxiLocation> locationStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> mapper.readValue(json, TaxiLocation.class))
                .filter(new DataCleaner());

        DataStream<TaxiLocation> cleanStream = locationStream
                .keyBy(loc -> loc.taxiId)
                .flatMap(new FilterAndClean());

	// SINK: dashboard location updates -> taxi-locations topic
	// Emits at most one location update every 5 seconds per taxi.
	DataStream<TaxiLocation> dashboardLocationStream = cleanStream
        	.keyBy(loc -> loc.taxiId)
        	.flatMap(new PropagateLocationInfo());

	dashboardLocationStream
        	.map((TaxiLocation loc) -> mapper.writeValueAsString(loc))
        	.returns(String.class)
        	.sinkTo(buildSink("taxi-locations"));

        DataStream<SpeedDistanceEvent> speedStream = cleanStream
                .keyBy(loc -> loc.taxiId)
                .flatMap(new ComputeSpeedAndDistance());

        speedStream.print();

        DataStream<AverageSpeedEvent> avgSpeedStream = speedStream
                .keyBy(event -> event.taxiId)
                .flatMap(new CalculateAverageSpeed());

        avgSpeedStream.print();

        DataStream<StateUpdateEvent> stateUpdateStream = avgSpeedStream
                .map(new StoreTaxiInfo());

        stateUpdateStream.print();

        
       // SINK: per-taxi state -> state-updates topic (key = "taxi:{taxiId}" for Redis)
        stateUpdateStream
                .map((StateUpdateEvent e) -> new String[]{
                        "taxi:" + e.taxiId,
                        mapper.writeValueAsString(e)
                })
                .returns(Types.OBJECT_ARRAY(Types.STRING))
                .sinkTo(buildKeyedSink("state-updates"));

        DataStream<FleetMetricsEvent> fleetStatsStream = speedStream
                .keyBy(event -> 0)
                .flatMap(new FleetStatsOperator());

        fleetStatsStream.print();

        // SINK: fleet-wide aggregates -> fleet-metrics topic (key = "fleet:metrics" for Redis)
        fleetStatsStream
                .map((FleetMetricsEvent e) -> new String[]{
                        "fleet:metrics",
                        mapper.writeValueAsString(e)
                })
                .returns(Types.OBJECT_ARRAY(Types.STRING))
                .sinkTo(buildKeyedSink("fleet-metrics"));

DataStream<AreaViolationEventM> areaViolationStream = cleanStream
                .keyBy(loc -> loc.taxiId)
                .flatMap(new AreaViolationsAlert());

        areaViolationStream.print();

        // SINK: area violations -> violation-events topic
        areaViolationStream
                .map((AreaViolationEventM e) -> mapper.writeValueAsString(e))
                .returns(String.class)
                .sinkTo(buildSink("violation-events"));

        DataStream<OverSpeedEventM> overSpeedStream = speedStream
                .flatMap(new OverSpeedAlert());

        overSpeedStream.print();

        // SINK: overspeed alerts -> violation-events topic (same topic, different JSON shape)
        overSpeedStream
                .map((OverSpeedEventM e) -> mapper.writeValueAsString(e))
                .returns(String.class)
                .sinkTo(buildSink("violation-events"));

        env.execute("Traffic Monitoring Job");
    }
}