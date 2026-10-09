package traffic.operators;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import traffic.model.FleetMetricsEvent;
import traffic.model.SpeedDistanceEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class FleetStatsOperatorTest {

    @Mock private RuntimeContext runtimeContext;
    @Mock private MapState<Integer, Double> lastTotalByTaxi;
    @Mock private ValueState<Integer> activeCount;
    @Mock private ValueState<Double> fleetTotal;

    private FleetStatsOperator operator;
    private List<FleetMetricsEvent> output;

    @BeforeEach
    public void setUp() throws Exception {
        when(runtimeContext.getMapState(any(MapStateDescriptor.class))).thenReturn(lastTotalByTaxi);
        when(runtimeContext.getState(any(ValueStateDescriptor.class)))
                .thenReturn((ValueState) activeCount)
                .thenReturn((ValueState) fleetTotal);
        operator = new FleetStatsOperator();
        operator.setRuntimeContext(runtimeContext);
        operator.open(null);
        output = new ArrayList<>();
    }

    @Test
    public void testNewTaxiIsCountedAndDistanceAdded() throws Exception {
        when(lastTotalByTaxi.get(5)).thenReturn(null);
        when(activeCount.value()).thenReturn(null);
        when(fleetTotal.value()).thenReturn(null);

        operator.flatMap(new SpeedDistanceEvent(5, 30.0, 10.0, "2008-02-02T10:00:00"), collectTo(output));

        assertEquals(1, output.size());
        assertEquals(1, output.get(0).activeTaxis);
        assertEquals(10.0, output.get(0).totalDistanceKm, 0.01);
        verify(activeCount).update(1);
        verify(lastTotalByTaxi).put(5, 10.0);
    }

    @Test
    public void testKnownTaxiAddsOnlyDeltaWithoutNewCount() throws Exception {
        when(lastTotalByTaxi.get(5)).thenReturn(10.0);
        when(activeCount.value()).thenReturn(1);
        when(fleetTotal.value()).thenReturn(10.0);

        operator.flatMap(new SpeedDistanceEvent(5, 25.0, 14.0, "2008-02-02T10:05:00"), collectTo(output));

        assertEquals(1, output.get(0).activeTaxis);
        assertEquals(14.0, output.get(0).totalDistanceKm, 0.01);
        verify(activeCount, never()).update(eq(2));
    }

    private Collector<FleetMetricsEvent> collectTo(List<FleetMetricsEvent> list) {
        return new Collector<>() {
            public void collect(FleetMetricsEvent e) { list.add(e); }
            public void close() {}
        };
    }
}
