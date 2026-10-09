package traffic.operators;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import traffic.model.SpeedDistanceEvent;
import traffic.model.TaxiLocation;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ComputeSpeedAndDistanceTest {

    @Mock private RuntimeContext runtimeContext;
    @Mock private ValueState<TaxiLocation> lastLocationState;
    @Mock private ValueState<Double> totalDistanceState;

    private ComputeSpeedAndDistance operator;
    private List<SpeedDistanceEvent> output;

    @BeforeEach
    public void setUp() throws Exception {
        when(runtimeContext.getState(any(ValueStateDescriptor.class)))
                .thenReturn((ValueState) lastLocationState)
                .thenReturn((ValueState) totalDistanceState);
        operator = new ComputeSpeedAndDistance();
        operator.setRuntimeContext(runtimeContext);
        operator.open(null);
        output = new ArrayList<>();
    }

    @Test
    public void testFirstLocationProducesNoOutput() throws Exception {
        TaxiLocation first = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        when(lastLocationState.value()).thenReturn(null);

        operator.flatMap(first, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testEmitsSpeedAndCumulativeDistance() throws Exception {
        TaxiLocation prev = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        TaxiLocation curr = new TaxiLocation(1, "2008-02-02T10:06:00", 39.9055, 116.3976, "location");
        when(lastLocationState.value()).thenReturn(prev);
        when(totalDistanceState.value()).thenReturn(5.0);

        operator.flatMap(curr, collectTo(output));

        assertEquals(1, output.size());
        SpeedDistanceEvent event = output.get(0);
        assertEquals(1, event.taxiId);
        assertTrue(event.speedKmh > 0);
        assertTrue(event.totalDistanceKm > 5.0);
    }

    @Test
    public void testEndTokenClearsState() throws Exception {
        TaxiLocation end = new TaxiLocation(1, null, 0, 0, "END");

        operator.flatMap(end, collectTo(output));

        verify(lastLocationState).clear();
        verify(totalDistanceState).clear();
        assertTrue(output.isEmpty());
    }

    private Collector<SpeedDistanceEvent> collectTo(List<SpeedDistanceEvent> list) {
        return new Collector<>() {
            public void collect(SpeedDistanceEvent e) { list.add(e); }
            public void close() {}
        };
    }
}
