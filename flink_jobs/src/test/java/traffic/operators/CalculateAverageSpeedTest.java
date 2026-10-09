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
import traffic.model.AverageSpeedEvent;
import traffic.model.SpeedDistanceEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CalculateAverageSpeedTest {

    @Mock private RuntimeContext runtimeContext;
    @Mock private ValueState<Double> speedSumState;
    @Mock private ValueState<Integer> speedCountState;

    private CalculateAverageSpeed operator;
    private List<AverageSpeedEvent> output;

    @BeforeEach
    public void setUp() throws Exception {
        when(runtimeContext.getState(any(ValueStateDescriptor.class)))
                .thenReturn((ValueState) speedSumState)
                .thenReturn((ValueState) speedCountState);
        operator = new CalculateAverageSpeed();
        operator.setRuntimeContext(runtimeContext);
        operator.open(null);
        output = new ArrayList<>();
    }

    @Test
    public void testAverageOfThreeSpeeds() throws Exception {
        when(speedSumState.value()).thenReturn(null).thenReturn(10.0).thenReturn(30.0);
        when(speedCountState.value()).thenReturn(null).thenReturn(1).thenReturn(2);

        operator.flatMap(new SpeedDistanceEvent(1, 10.0, 0.0, "2008-02-02T10:00:00"), collectTo(output));
        operator.flatMap(new SpeedDistanceEvent(1, 20.0, 0.0, "2008-02-02T10:01:00"), collectTo(output));
        operator.flatMap(new SpeedDistanceEvent(1, 30.0, 0.0, "2008-02-02T10:02:00"), collectTo(output));

        assertEquals(3, output.size());
        assertEquals(20.0, output.get(2).avgSpeedKmh, 0.01);
    }

    @Test
    public void testSingleSpeedAverageEqualsSelf() throws Exception {
        when(speedSumState.value()).thenReturn(null);
        when(speedCountState.value()).thenReturn(null);

        operator.flatMap(new SpeedDistanceEvent(1, 42.0, 0.0, "2008-02-02T10:00:00"), collectTo(output));

        assertEquals(42.0, output.get(0).avgSpeedKmh, 0.01);
    }

    private Collector<AverageSpeedEvent> collectTo(List<AverageSpeedEvent> list) {
        return new Collector<>() {
            public void collect(AverageSpeedEvent e) { list.add(e); }
            public void close() {}
        };
    }
}
