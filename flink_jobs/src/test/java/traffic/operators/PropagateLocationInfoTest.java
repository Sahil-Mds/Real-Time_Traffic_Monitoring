package traffic.operators;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import traffic.model.TaxiLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PropagateLocationInfoTest {

    private PropagateLocationInfo operator;
    private AtomicReference<Long> stateValue;

    @BeforeEach
    public void setUp() throws Exception {
        operator = new PropagateLocationInfo();

        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        ValueState<Long> lastEmittedState = mock(ValueState.class);
        stateValue = new AtomicReference<>();

        when(runtimeContext.getState(any(ValueStateDescriptor.class))).thenReturn(lastEmittedState);

        when(lastEmittedState.value()).thenAnswer(invocation -> stateValue.get());

        doAnswer(invocation -> {
            stateValue.set(invocation.getArgument(0));
            return null;
        }).when(lastEmittedState).update(any(Long.class));

        doAnswer(invocation -> {
            stateValue.set(null);
            return null;
        }).when(lastEmittedState).clear();

        operator.setRuntimeContext(runtimeContext);
        operator.open(new Configuration());
    }

    @Test
    public void emitsFirstLocation() throws Exception {
        List<TaxiLocation> output = new ArrayList<>();

        TaxiLocation first = new TaxiLocation(
                1,
                "2008-02-02T10:00:00",
                39.9163,
                116.3972,
                "location"
        );

        operator.flatMap(first, collectTo(output));

        assertEquals(1, output.size());
        assertEquals(1, output.get(0).taxiId);
    }

    @Test
    public void suppressesLocationsInsideFiveSecondWindow() throws Exception {
        List<TaxiLocation> output = new ArrayList<>();

        TaxiLocation first = new TaxiLocation(
                1,
                "2008-02-02T10:00:00",
                39.9163,
                116.3972,
                "location"
        );

        TaxiLocation second = new TaxiLocation(
                1,
                "2008-02-02T10:00:03",
                39.9164,
                116.3973,
                "location"
        );

        operator.flatMap(first, collectTo(output));
        operator.flatMap(second, collectTo(output));

        assertEquals(1, output.size());
        assertEquals("2008-02-02T10:00:00", output.get(0).timestamp);
    }

    @Test
    public void emitsLocationAfterFiveSeconds() throws Exception {
        List<TaxiLocation> output = new ArrayList<>();

        TaxiLocation first = new TaxiLocation(
                1,
                "2008-02-02T10:00:00",
                39.9163,
                116.3972,
                "location"
        );

        TaxiLocation second = new TaxiLocation(
                1,
                "2008-02-02T10:00:05",
                39.9165,
                116.3975,
                "location"
        );

        operator.flatMap(first, collectTo(output));
        operator.flatMap(second, collectTo(output));

        assertEquals(2, output.size());
        assertEquals("2008-02-02T10:00:05", output.get(1).timestamp);
    }

    @Test
    public void alwaysForwardsEndEventAndClearsState() throws Exception {
        List<TaxiLocation> output = new ArrayList<>();

        TaxiLocation first = new TaxiLocation(
                1,
                "2008-02-02T10:00:00",
                39.9163,
                116.3972,
                "location"
        );

        TaxiLocation end = new TaxiLocation(
                1,
                "2008-02-02T10:00:02",
                39.9163,
                116.3972,
                "END"
        );

        TaxiLocation afterEnd = new TaxiLocation(
                1,
                "2008-02-02T10:00:03",
                39.9164,
                116.3973,
                "location"
        );

        operator.flatMap(first, collectTo(output));
        operator.flatMap(end, collectTo(output));
        operator.flatMap(afterEnd, collectTo(output));

        assertEquals(3, output.size());
        assertEquals("END", output.get(1).type);
        assertEquals("2008-02-02T10:00:03", output.get(2).timestamp);
    }

    private Collector<TaxiLocation> collectTo(List<TaxiLocation> output) {
        return new Collector<TaxiLocation>() {
            @Override
            public void collect(TaxiLocation record) {
                output.add(record);
            }

            @Override
            public void close() {
            }
        };
    }
}