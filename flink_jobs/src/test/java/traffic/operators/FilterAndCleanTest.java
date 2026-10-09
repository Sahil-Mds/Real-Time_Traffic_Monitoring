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
import traffic.model.TaxiLocation;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class FilterAndCleanTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private ValueState<TaxiLocation> valueState;

    private FilterAndClean operator;
    private List<TaxiLocation> output;

    @BeforeEach
    public void setUp() throws Exception {
        when(runtimeContext.getState(any(ValueStateDescriptor.class))).thenReturn(valueState);
        operator = new FilterAndClean();
        operator.setRuntimeContext(runtimeContext);
        operator.open(null);
        output = new ArrayList<>();
    }

    @Test
    public void testFirstValidLocationIsEmitted() throws Exception {
        TaxiLocation first = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        when(valueState.value()).thenReturn(null);

        operator.flatMap(first, collectTo(output));

        assertEquals(1, output.size());
        verify(valueState).update(first);
    }

    @Test
    public void testOutOfRangeCoordinatesDropped() throws Exception {
        TaxiLocation bad = new TaxiLocation(1, "2008-02-02T10:00:00", 100.0, 116.3972, "location");

        operator.flatMap(bad, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testDuplicateTimestampDropped() throws Exception {
        TaxiLocation prev = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        TaxiLocation dup = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9200, 116.4000, "location");
        when(valueState.value()).thenReturn(prev);

        operator.flatMap(dup, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testZeroMovementDropped() throws Exception {
        TaxiLocation prev = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        TaxiLocation same = new TaxiLocation(1, "2008-02-02T10:05:00", 39.9163, 116.3972, "location");
        when(valueState.value()).thenReturn(prev);

        operator.flatMap(same, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testGpsJumpDropped() throws Exception {
        TaxiLocation prev = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        TaxiLocation jump = new TaxiLocation(1, "2008-02-02T10:00:30", 40.9163, 117.3972, "location");
        when(valueState.value()).thenReturn(prev);

        operator.flatMap(jump, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testValidMovementIsEmitted() throws Exception {
        TaxiLocation prev = new TaxiLocation(1, "2008-02-02T10:00:00", 39.9163, 116.3972, "location");
        TaxiLocation curr = new TaxiLocation(1, "2008-02-02T10:06:00", 39.9055, 116.3976, "location");
        when(valueState.value()).thenReturn(prev);

        operator.flatMap(curr, collectTo(output));

        assertEquals(1, output.size());
        assertEquals(1, output.get(0).taxiId);
    }

    @Test
    public void testEndTokenClearsStateAndPassesThrough() throws Exception {
        TaxiLocation end = new TaxiLocation(1, null, 0, 0, "END");

        operator.flatMap(end, collectTo(output));

        verify(valueState).clear();
        assertEquals(1, output.size());
        assertEquals("END", output.get(0).type);
    }

    private Collector<TaxiLocation> collectTo(List<TaxiLocation> list) {
        return new Collector<>() {
            public void collect(TaxiLocation e) { list.add(e); }
            public void close() {}
        };
    }
}
