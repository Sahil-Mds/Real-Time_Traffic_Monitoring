package traffic.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;

import traffic.model.AreaViolationEventM;
import traffic.model.TaxiLocation;

@ExtendWith(MockitoExtension.class)
public class AreaViolationsAlertTest {

    @Mock
    private RuntimeContext runtimeContext;

    @Mock
    private ValueState<Boolean> warningSentState;

    @Mock
    private ValueState<Boolean> discardSentState;

    private AreaViolationsAlert operator;
    private List<AreaViolationEventM> output;

    @BeforeEach
    public void setUp() throws Exception {
        when(runtimeContext.getState(any(ValueStateDescriptor.class)))
                .thenReturn(warningSentState)
                .thenReturn(discardSentState);

        operator = new AreaViolationsAlert();
        operator.setRuntimeContext(runtimeContext);
        operator.open(null);

        output = new ArrayList<>();
    }

    @Test
    public void testTaxiOutside15KmProducesDiscardAlert() throws Exception {
        when(warningSentState.value()).thenReturn(false);
        when(discardSentState.value()).thenReturn(false);

        TaxiLocation location = new TaxiLocation(
                99,
                "2024-06-01T10:00:00",
                40.1000,
                116.6000,
                "location"
        );

        operator.flatMap(location, collectTo(output));

        assertEquals(1, output.size());
        assertEquals(99, output.get(0).taxiId);
        assertEquals("DISCARD_15KM", output.get(0).violationType);

        verify(discardSentState).update(true);
    }

    @Test
    public void testTaxiInside10KmProducesNoAlert() throws Exception {
        when(warningSentState.value()).thenReturn(false);
        when(discardSentState.value()).thenReturn(false);

        TaxiLocation location = new TaxiLocation(
                1,
                "2024-06-01T10:00:00",
                39.9163,
                116.3972,
                "location"
        );

        operator.flatMap(location, collectTo(output));

        assertTrue(output.isEmpty());
    }

    @Test
    public void testEndTokenClearsState() throws Exception {
        TaxiLocation end = new TaxiLocation(
                99,
                null,
                0,
                0,
                "END"
        );

        operator.flatMap(end, collectTo(output));

        verify(warningSentState).clear();
        verify(discardSentState).clear();
        assertTrue(output.isEmpty());
    }

    private Collector<AreaViolationEventM> collectTo(List<AreaViolationEventM> list) {
        return new Collector<>() {
            @Override
            public void collect(AreaViolationEventM record) {
                list.add(record);
            }

            @Override
            public void close() {
            }
        };
    }
}