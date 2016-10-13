package com.exasol.adapter.capabilities;

import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.Predicate;
import com.exasol.adapter.sql.ScalarFunction;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class ScalarFunctionCapabilityTest {

    @Test
    public void testCompleteness() {
        // Do we have functions where we don't have capabilities for?
        for (ScalarFunction function : ScalarFunction.values()) {
            boolean foundCap = false;
            for (ScalarFunctionCapability cap : ScalarFunctionCapability.values()) {
                if (cap.getFunction() == function) {
                    foundCap = true;
                }
            }
            assertTrue("Did not find a capability for function " + function.name(), foundCap);
        }
    }

    @Test
    public void testConsistentNaming () {
        for (ScalarFunctionCapability cap : ScalarFunctionCapability.values()) {
            assertEquals(cap.name(), cap.getFunction().name());
        }
    }

}