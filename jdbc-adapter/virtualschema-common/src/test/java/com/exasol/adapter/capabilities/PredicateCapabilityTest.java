package com.exasol.adapter.capabilities;

import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.Predicate;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class PredicateCapabilityTest {

    @Test
    public void testCompleteness() {
        // Do we have predicates where we don't have capabilities for?
        for (Predicate pred : Predicate.values()) {
            boolean foundCap = false;
            for (PredicateCapability cap : PredicateCapability.values()) {
                if (cap.getPredicate() == pred) {
                    foundCap = true;
                }
            }
            assertTrue("Did not find a capability for predicate " + pred.name(), foundCap);
        }
    }

    @Test
    public void testConsistentNaming () {
        for (PredicateCapability cap : PredicateCapability.values()) {
            assertTrue(cap.name().startsWith(cap.getPredicate().name()));
        }
    }

}