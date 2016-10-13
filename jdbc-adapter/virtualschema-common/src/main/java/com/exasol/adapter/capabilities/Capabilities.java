package com.exasol.adapter.capabilities;

import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.Predicate;

import java.util.HashSet;
import java.util.Set;

/**
 * Manages a set of supported Capabilities
 */
public class Capabilities {
    
    private Set<MainCapability> mainCapabilities = new HashSet<>();
    private Set<ScalarFunctionCapability> scalarFunctionCaps = new HashSet<>();
    private Set<PredicateCapability> predicateCaps = new HashSet<>();
    private Set<AggregateFunctionCapability> aggregateFunctionCaps = new HashSet<>();
    private Set<LiteralCapability> literalCaps = new HashSet<>();

    public void supportAllCapabilities() {
        for (MainCapability cap : MainCapability.values()) {
            supportMainCapability(cap);
        }
        for (ScalarFunctionCapability function : ScalarFunctionCapability.values()) {
            supportScalarFunction(function);
        }
        for (PredicateCapability pred : PredicateCapability.values()) {
            supportPredicate(pred);
        }
        for (AggregateFunctionCapability function : AggregateFunctionCapability.values()) {
            supportAggregateFunction(function);
        }
        for (LiteralCapability cap : LiteralCapability.values()) {
            supportLiteral(cap);
        }
    }

    public void subtractCapabilities(Capabilities capabilitiesToSubtract) {
        for (MainCapability cap : capabilitiesToSubtract.mainCapabilities) {
            mainCapabilities.remove(cap);
        }
        for (ScalarFunctionCapability cap : capabilitiesToSubtract.getScalarFunctionCapabilities()) {
            scalarFunctionCaps.remove(cap);
        }
        for (PredicateCapability cap : capabilitiesToSubtract.getPredicateCapabilities()) {
            predicateCaps.remove(cap);
        }
        for (AggregateFunctionCapability cap : capabilitiesToSubtract.getAggregateFunctionCapabilities()) {
            aggregateFunctionCaps.remove(cap);
        }
        for (LiteralCapability cap : capabilitiesToSubtract.getLiteralCapabilities()) {
            literalCaps.remove(cap);
        }
    }

    public void supportMainCapability(MainCapability cap) {
        mainCapabilities.add(cap);
    }

    public void supportScalarFunction(ScalarFunctionCapability functionType) {
        scalarFunctionCaps.add(functionType);
    }

    public void supportPredicate(PredicateCapability predicate) {
        predicateCaps.add(predicate);
    }

    public void supportAggregateFunction(AggregateFunctionCapability functionType) {
        aggregateFunctionCaps.add(functionType);
    }
    
    public void supportLiteral(LiteralCapability literal) {
        literalCaps.add(literal);
    }
    
    public Set<MainCapability> getMainCapabilities() {
        return mainCapabilities;
    }
    
    public Set<ScalarFunctionCapability> getScalarFunctionCapabilities() {
        return scalarFunctionCaps;
    }

    public Set<PredicateCapability> getPredicateCapabilities() {
        return predicateCaps;
    }

    public Set<AggregateFunctionCapability> getAggregateFunctionCapabilities() {
        return aggregateFunctionCaps;
    }
    
    public Set<LiteralCapability> getLiteralCapabilities() {
        return literalCaps;
    }
}
