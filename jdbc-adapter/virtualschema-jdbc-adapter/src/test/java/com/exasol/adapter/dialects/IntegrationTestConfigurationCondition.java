package com.exasol.adapter.dialects;

import org.junit.jupiter.api.extension.*;

/**
 * This class checks the preconditions for running the integration tests.
 */
public class IntegrationTestConfigurationCondition implements ExecutionCondition {
    private static final String INTEGRATION_TEST_CONFIGURATION_FILE_PROPERTY = "integrationtest.configfile";

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        if (System.getProperty(INTEGRATION_TEST_CONFIGURATION_FILE_PROPERTY) == null) {
            return ConditionEvaluationResult.disabled("The property \"" + INTEGRATION_TEST_CONFIGURATION_FILE_PROPERTY
                    + "\" is not set. Integration test are disabled.");
        } else {
            return ConditionEvaluationResult.enabled("Integration tests are enabled");
        }
    }
}