package com.exasol.adapter.dialects;

import org.junit.jupiter.api.extension.*;

/**
 * This class checks the preconditions for running the integration tests.
 */
public class IntegrationTestConfigurationCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        if (IntegrationTestPreconditions.isIntegrationConfigurationAvailable()) {
            return ConditionEvaluationResult.enabled("Integration tests are enabled");
        } else {
            return ConditionEvaluationResult
                    .disabled("The property \"" + IntegrationTestConstants.INTEGRATION_TEST_CONFIGURATION_FILE_PROPERTY
                            + "\" is not set. Integration test are disabled.");
        }
    }
}