package com.exasol.adapter.dialects;

public final class IntegrationTestPreconditions {
    public static boolean isIntegrationConfigurationAvailable() {
        return System.getProperty(IntegrationTestConstants.INTEGRATION_TEST_CONFIGURATION_FILE_PROPERTY) != null;
    }

    private IntegrationTestPreconditions() {
        // prevent instantiation
    }
}