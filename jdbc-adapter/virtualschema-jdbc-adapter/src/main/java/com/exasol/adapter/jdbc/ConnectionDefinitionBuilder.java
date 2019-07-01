package com.exasol.adapter.jdbc;

import com.exasol.ExaConnectionInformation;
import com.exasol.adapter.AdapterProperties;

/**
 * Common interface for all connection definition builders.
 */
public interface ConnectionDefinitionBuilder {
    /**
     * Get the connection definition part of a push-down query.
     *
     * @param properties               user-defined adapter properties
     * @param exaConnectionInformation details of a named Exasol connection
     * @return credentials part of the push-down query
     */
    String buildConnectionDefinition(AdapterProperties properties, ExaConnectionInformation exaConnectionInformation);
}