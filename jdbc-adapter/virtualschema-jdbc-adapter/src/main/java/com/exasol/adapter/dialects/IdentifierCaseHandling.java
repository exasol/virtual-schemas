package com.exasol.adapter.dialects;

/**
 * This enumeration represents how unquoted or quoted identifiers in queries or DDLs are handled.
 */
public enum IdentifierCaseHandling {
    INTERPRET_AS_LOWER, INTERPRET_AS_UPPER, INTERPRET_CASE_SENSITIVE
}