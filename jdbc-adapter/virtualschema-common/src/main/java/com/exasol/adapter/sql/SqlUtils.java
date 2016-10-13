package com.exasol.adapter.sql;

import java.util.Map;

public class SqlUtils {

    public static String quoteIdentifierIfNeeded(String identifier, Map<String, ?> config) {
        String quoteChar = "\"";
        if (config.containsKey("QUOTE_CHAR")) {
            quoteChar = config.get("QUOTE_CHAR").toString();
        }
        if (identifier.toUpperCase().equals(identifier)) {
            // Only upper case, no need to quote
            return identifier;
        } else {
            return quoteChar + identifier + quoteChar;
        }
    }
    
}
