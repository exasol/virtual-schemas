package com.exasol.adapter.dialects.saphana;

import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.SqlLiteralBool;

/**
 * This class generates SQL queries for the {@link DB2SqlDialect}.
 */
public class SapHanaSqlGenerationVisitor extends SqlGenerationVisitor {
    /**
     * Create a new instance of the {@link SapHanaSqlGenerationVisitor}.
     *
     * @param dialect {@link SapHanaSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public SapHanaSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlLiteralBool literal) {
        if (literal.getValue())

        {
            return "1=1";
        } else

        {
            return "1=0";
        }
    }
}
