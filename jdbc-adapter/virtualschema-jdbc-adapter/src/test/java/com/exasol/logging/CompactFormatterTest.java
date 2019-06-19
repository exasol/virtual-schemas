package com.exasol.logging;

import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.Assert.assertThat;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.junit.Test;

public class CompactFormatterTest {

    @Test
    public void testFormat() {
        final LogRecord record = new LogRecord(Level.FINEST, "Regular.");
        record.setSourceClassName(this.getClass().getName());
        assertFormattedRecordMatchesPattern(record,
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} FINEST  \\[c.e.l.CompactFormatterTest\\] Regular.[\\n\\r]+");
    }

    private void assertFormattedRecordMatchesPattern(final LogRecord record, final String expectedPattern) {
        final String formattedMessage = new CompactFormatter().format(record);
        assertThat(formattedMessage, matchesPattern(expectedPattern));
    }

    @Test
    public void testFormatForClassWithoutPackageName() {
        final LogRecord record = new LogRecord(Level.INFO, "No package.");
        record.setSourceClassName("TheClass");
        assertFormattedRecordMatchesPattern(record,
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} INFO    \\[TheClass\\] No package.[\\n\\r]+");
    }

    @Test
    public void testFormatForClassWithAnEmptytPackagePart() {
        final LogRecord record = new LogRecord(Level.INFO, "Empty package part.");
        record.setSourceClassName("alpha.beta..TheClass");
        assertFormattedRecordMatchesPattern(record,
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} INFO    \\[a\\.b\\.\\.TheClass\\] Empty package part.[\\n\\r]+");
    }

    @Test
    public void testFormatForEmptyClass() {
        final LogRecord record = new LogRecord(Level.INFO, "Empty class.");
        record.setSourceClassName("");
        assertFormattedRecordMatchesPattern(record,
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} INFO    Empty class.[\\n\\r]+");
    }

    @Test
    public void testFormatForNullClass() {
        final LogRecord record = new LogRecord(Level.INFO, "Null class.");
        record.setSourceClassName(null);
        assertFormattedRecordMatchesPattern(record,
                "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} INFO    Null class.[\\n\\r]+");
    }
}
