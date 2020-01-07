package com.exasol.matcher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * This matcher compares two result sets.
 */
public final class ResultSetMatcher extends TypeSafeMatcher<ResultSet> {
    private final ResultSet expectedResultSet;

    /**
     * Creates a new instance of {@link ResultSetMatcher}.
     *
     * @param expectedResultSet expected Result Set
     */
    public ResultSetMatcher(final ResultSet expectedResultSet) {
        this.expectedResultSet = expectedResultSet;
    }

    /**
     * Compares against a result set.
     *
     * @param expectedResultSet expected result set
     * @return an instance of {@link ResultSetMatcher}
     */
    public static ResultSetMatcher matchesResultSet(final ResultSet expectedResultSet) {
        return new ResultSetMatcher(expectedResultSet);
    }

    @Override
    protected boolean matchesSafely(final ResultSet actualResultSet) {
        try {
            return assertEqualResultSets(actualResultSet);
        } catch (final SQLException exception) {
            throw new AssertionError(
                    "Assertion failed due to an unexpected SQL exception. Cause: " + exception.getSQLState(),
                    exception);
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendValue(this.expectedResultSet);
    }

    private boolean assertEqualResultSets(final ResultSet actualResultSet) throws SQLException {
        final int expectedColumnCount = this.expectedResultSet.getMetaData().getColumnCount();
        final int actualColumnCount = actualResultSet.getMetaData().getColumnCount();
        if (expectedColumnCount != actualColumnCount) {
            return false;
        }
        boolean expectedNext;
        do {
            expectedNext = this.expectedResultSet.next();
            if (expectedNext != actualResultSet.next()) {
                return false;
            }
            if (this.expectedResultSet.isLast() != actualResultSet.isLast()) {
                return false;
            }
            if (expectedNext) {
                if (!doesRowMatch(actualResultSet, expectedColumnCount)) {
                    return false;
                }
            }
        } while (expectedNext);
        return true;
    }

    private boolean doesRowMatch(final ResultSet actualResultSet, final int expectedColumnCount) throws SQLException {
        for (int column = 1; column <= expectedColumnCount; ++column) {
            if (!doesFieldMatch(actualResultSet, column)) {
                return false;
            }
        }
        return true;
    }

    private boolean doesFieldMatch(final ResultSet actualRow, final int column) throws SQLException {
        final int resultSetTypeExpected = this.expectedResultSet.getMetaData().getColumnType(column);
        final int resultSetTypeActual = actualRow.getMetaData().getColumnType(column);
        if (resultSetTypeExpected == resultSetTypeActual) {
            return doesValueMatch(actualRow, column, resultSetTypeExpected);
        } else {
            return false;
        }
    }

    private boolean doesValueMatch(final ResultSet actualRow, final int column, final int resultSetTypeExpected)
            throws SQLException {
        switch (resultSetTypeExpected) {
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.DECIMAL:
                if (!doesIntMatch(actualRow, column)) {
                    return false;
                }
                break;
            case Types.VARCHAR:
                if (!doesStringMatch(actualRow, column)) {
                    return false;
                }
                break;
            default:
                throw new AssertionError("Unknown data type. ResultSetMatcher compares only String and int currently.");
        }
        return true;
    }

    private boolean doesStringMatch(final ResultSet actualRow, final int column) throws SQLException {
        final String expectedString = this.expectedResultSet.getString(column);
        final String actualString = actualRow.getString(column);
        return expectedString.equals(actualString);
    }

    private boolean doesIntMatch(final ResultSet actualRow, final int column) throws SQLException {
        final int expectedInt = this.expectedResultSet.getInt(column);
        final int actualInt = actualRow.getInt(column);
        return expectedInt == actualInt;
    }
}