package com.exasol.adapter.dialects;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.*;

import javax.json.JsonObject;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.request.AdapterRequest;
import com.exasol.adapter.request.PushDownRequest;
import com.exasol.adapter.request.parser.RequestParser;
import com.exasol.utils.JsonHelper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * This is an integration test for virtual schemas. The idea is that the jdbc-adapter and the EXASOL database have a
 * common set of testdata to use. By doing this we avoid to write and keep tests in multiple locations.
 * <p>
 * This class is a testrunner that executes the given (in json file) test scenarios and asserts the results.
 * <p>
 * Writing a new test means writing new testdata files. The testfiles are in json format and have to have the extension
 * .json. The following attributes have to be present:
 * <ul>
 * <li>testSchema: This is the schema definition. This test does not use the schema definition, since the testcase
 * parses the pushdown request directly.
 * <li>testCases: A list of testcases to be performed on the schema, each of which contains:
 * <ul>
 * <li>testQuery: A single string containing the test query. This test does not use the test query, since the testcase
 * parses the pushdown request directly.
 * <li>expectedPushdownRequest: A list of pushdownRequests as they are generated by the database. This is a list because
 * a single query can generate multiple pushdowns (e.g. join).
 * <li>expectedPushdownResponse: For each dialect that should be tested a list of strings with the returned Pushdown
 * SQLs.
 */
@RunWith(Parameterized.class)
public class FileBasedIntegrationTest {
    private static final String INTEGRATION_TESTFILES_DIR = "target/test-classes/integration";
    private static final String TEST_FILE_KEY_TESTCASES = "testCases";
    private static final String TEST_FILE_KEY_EXP_PD_REQUEST = "expectedPushdownRequest";
    private static final String TEST_FILE_KEY_EXP_PD_RESPONSE = "expectedPushdownResponse";

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<? extends Object> data() {
        final File testDir = new File(INTEGRATION_TESTFILES_DIR);
        return Arrays.asList(testDir.listFiles((dir, name) -> name.endsWith(".json")));
    }

    private final File testFile;

    public FileBasedIntegrationTest(final File testFile) {
        this.testFile = testFile;
    }

    @Test
    public void testPushdownFromTestFile() throws Exception {
        final String jsonTest = Files.toString(this.testFile, Charsets.UTF_8);
        final int numberOfTests = getNumberOfTestsFrom(jsonTest);
        for (int testNr = 0; testNr < numberOfTests; testNr++) {
            final List<PushDownRequest> PushDownRequests = getPushDownRequestsFrom(jsonTest, testNr);
            final Map<String, List<String>> expectedPushdownQueries = getExpectedPushdownQueriesFrom(jsonTest, testNr);
            for (final String dialect : expectedPushdownQueries.keySet()) {
                for (final PushDownRequest PushDownRequest : PushDownRequests) {
                    final String pushdownQuery = generatePushdownQuery(dialect, PushDownRequest,
                            this.testFile.getName(), testNr);
                    assertExpectedPushdowns(expectedPushdownQueries.get(dialect), pushdownQuery,
                            this.testFile.getName(), testNr, dialect);
                }
            }
        }
    }

    private void assertExpectedPushdowns(final List<String> expectedPushdownQueries, final String pushdownQuery,
            final String testFile, final int testNr, final String dialect) {
        final boolean foundInExpected = expectedPushdownQueries.stream().anyMatch(pushdownQuery::contains);
        final StringBuilder errorMessage = new StringBuilder();
        if (!foundInExpected) {
            errorMessage.append("Generated Pushdown: ");
            errorMessage.append(pushdownQuery);
            errorMessage.append(" not found in expected pushdowns (");
            errorMessage.append(expectedPushdownQueries);
            errorMessage.append("). Testfile: ");
            errorMessage.append(testFile);
            errorMessage.append(" ,Test#: ");
            errorMessage.append(testNr);
            errorMessage.append(" ,Dialect: ");
            errorMessage.append(dialect);
        }
        assertTrue(errorMessage.toString(), foundInExpected);
    }

    private int getNumberOfTestsFrom(final String jsonTest) throws Exception {
        final JsonObject root = JsonHelper.getJsonObject(jsonTest);
        return root.getJsonArray(TEST_FILE_KEY_TESTCASES).size();
    }

    private List<PushDownRequest> getPushDownRequestsFrom(final String jsonTest, final int testNr) throws Exception {
        final JsonObject root = JsonHelper.getJsonObject(jsonTest);
        final JsonObject test = root.getJsonArray(TEST_FILE_KEY_TESTCASES).getValuesAs(JsonObject.class).get(testNr);
        final int numberOfPushDownRequests = test.getJsonArray(TEST_FILE_KEY_EXP_PD_REQUEST).size();
        final List<PushDownRequest> PushDownRequests = new ArrayList<>(numberOfPushDownRequests);
        for (int requestNr = 0; requestNr < numberOfPushDownRequests; requestNr++) {
            final String request = test.getJsonArray(TEST_FILE_KEY_EXP_PD_REQUEST).get(requestNr).toString();
            final RequestParser parser = new RequestParser();
            final AdapterRequest parsedRequest = parser.parse(request);
            PushDownRequests.add((PushDownRequest) parsedRequest);
        }
        return PushDownRequests;
    }

    private String generatePushdownQuery(final String dialect, final PushDownRequest PushDownRequest,
            final String testFile, final int testNr)
            throws AdapterException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException {
        final String schemaName = "LS";
        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        final Class<?> dialectClass = getDialectClass(dialect);
        final SqlDialect sqlDialect = (SqlDialect) dialectClass
                .getConstructor(Connection.class, AdapterProperties.class)
                .newInstance(null, AdapterProperties.emptyProperties());
        final SqlGenerationVisitor sqlGeneratorVisitor = sqlDialect.getSqlGenerationVisitor(context);
        try {
            return PushDownRequest.getSelect().accept(sqlGeneratorVisitor);
        } catch (final Exception e) {
            System.err.println("Exception in: " + testFile + " Test#: " + testNr + " dialect: " + dialect);
            throw e;
        }
    }

    protected Class<?> getDialectClass(final String dialect) throws ClassNotFoundException {
        final String fullyQualifiedDialectName = "com.exasol.adapter.dialects." + dialect.toLowerCase() + "." + dialect
                + "SqlDialect";
        final Class<?> dialectClass = Class.forName(fullyQualifiedDialectName);
        return dialectClass;
    }

    private Map<String, List<String>> getExpectedPushdownQueriesFrom(final String jsonTest, final int testNr)
            throws Exception {
        final JsonObject root = JsonHelper.getJsonObject(jsonTest);
        final JsonObject test = root.getJsonArray(TEST_FILE_KEY_TESTCASES).getValuesAs(JsonObject.class).get(testNr);
        final JsonObject expectedResponses = test.getJsonObject(TEST_FILE_KEY_EXP_PD_RESPONSE);
        final Map<String, List<String>> expectedQueriesForDialects = new HashMap<>();
        for (final String dialect : expectedResponses.keySet()) {
            final int numberOfPushdownResponses = test.getJsonObject(TEST_FILE_KEY_EXP_PD_RESPONSE)
                    .getJsonArray(dialect).size();
            final List<String> pushdownResponses = new ArrayList<>(numberOfPushdownResponses);
            for (int pushdownNr = 0; pushdownNr < numberOfPushdownResponses; pushdownNr++) {
                pushdownResponses
                        .add(test.getJsonObject(TEST_FILE_KEY_EXP_PD_RESPONSE).getJsonArray(dialect).get(pushdownNr)
                                .toString().replaceAll("\\\\\"", "\"").replaceAll("^\"+", "").replaceAll("\"$", ""));
            }
            expectedQueriesForDialects.put(dialect, pushdownResponses);
        }
        return expectedQueriesForDialects;
    }
}