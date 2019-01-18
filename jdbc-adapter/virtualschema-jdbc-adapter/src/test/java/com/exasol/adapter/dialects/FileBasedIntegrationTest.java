package com.exasol.adapter.dialects;

import static org.junit.Assert.assertTrue;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.json.JsonObject;
import javax.json.JsonValue;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.request.AdapterRequest;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.utils.JsonHelper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.junit.Test;
import org.mockito.Mockito;



public class FileBasedIntegrationTest {
    private static final String INTEGRATION_TESTFILES_DIR = "target/test-classes/integration";

    @Test
    public void testPushdownFromTestFile() throws Exception {
        File testDir = new File(INTEGRATION_TESTFILES_DIR);
        File[] files = testDir.listFiles((dir, name) -> name.endsWith(".json"));
        for (File testFile : files) {
            String jsonTest = Files.toString(testFile, Charsets.UTF_8);
            int numberOftests = getNumberOfTestsFrom(jsonTest);
            for (int testNr = 0; testNr < numberOftests; testNr++) {
                List<PushdownRequest> pushdownRequests = getPushdownRequestsFrom(jsonTest, testNr);
                List<String> expectedPushdownQueries = getExpectedPushdownQueriesFrom(jsonTest, testNr);
                for (PushdownRequest pushdownRequest: pushdownRequests) {
                    String pushdownQuery = generatePushdownQuery(pushdownRequest, hasMultipleTables(jsonTest, testNr));
                    assertExpectedPushdowns(expectedPushdownQueries, pushdownQuery, testFile.getName(), testNr);
                }
            }
        }
    }

    private void assertExpectedPushdowns(List<String> expectedPushdownQueries, String pushdownQuery, String testFile,
            int testNr) {
        boolean foundInExpected = expectedPushdownQueries.stream().anyMatch(pushdownQuery::contains);
        StringBuilder errorMessage = new StringBuilder();
        if (!foundInExpected)
        {
            errorMessage.append("Generated Pushdown: ");
            errorMessage.append(pushdownQuery);
            errorMessage.append(" not found in expected pushdowns (");
            errorMessage.append(expectedPushdownQueries);
            errorMessage.append("). Testfile: ");
            errorMessage.append(testFile);
            errorMessage.append(" ,Test#: ");
            errorMessage.append(testNr);
        }
        assertTrue(errorMessage.toString(), foundInExpected);
    }

    private int getNumberOfTestsFrom(String jsonTest) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        return root.getJsonArray("testCases").size();
    }

    private List<PushdownRequest> getPushdownRequestsFrom(String jsonTest, int testNr) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(testNr);
        int numberOfPushdownRequests = test.getJsonArray("expectedPushdownRequest").size();
        List<PushdownRequest> pushdownRequests = new ArrayList<PushdownRequest>(numberOfPushdownRequests);
        for(int requestNr = 0; requestNr < numberOfPushdownRequests; requestNr++) {
            String req = test.getJsonArray("expectedPushdownRequest").get(requestNr).toString();
            RequestJsonParser parser = new RequestJsonParser();
            AdapterRequest request = parser.parseRequest(req);
            pushdownRequests.add((PushdownRequest) request);
        }
        return pushdownRequests;
    }

    private Boolean hasMultipleTables(String jsonTest, int testNr) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(testNr);
        JsonValue req = test.getJsonArray("expectedPushdownRequest").get(0);
        int size = ((JsonObject) req).getJsonArray("involvedTables").size();
        return size > 1;
    }

    private String generatePushdownQuery(PushdownRequest pushdownRequest, Boolean multipleTables) throws AdapterException {
        String schemaName = "LS";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false, multipleTables);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        ExasolSqlDialect dialect = new ExasolSqlDialect(dialectContext);
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        return pushdownRequest.getSelect().accept(sqlGeneratorVisitor);
    }

    private List<String> getExpectedPushdownQueriesFrom(String jsonTest, int testNr) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(testNr);
        int numberOfPushdownResponses = test.getJsonObject("expectedPushdownResponse").getJsonArray("Exasol").size();
        List<String> pushdownResponses = new ArrayList<>(numberOfPushdownResponses);
        for(int pushdownNr = 0; pushdownNr < numberOfPushdownResponses; pushdownNr++) {
            pushdownResponses.add(test.getJsonObject("expectedPushdownResponse").getJsonArray("Exasol").get(pushdownNr)
                    .toString().replaceAll("\\\\\"", "\"").replaceAll("^\"+", "").replaceAll("\"$", ""));
        }
        return pushdownResponses;
    }
}