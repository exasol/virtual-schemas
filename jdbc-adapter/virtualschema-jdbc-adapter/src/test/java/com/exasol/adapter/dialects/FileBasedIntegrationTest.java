package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FilenameFilter;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
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
        //FilenameFilter filter = (dir, name) -> name.endsWith(".json");
        File[] files = testDir.listFiles((dir, name) -> name.endsWith(".json"));
        for (File testFile : files) {
            String jsonTest = Files.toString(testFile, Charsets.UTF_8);
            PushdownRequest pushdownRequest = getPushdownRequestFrom(jsonTest);
            String pushdownQuery = generatePushdownQuery(pushdownRequest, hasMultipleTables(jsonTest));
            String expectedPushdownQuery = getExpectedPushdownQueryFrom(jsonTest);
            System.out.println("$$$$$$$$$$$$$$$$$$ " + testFile);
            assertEquals(expectedPushdownQuery, pushdownQuery);
        }
    }

    private PushdownRequest getPushdownRequestFrom(String jsonTest) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(0);
        String req = test.getJsonArray("expectedPushdownRequest").get(0).toString();
        RequestJsonParser parser = new RequestJsonParser();
        AdapterRequest request = parser.parseRequest(req);
        PushdownRequest pushdownRequest = (PushdownRequest) request;
        return pushdownRequest;
    }

    private Boolean hasMultipleTables(String jsonTest) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(0);
        JsonValue req = test.getJsonArray("expectedPushdownRequest").get(0);
        int size = ((JsonObject) req).getJsonArray("involvedTables").size();
        System.out.println("SIZESIZESIZSE: " + size);
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

    private String getExpectedPushdownQueryFrom(String jsonTest) throws Exception {
        JsonObject root = JsonHelper.getJsonObject(jsonTest);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(0);
        return test.getJsonObject("expectedPushdownResponse").getJsonArray("Exasol").get(0)
                .toString().replaceAll("\\\\\"", "\"").replaceAll("^\"+", "").replaceAll("\"$", "");
    }
}