package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.json.JsonObject;
import javax.json.JsonValue;

import com.exasol.ExaMetadata;
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

    @Test
    public void testPushdownFromTestFile() throws Exception {
        //open file and parse pushdown
        String file = "target/test-classes/integration/vschema_simple_inner_join.json";
        String json = Files.toString(new File(file), Charsets.UTF_8);

        // get pushdown from testfile
        JsonObject root = JsonHelper.getJsonObject(json);
        JsonObject test = root.getJsonArray("testCases").getValuesAs(JsonObject.class).get(0);
        String req = test.getJsonArray("expectedPushdownRequest").get(0).toString();
        RequestJsonParser parser = new RequestJsonParser();
        AdapterRequest request = parser.parseRequest(req);
        PushdownRequest pushdownRequest = (PushdownRequest) request;

        //geenrate SQL
        String schemaName = "LS";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false, true);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        ExasolSqlDialect dialect = new ExasolSqlDialect(dialectContext);
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = pushdownRequest.getSelect().accept(sqlGeneratorVisitor);

        

        // compare to expected result
        String expectedPushdown = test.getJsonObject("expectedPushdownResponse").getJsonArray("Exasol").get(0)
                .toString().replaceAll("\\\\\"", "\"").replaceAll("^\"+", "").replaceAll("\"$", "");

        assertEquals(pushdownQuery, expectedPushdown);
    }
}