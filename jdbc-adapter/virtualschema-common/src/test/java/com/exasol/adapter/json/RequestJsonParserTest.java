package com.exasol.adapter.json;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.internal.matchers.apachecommons.ReflectionEquals;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.metadata.DataType.ExaCharset;
import com.exasol.adapter.request.AdapterRequest;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.request.SetPropertiesRequest;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class RequestJsonParserTest {

    @Test
    public void testParsePushdownRequest() throws Exception {
        // test resources from src/test/resources are copied to target/test-classes, and this folder is the classpath of the junit test.
        String file = "target/test-classes/pushdown_request.json";
        String json = Files.toString(new File(file), Charsets.UTF_8);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("HIVE_SERVER", "my-hive-server");
        properties.put("HIVE_DB", "my-hive-db");
        properties.put("HIVE_USER", "my-hive-user");
        SchemaMetadataInfo expectedSchemaMetaInfo = new SchemaMetadataInfo(
                "MY_HIVE_VSCHEMA", "{\"lastRefreshed\":\"2015-03-01 12:10:01\",\"key\":\"Any custom schema state here\"}",
                properties);
        
        List<TableMetadata> expectedInvolvedTablesMetadata = new ArrayList<>();
        String tableName = "CLICKS";
        String tableAdapterNotes = "";
        List<ColumnMetadata> tableColumns = new ArrayList<>();
        tableColumns.add(new ColumnMetadata("ID", "", DataType.createDecimal(22, 0), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("USER_ID", "", DataType.createDecimal(18, 0), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("URL", "", DataType.createVarChar(1000, ExaCharset.UTF8), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("REQUEST_TIME", "", DataType.createTimestamp(false), true, false, "", ""));
        String tableComment = "";
        expectedInvolvedTablesMetadata.add(new TableMetadata(tableName, tableAdapterNotes, tableColumns, tableComment));
        
        RequestJsonParser parser = new RequestJsonParser();
        AdapterRequest request = parser.parseRequest(json);
        assertObjectEquals(expectedSchemaMetaInfo, request.getSchemaMetadataInfo());
        assertObjectEquals(expectedInvolvedTablesMetadata, ((PushdownRequest)request).getInvolvedTablesMetadata());
    }

    @Test
    public void testParsePushdownRequestAllTypes() throws Exception {
        String file = "target/test-classes/pushdown_request_alltypes.json";
        String json = Files.toString(new File(file), Charsets.UTF_8);

        Map<String, String> properties = new HashMap<String, String>();
        SchemaMetadataInfo expectedSchemaMetaInfo = new SchemaMetadataInfo("VS", "", properties);
        
        List<TableMetadata> expectedInvolvedTablesMetadata = new ArrayList<>();
        String tableName = "T1";
        String tableAdapterNotes = "";
        String tableComment = "";
        List<ColumnMetadata> tableColumns = new ArrayList<>();
        tableColumns.add(new ColumnMetadata("C_DECIMAL", "", DataType.createDecimal(18, 2), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_DOUBLE", "", DataType.createDouble(), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_VARCHAR_UTF8_1", "", DataType.createVarChar(10000, ExaCharset.UTF8), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_VARCHAR_UTF8_2", "", DataType.createVarChar(10000, ExaCharset.UTF8), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_VARCHAR_ASCII", "", DataType.createVarChar(10000, ExaCharset.ASCII), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_CHAR_UTF8_1", "", DataType.createChar(3, ExaCharset.UTF8), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_CHAR_UTF8_2", "", DataType.createChar(3, ExaCharset.UTF8), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_CHAR_ASCII", "", DataType.createChar(3, ExaCharset.ASCII), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_DATE", "", DataType.createDate(), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_TIMESTAMP_1", "", DataType.createTimestamp(false), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_TIMESTAMP_2", "", DataType.createTimestamp(false), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_TIMESTAMP_3", "", DataType.createTimestamp(true), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_BOOLEAN", "", DataType.createBool(), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_GEOMETRY", "", DataType.createGeometry(1), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_INTERVAL_DS_1", "", DataType.createIntervalDaySecond(2, 3), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_INTERVAL_DS_2", "", DataType.createIntervalDaySecond(3, 4), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_INTERVAL_YM_1", "", DataType.createIntervalYearMonth(2), true, false, "", ""));
        tableColumns.add(new ColumnMetadata("C_INTERVAL_YM_2", "", DataType.createIntervalYearMonth(3), true, false, "", ""));
        expectedInvolvedTablesMetadata.add(new TableMetadata(tableName, tableAdapterNotes, tableColumns, tableComment));
        
        RequestJsonParser parser = new RequestJsonParser();
        AdapterRequest request = parser.parseRequest(json);
        assertObjectEquals(expectedSchemaMetaInfo, request.getSchemaMetadataInfo());
        assertObjectEquals(expectedInvolvedTablesMetadata, ((PushdownRequest)request).getInvolvedTablesMetadata());
    }

    @Test
    public void testParseSetPropertiesRequest() throws Exception {
        String file = "target/test-classes/set_properties_request.json";
        String json = Files.toString(new File(file), Charsets.UTF_8);

        Map<String, String> expectedOldSchemaProperties = new HashMap<String, String>();
        expectedOldSchemaProperties.put("EXISTING_PROP_1", "Old Value 1");
        expectedOldSchemaProperties.put("EXISTING_PROP_2", "Old Value 2");
        SchemaMetadataInfo expectedSchemaMetaInfo = new SchemaMetadataInfo("VS", "", expectedOldSchemaProperties);
        
        Map<String, String> expectedNewProperties = new HashMap<String, String>();
        expectedNewProperties.put("EXISTING_PROP_1", "New Value");
        expectedNewProperties.put("EXISTING_PROP_2", null);
        expectedNewProperties.put("NEW_PROP", "VAL2");
        expectedNewProperties.put("DELETED_PROP_NON_EXISTING", null);
        
        RequestJsonParser parser = new RequestJsonParser();
        AdapterRequest request = parser.parseRequest(json);
        assertObjectEquals(expectedSchemaMetaInfo, request.getSchemaMetadataInfo());
        assertObjectEquals(expectedNewProperties, ((SetPropertiesRequest)request).getProperties());
    }
    
    /**
     * Without this method we would need to override equals() and .hashcode() for each object, which explodes code and makes it less maintainable
     */
    public <T> void assertObjectEquals(final T expected, final T actual) {
        assertTrue("Expected:\n" + expected + "\nactual:\n" + actual, new ReflectionEquals(actual, (String[])null).matches(expected));
    }
}
