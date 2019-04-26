package com.exasol.adapter.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exasol.adapter.metadata.DataType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.exasol.adapter.AdapterException;
import com.google.common.collect.ImmutableList;

public class JdbcAdapterPropertiesTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static Map<String, String> getMinimumMandatory() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        return properties;
    }

    @Test
    public void testGetTableFilters() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_TABLES, "T1, T2,T3,t4");
        final List<String> expectedTables = ImmutableList.of("T1", "T2", "T3", "t4");
        assertEquals(expectedTables, JdbcAdapterProperties.getTableFilter(properties));
    }

    @Test
    public void testGetNewSchemaMetadata() {
        final Map<String, String> oldSchemaProperties = new HashMap<String, String>();
        oldSchemaProperties.put("EXISTING_PROP_1", "Old Value 1");
        oldSchemaProperties.put("EXISTING_PROP_2", "Old Value 2");

        final Map<String, String> changedProperties = new HashMap<String, String>();
        changedProperties.put("EXISTING_PROP_1", "New Value");
        changedProperties.put("EXISTING_PROP_2", null);
        changedProperties.put("NEW_PROP", "VAL2");
        changedProperties.put("DELETED_PROP_NON_EXISTING", null);

        final Map<String, String> expectedChangedProperties = new HashMap<String, String>();
        expectedChangedProperties.put("EXISTING_PROP_1", "New Value");
        expectedChangedProperties.put("NEW_PROP", "VAL2");

        assertEquals(expectedChangedProperties,
                JdbcAdapterProperties.getNewProperties(oldSchemaProperties, changedProperties));
    }



    @Test
    public void testGetOracleCastNumberToDecimal() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("SQL_DIALECT", "ORACLE");
        properties.put("ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE", "12,9");
        final DataType type = JdbcAdapterProperties.getOracleNumberTargetType(properties);
        assertAll(() -> assertThat(type.getPrecision(), equalTo(12)),
                () -> assertThat(type.getScale(), equalTo(9)));
    }

    @Test
    public void testGetOracleCastNumberToDecimalDefault() {
        final Map<String, String> properties = new HashMap<>();
        final DataType type = JdbcAdapterProperties.getOracleNumberTargetType(properties);
        assertAll(() -> assertThat(type.getSize(), equalTo(DataType.MAX_EXASOL_VARCHAR_SIZE)),
                () -> assertThat(type.getCharset(), equalTo(DataType.ExaCharset.UTF8)));
    }

}
