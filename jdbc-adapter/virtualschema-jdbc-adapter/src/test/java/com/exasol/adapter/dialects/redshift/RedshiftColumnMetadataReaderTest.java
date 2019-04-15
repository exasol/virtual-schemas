package com.exasol.adapter.dialects.redshift;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.AbstractColumnMetadataReaderTest;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.redshift.RedshiftColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

class RedshiftColumnMetadataReaderTest extends AbstractColumnMetadataReaderTest {
    @BeforeEach
    void beforeEach() {
        this.columnMetadataReader = new RedshiftColumnMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testMapJdbcTypeNumeric() {
        assertNumericMappedToDecimalWithPrecisionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION, 2);
    }

    @Test
    void testMapJdbcTypeNumericExceedingExsolMaxPrecisionToDouble() {
        assertNumericMappedToDoubleWithPrecsionAndScale(DataType.MAX_EXASOL_DECIMAL_PRECISION + 1, 2);
    }

    @Test
    void testMapJdbcTypeFallbackToParent() {
        assertThat(this.columnMetadataReader.mapJdbcType(new JdbcTypeDescription(Types.BOOLEAN, 0, 0, 0, "")),
                equalTo(DataType.createBool()));
    }
}
