package com.exasol.adapter.sql;

/**
 * List of all scalar functions supported by EXASOL. Note that predicates are defined separately in {@link Predicate}.
 */
public enum ScalarFunction {

    // Standard Arithmetic Operators
    ADD,
    SUB,
    MULT,
    FLOAT_DIV,

    // Unary prefix operators
    NEG,

    // Numeric
    ABS,
    ACOS,
    ASIN,
    ATAN,
    ATAN2,
    CEIL,
    COS,
    COSH,
    COT,
    DEGREES,
    DIV,
    EXP,
    FLOOR,
    GREATEST,
    LEAST,
    LN,
    LOG,
    MOD,
    POWER,
    RADIANS,
    RAND,
    ROUND,
    SIGN,
    SIN,
    SINH,
    SQRT,
    TAN,
    TANH,
    TRUNC,

    // String Functions
    ASCII,
    BIT_LENGTH,
    CHR,
    COLOGNE_PHONETIC,
    CONCAT,
    DUMP,
    EDIT_DISTANCE,
    INSERT,
    INSTR,
    LENGTH,
    LOCATE,
    LOWER,
    LPAD,
    LTRIM,
    OCTET_LENGTH,
    REGEXP_INSTR,
    REGEXP_REPLACE,
    REGEXP_SUBSTR,
    REPEAT,
    REPLACE,
    REVERSE,
    RIGHT,
    RPAD,
    RTRIM,
    SOUNDEX,
    SPACE,
    SUBSTR,
    TRANSLATE,
    TRIM,
    UNICODE,
    UNICODECHR,
    UPPER,

    // Date/Time Functions
    ADD_DAYS,
    ADD_HOURS,
    ADD_MINUTES,
    ADD_MONTHS,
    ADD_SECONDS,
    ADD_WEEKS,
    ADD_YEARS,
    CONVERT_TZ,
    CURRENT_DATE,
    CURRENT_TIMESTAMP,
    DATE_TRUNC,
    DAY,
    DAYS_BETWEEN,
    DBTIMEZONE,
    EXTRACT(false),
    HOURS_BETWEEN,
    LOCALTIMESTAMP,
    MINUTE,
    MINUTES_BETWEEN,
    MONTH,
    MONTHS_BETWEEN,
    NUMTODSINTERVAL,
    NUMTOYMINTERVAL,
    POSIX_TIME,
    SECOND,
    SECONDS_BETWEEN,
    SESSIONTIMEZONE,
    SYSDATE,
    SYSTIMESTAMP,
    WEEK,
    YEAR,
    YEARS_BETWEEN,

    // Geospatial
    ST_X,
    ST_Y,
    ST_ENDPOINT,
    ST_ISCLOSED,
    ST_ISRING,
    ST_LENGTH,
    ST_NUMPOINTS,
    ST_POINTN,
    ST_STARTPOINT,
    ST_AREA,
    ST_EXTERIORRING,
    ST_INTERIORRINGN,
    ST_NUMINTERIORRINGS,
    ST_GEOMETRYN,
    ST_NUMGEOMETRIES,
    ST_BOUNDARY,
    ST_BUFFER,
    ST_CENTROID,
    ST_CONTAINS,
    ST_CONVEXHULL,
    ST_CROSSES,
    ST_DIFFERENCE,
    ST_DIMENSION,
    ST_DISJOINT,
    ST_DISTANCE,
    ST_ENVELOPE,
    ST_EQUALS,
    ST_FORCE2D,
    ST_GEOMETRYTYPE,
    ST_INTERSECTION,
    ST_INTERSECTS,
    ST_ISEMPTY,
    ST_ISSIMPLE,
    ST_OVERLAPS,
    ST_SETSRID,
    ST_SYMDIFFERENCE,
    ST_TOUCHES,
    ST_TRANSFORM,
    ST_UNION,
    ST_WITHIN,

    // Conversion functions
    CAST(false),
    IS_NUMBER,
    IS_BOOLEAN,
    IS_DATE,
    IS_DSINTERVAL,
    IS_YMINTERVAL,
    IS_TIMESTAMP,
    TO_CHAR,
    TO_DATE,
    TO_DSINTERVAL,
    TO_YMINTERVAL,
    TO_NUMBER,
    TO_TIMESTAMP,

    // Bitwise functions
    BIT_AND,
    BIT_CHECK,
    BIT_NOT,
    BIT_OR,
    BIT_SET,
    BIT_TO_NUM,
    BIT_XOR,

    // Other Functions
    CASE(false),
    CURRENT_SCHEMA,
    CURRENT_SESSION,
    CURRENT_STATEMENT,
    CURRENT_USER,
    HASH_MD5,
    HASH_SHA,
    HASH_SHA1,
    HASH_TIGER,
    NULLIFZERO,
    SYS_GUID,
    ZEROIFNULL;

    private boolean isSimple;

    /**
     * True if the function is simple, i.e. is handled by {@link SqlFunctionScalar}, and false if it has it's own implementation.
     */
    public boolean isSimple() {
        return isSimple;
    }

    ScalarFunction() {
        this.isSimple = true;
    }

    ScalarFunction(boolean isSimple) {
        this.isSimple = isSimple;
    }
}
