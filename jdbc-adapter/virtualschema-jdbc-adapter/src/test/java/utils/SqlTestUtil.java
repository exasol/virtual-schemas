package utils;

public final class SqlTestUtil {
    private SqlTestUtil(){
        //Intentionally left blank
    }

    /**
     * Convert newlines, tabs, and double whitespaces to whitespaces. At the end only single whitespaces remain.
     */
    public static String normalizeSql(final String sql) {
        return sql.replaceAll("\t", " ")
              .replaceAll("\n", " ")
              .replaceAll("\\s+", " ");
    }
}

