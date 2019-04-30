package utils;

public final class SqlTestUtil {
    private SqlTestUtil(){
        //Intentionally left blank
    }

    /**
     * Convert newlines, tabs, and double whitespaces to whitespaces. At the end only single whitespaces remain.
     * 
     * @param sql original SQL
     * @return normalized SQL
     */
    public static String normalizeSql(final String sql) {
        return sql.replaceAll("\t", " ")
              .replaceAll("\n", " ")
              .replaceAll("\\s+", " ");
    }
}

