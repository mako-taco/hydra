package com.addthis.hydra.job.store;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class H2DataStore extends JdbcDataStore {
    private static final String description = "h2";

    public H2DataStore(String dbPath, String tableName) throws Exception {
        if (dbPath == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        this.tableName = tableName;
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:" + dbPath);
        conn.setAutoCommit(false);
        createStartupCommand().execute();
    }

    @Override
    protected PreparedStatement makeInsertStatement(String path, String value, String childId) throws SQLException {
        String insertTemplate = "merge into " + tableName +
                                "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                                "values( ? , ? , ? )";
        PreparedStatement preparedStatement = conn.prepareStatement(insertTemplate);
        preparedStatement.setString(1, path);
        preparedStatement.setBlob(2, valueToBlob(value));
        preparedStatement.setString(3, childId != null ? childId : blankChildId);
        return preparedStatement;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
