package com.addthis.hydra.job.store;

import java.util.Properties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class H2DataStore extends JdbcDataStore {
    private static final String description = "h2";
    private final String insertTemplate;

    public H2DataStore(String dbPath, String tableName) throws Exception {
        super("org.h2.Driver",  "jdbc:h2:" + dbPath, new Properties(), tableName);
        if (dbPath == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        insertTemplate = "merge into " + tableName +
                         "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                         "values( ? , ? , ? )";
        runStartupCommand();

    }

    @Override
    protected void runInsert(String path, String value, String childId) throws SQLException {
        try (Connection connection = getConnection()){
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId != null ? childId : blankChildId);
            preparedStatement.execute();
            connection.commit();
        }
    }

    @Override
    public String getDescription() {
        return description;
    }
}
