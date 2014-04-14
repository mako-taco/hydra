package com.addthis.hydra.job.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class H2DataStore extends JdbcDataStore {
    private static final String description = "h2";
    private final String dbPath;

    public H2DataStore(String dbPath, String tableName) throws Exception {
        if (dbPath == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        this.dbPath = dbPath;
        this.tableName = tableName;
        Class.forName("org.h2.Driver");
        runStartupCommand();
    }

    @Override
    protected Connection getConnection() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:" + dbPath);
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    protected void runInsert(String path, String value, String childId) throws SQLException {
        String insertTemplate = "merge into " + tableName +
                                "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                                "values( ? , ? , ? )";
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
