package com.addthis.hydra.job.store;

import java.util.Properties;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlDataStore extends JdbcDataStore {
    private static final String description = "mysql";
    private static final Properties properties;
    static {
        properties = new Properties();
        properties.put("user", "spawn");
        properties.put("password", "pw");
    }

    public MysqlDataStore(String host, int port, String dbName, String tableName) throws Exception {
        super("org.drizzle.jdbc.DrizzleDriver", "jdbc:mysql:thin://" + host + ":" + port + "/" + dbName, properties);
        if (host == null || dbName == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        this.tableName = tableName;
        runStartupCommand();
    }

    @Override
    protected void runInsert(String path, String value, String childId) throws SQLException {
        try (Connection connection = getConnection()) {
            String insertTemplate = "insert into " + tableName +
                                    "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                                    "values( ? , ? , ? ) on duplicate key update " + valueKey + "=values(" + valueKey + ")";
            PreparedStatement preparedStatement = connection.prepareStatement(insertTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.setBlob(2, valueToBlob(value));
            preparedStatement.setString(3, childId != null ? childId : blankChildId);
            preparedStatement.execute();
            connection.commit();
        }

    }

    @Override
    protected Blob getValueBlobFromResultSet(ResultSet resultSet) throws SQLException {
        try {
            // Drizzle throws an NPE for the null blob.
            return resultSet.getBlob(valueKey);
        } catch (NullPointerException npe) {
            return null;
        }

    }
    @Override
    public String getDescription() {
        return description;
    }
}
