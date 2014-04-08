package com.addthis.hydra.job.store;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresqlDataStore extends JdbcDataStore {

    private static final String description = "postgres";


    public PostgresqlDataStore(String host, int port, String dbName, String tableName) throws Exception {
        this.tableName = tableName;
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbName);
        conn.setAutoCommit(false);
        createStartupCommand().execute();
    }

    @Override
    protected PreparedStatement createStartupCommand() throws SQLException {
        return conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                     + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                     + valueKey + " TEXT, "
                                     + childKey + " VARCHAR(" + maxPathLength + "))"

        );

    }


    @Override
    protected PreparedStatement makeInsertStatement(String path, String value, String childId) throws SQLException {


        String insertTemplate = "UPDATE " + tableName + " SET " + valueKey + "=? WHERE " + pathKey + "=? AND " + childKey  + "=?;\n" +
                                "INSERT INTO " + tableName + "(" + pathKey + ", " + valueKey +", " + childKey +" )\n" +
                                "       SELECT ?, ?, ?\n" +
                                "       WHERE NOT EXISTS (SELECT ? FROM " + tableName + " WHERE " + pathKey +  "=? AND " + childKey + "=? );";
        PreparedStatement preparedStatement = conn.prepareStatement(insertTemplate);
        childId = childId != null ? childId : blankChildId;
        preparedStatement.setString(1, value);
        preparedStatement.setString(2, path);
        preparedStatement.setString(3, childId);
        preparedStatement.setString(4, path);
        preparedStatement.setString(5, value);
        preparedStatement.setString(6, childId);
        preparedStatement.setString(7, value);
        preparedStatement.setString(8, path);
        preparedStatement.setString(9, childId);
        return preparedStatement;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
