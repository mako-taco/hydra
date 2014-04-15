package com.addthis.hydra.job.store;

import javax.sql.rowset.serial.SerialBlob;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.ning.compress.lzf.LZFEncoder;

public class PostgresqlDataStore extends JdbcDataStore {

    private static final String description = "postgres";
    private final String insertTemplate;


    public PostgresqlDataStore(String host, int port, String dbName, String tableName) throws Exception {
        this.tableName = tableName;
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + dbName);
        conn.setAutoCommit(false);
        insertTemplate = "WITH new_values (" + pathKey + ", " + valueKey + ", " + childKey + ") as (values (?, ?, ?)),\n" +
                         "upsert as( \n" +
                         "    update " + tableName + " m \n" +
                         "        set " + valueKey + " = nv." + valueKey + "\n" +
                         "    FROM new_values nv\n" +
                         "    WHERE m." + pathKey + " = nv." + pathKey  + " AND m." + childKey + " = nv." + childKey + "\n" +
                         "    RETURNING m.*\n" +
                         ")\n" +
                         "INSERT INTO "+ tableName +  " (" + pathKey + ", " + valueKey + ", " + childKey + ")\n" +
                         "SELECT " + pathKey + ", " + valueKey + ", " + childKey + " FROM new_values\n" +
                         "WHERE NOT EXISTS (SELECT 1 \n" +
                         "                  FROM upsert up \n" +
                         "                  WHERE up." + pathKey + " = new_values." + pathKey +" AND up." + childKey + " = new_values." + childKey + ")";
        createStartupCommand().execute();
    }

    @Override
    protected PreparedStatement createStartupCommand() throws SQLException {
        return conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                     + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                     + valueKey + " TEXT, "
                                     + childKey + " VARCHAR(" + maxPathLength + "), "
                                     + "PRIMARY KEY (" + pathKey + ", " + childKey + "))"
        );

    }

    @Override
    protected PreparedStatement makeInsertStatement(String path, String value, String childId) throws SQLException {
        childId = childId != null ? childId : blankChildId;
        PreparedStatement preparedStatement = conn.prepareStatement(insertTemplate);
        preparedStatement.setString(1, path);
        preparedStatement.setBlob(2, new SerialBlob(LZFEncoder.encode(value.getBytes())));
        preparedStatement.setString(3, childId);
        return preparedStatement;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
