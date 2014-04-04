package com.addthis.hydra.job.store;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;

public class JdbcDataStore implements SpawnDataStore {
    private static final String description = "jdbc_v1";
    private final Connection conn;
    private static final String pathKey = "p";
    private static final String valueKey = "v";
    private static final String dbName = "SpawnData";
    private static final String tableName = "jdbc_datastore_v2";
    private static final String queryTemplate = "select ? from " + tableName;
    private static final String insertTemplate = "insert into " + tableName + "(" + pathKey + "," + valueKey + ") values( ? , ? )";

    public JdbcDataStore() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Properties props = new Properties();
        props.put("user", "spawn");
        props.put("password", "pw");
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/SpawnData1", props);
        conn.setAutoCommit(false);
        createStartupCommand().execute();
        // Create database/table
        // Test put/get
    }

    private PreparedStatement createStartupCommand() throws SQLException {
        return conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(id), " +pathKey+ " TEXT NOT NULL, "+valueKey+" TEXT );");

    }

    @Override
    public String getDescription() {
        return description;
    }

    private PreparedStatement makeQueryStatement(String columns, String key, String value) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement("select " + columns + " from " + tableName + " where " + key + "=?");
        preparedStatement.setString(1, value);
        return preparedStatement;
    }

    private PreparedStatement makeInsertStatement() throws SQLException {
        return conn.prepareStatement(insertTemplate);
    }

    @Override
    public String get(String path) {
        try {
            PreparedStatement preparedStatement = makeQueryStatement(valueKey, pathKey, path);
            ResultSet resultSet = preparedStatement.executeQuery();
            boolean foundRows = resultSet.next();
            if (!foundRows) {
                return null;
            }
            String firstResult = resultSet.getString(valueKey);
            // If multiple rows found, what happens?
            resultSet.close();
            return firstResult;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> get(String[] paths) {
        try {
            // If path contains a comma, need to encode
            PreparedStatement preparedStatement = makeQueryStatement(pathKey + "," + valueKey, pathKey, Strings.join(paths, ","));
            ResultSet resultSet = preparedStatement.executeQuery();
            Map<String, String> rv = new HashMap<>();
            for (int i=0; i<resultSet.getFetchSize(); i++) {
                rv.put(resultSet.getString(0), resultSet.getString(1));
            }
            return rv;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String path, String value) throws Exception {
        // Update if exists already
        PreparedStatement preparedStatement = makeInsertStatement();
        preparedStatement.setString(1, path);
        preparedStatement.setString(2, value);
        preparedStatement.addBatch();
        preparedStatement.execute();
        conn.commit();
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {

    }

    @Override
    public <T extends Codec.Codable> boolean loadCodable(String path, T shell) {
        return false;
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        return null;
    }

    @Override
    public void deleteChild(String parent, String childId) {

    }

    @Override
    public void delete(String path) {

    }

    @Override
    public List<String> getChildrenNames(String path) {
        return null;
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        return null;
    }

    @Override
    public void close() {

    }
}
