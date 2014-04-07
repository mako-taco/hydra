package com.addthis.hydra.job.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecExceptionLineNumber;
import com.addthis.codec.CodecJSON;
import com.addthis.maljson.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDataStore implements SpawnDataStore {
    private static Logger log = LoggerFactory.getLogger(JdbcDataStore.class);
    private static final CodecJSON codecJSON = new CodecJSON();
    private static final String description = "jdbc_v1";
    private final Connection conn;
    private static final String pathKey = "path";
    private static final String valueKey = "val";
    private static final String childKey = "child";
    private final String tableName;
    private static final int maxPathLength = Parameter.intValue("jdbc.datastore.max.path.length", 200);
    private static final String blankChildId = "_";

    public JdbcDataStore(String dbName, String tableName) throws Exception {
        if (dbName == null || tableName == null) {
            throw new NullPointerException("Null dbName/tableName passed to JdbcDataStore");
        }
        this.tableName = tableName;
        Class.forName("org.drizzle.jdbc.DrizzleDriver");
        Properties props = new Properties();
        props.put("user", "spawn");
        props.put("password", "pw");
        conn = DriverManager.getConnection("jdbc:mysql:thin://localhost:3306/" + dbName, props);
        conn.setAutoCommit(false);
        createStartupCommand().execute();
    }

    private PreparedStatement createStartupCommand() throws SQLException {
        return conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                     + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                     + valueKey + " TEXT, "
                                     + childKey + " VARCHAR(" + maxPathLength + "), "
                                     + "PRIMARY KEY (" + pathKey + ", " + childKey + "))"
        );

    }

    @Override
    public String getDescription() {
        return description;
    }

    private PreparedStatement makeInsertStatement() throws SQLException {
        String insertTemplate = "insert into " + tableName +
                                "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                                "values( ? , ? , ? ) on duplicate key update " + valueKey + "=values(" + valueKey + ")";
        return conn.prepareStatement(insertTemplate);
    }

    @Override
    public String get(String path) {
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey+ "=?");
            preparedStatement.setString(1, path);
            return getSingleResult(preparedStatement.executeQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> get(String[] paths) {
        // Should be possible to get multiple values from a single query.
        if (paths == null) {
            return null;
        }
        Map<String, String> rv = new HashMap<>();
        for (String path : paths) {
            String val = get(path);
            if (val != null) {
                rv.put(path, val);
            }
        }
        return rv;
    }

    private void insert(String path, String value, String childId) throws SQLException {
        if (path.length() > maxPathLength || (childId != null && childId.length() > maxPathLength)) {
            throw new IllegalArgumentException("Input path longer than max of " + maxPathLength);
        }
        PreparedStatement preparedStatement = makeInsertStatement();
        preparedStatement.setString(1, path);
        preparedStatement.setString(2, value);
        preparedStatement.setString(3, childId != null ? childId : blankChildId);
        preparedStatement.execute();
        conn.commit();
    }

    @Override
    public void put(String path, String value) throws Exception {
        insert(path, value, null);
    }

    @Override
    public void putAsChild(String parent, String childId, String value) throws Exception {
        insert(parent, value, childId);
    }

    @Override
    public <T extends Codec.Codable> boolean loadCodable(String path, T shell) {
        String val = get(path);
        if (val == null) {
            return false;
        }
        try {
            codecJSON.decode(shell, path.getBytes());
            return true;
        } catch (CodecExceptionLineNumber codecExceptionLineNumber) {
            return false;
        } catch (JSONException e) {
            return false;
        }

    }

    private static String getSingleResult(ResultSet resultSet) throws SQLException {
        boolean foundRows = resultSet.next();
        if (!foundRows) {
            return null;
        }
        String firstResult = resultSet.getString(valueKey);
        // If multiple rows found, what happens?
        resultSet.close();
        return firstResult;
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        PreparedStatement preparedStatement = conn.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey+ "=? and " + childKey + "=?");
        preparedStatement.setString(1, parent);
        preparedStatement.setString(2, childId);
        return getSingleResult(preparedStatement.executeQuery());

    }

    @Override
    public void deleteChild(String parent, String childId) {
        try {
            String deleteTemplate = "delete from " + tableName + " where " + pathKey + "=? and " + childKey + "=?";
            PreparedStatement preparedStatement = conn.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, parent);
            preparedStatement.setString(2, childId);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try {
            String deleteTemplate = "delete from " + tableName + " where " + pathKey + "=?";
            PreparedStatement preparedStatement = conn.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String makeChildQueryTemplate(boolean includeChildValues) {
        return "select " + childKey + (includeChildValues ? "," + valueKey : "") + " from " + tableName + " where " + pathKey + "=?";
    }

    private ResultSet getResultsForQuery(String template, String path) throws SQLException {
        PreparedStatement preparedStatement = conn.prepareStatement(template);
        preparedStatement.setString(1, path);
        ResultSet resultSet = preparedStatement.executeQuery();
        boolean foundRows = resultSet.next();
        if (!foundRows) {
            return null;
        }
        return resultSet;
    }

    @Override
    public List<String> getChildrenNames(String path) {
        String template = makeChildQueryTemplate(false);
        try {
            ResultSet resultSet = getResultsForQuery(template, path);
            ArrayList<String> rv = new ArrayList<>();
            if (resultSet == null) {
                return rv;
            }
            do {
                rv.add(resultSet.getString(1));
            } while (resultSet.next());
            return rv;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> getAllChildren(String path) {
        String template = makeChildQueryTemplate(true);
        try {
            HashMap<String, String> rv = new HashMap<>();
            ResultSet resultSet = getResultsForQuery(template, path);
            if (resultSet == null) {
                return rv;
            }
            do {
                rv.put(resultSet.getString(1), resultSet.getString(2));
            } while (resultSet.next());
            return rv;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close sql connection", e);
        }
    }
}
