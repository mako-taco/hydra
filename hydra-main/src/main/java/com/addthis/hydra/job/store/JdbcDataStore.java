package com.addthis.hydra.job.store;

import javax.sql.rowset.serial.SerialBlob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecExceptionLineNumber;
import com.addthis.codec.CodecJSON;
import com.addthis.maljson.JSONException;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JdbcDataStore implements SpawnDataStore {
    private static Logger log = LoggerFactory.getLogger(JdbcDataStore.class);
    private static final CodecJSON codecJSON = new CodecJSON();
    protected static final String pathKey = "path";
    protected static final String valueKey = "val";
    protected static final String childKey = "child";
    protected String tableName;
    protected static final int maxPathLength = Parameter.intValue("jdbc.datastore.max.path.length", 200);
    protected static final String blankChildId = "_";

    protected abstract Connection getConnection() throws SQLException;

    protected void runStartupCommand() throws SQLException {
        try (Connection connection = getConnection()) {
            connection.prepareStatement("CREATE TABLE IF NOT EXISTS " + tableName + "( "
                                         + pathKey + " VARCHAR(" + maxPathLength + ") NOT NULL, "
                                         + valueKey + " MEDIUMBLOB, "
                                         + childKey + " VARCHAR(" + maxPathLength + "), "
                                         + "PRIMARY KEY (" + pathKey + ", " + childKey + "))"
            ).execute();
        }
    }

    protected abstract void runInsert(String path, String value, String childId) throws SQLException;

    @Override
    public String get(String path) {
        try (Connection connection = getConnection()){
            PreparedStatement preparedStatement = connection.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey + "=?");
            preparedStatement.setString(1, path);
            return getSingleResult(preparedStatement.executeQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, String> get(String[] paths) {
        if (paths == null) {
            return null;
        }
        Map<String, String> rv = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        sb.append("select " + (pathKey + "," + valueKey) + " from " + tableName);
        boolean started = false;
        for (int i=0; i<paths.length; i++) {
            sb.append((started ? " or " : " where ") + pathKey + "=?");
            started = true;
        }
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(sb.toString());
            int j=1;
            for (String path : paths) {
                preparedStatement.setString(j++, path);
            }
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            do {
                Blob blob =  resultSet.getBlob(valueKey);
                if (blob != null) {
                    rv.put(resultSet.getString(pathKey), blobToValue(blob));
                }
            } while(resultSet.next());
            return rv;
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }
    }

    private void insert(String path, String value, String childId) throws SQLException {
        if (path.length() > maxPathLength || (childId != null && childId.length() > maxPathLength)) {
            throw new IllegalArgumentException("Input path longer than max of " + maxPathLength);
        }
        runInsert(path, value, childId);
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

    protected Blob getValueBlobFromResultSet(ResultSet resultSet) throws SQLException {
        // Needs to be overwritten in MysqlDataStore to get around an annoying drizzle bug
        return resultSet.getBlob(valueKey);
    }

    private String getSingleResult(ResultSet resultSet) throws SQLException {
        boolean foundRows = resultSet.next();
        if (!foundRows) {
            return null;
        }
        Blob b = getValueBlobFromResultSet(resultSet);
        String firstResult = null;
        if (b != null) {
            try {
                firstResult = blobToValue(b);
            } catch (LZFException e) {
                throw new RuntimeException(e);
            }
        }
        boolean moreResults = resultSet.next();
        if (moreResults) {
            throw new RuntimeException("Found multiple results after expecting a unique result; bailing");
        }
        resultSet.close();
        return firstResult;
    }

    @Override
    public String getChild(String parent, String childId) throws Exception {
        try (Connection connection = getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("select " + valueKey + " from " + tableName + " where " + pathKey + "=? and " + childKey + "=?");
            preparedStatement.setString(1, parent);
            preparedStatement.setString(2, childId);
            return getSingleResult(preparedStatement.executeQuery());
        }


    }

    @Override
    public void deleteChild(String parent, String childId) {
        try (Connection connection = getConnection()) {
            String deleteTemplate = "delete from " + tableName + " where " + pathKey + "=? and " + childKey + "=?";
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, parent);
            preparedStatement.setString(2, childId);
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String path) {
        try (Connection connection = getConnection()){
            String deleteTemplate = "delete from " + tableName;
            PreparedStatement preparedStatement = connection.prepareStatement(deleteTemplate);
            preparedStatement.setString(1, path);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String makeChildQueryTemplate(boolean includeChildValues) {
        return "select " + childKey + (includeChildValues ? "," + valueKey : "") + " from " + tableName + " where " + pathKey + "=?";
    }

    protected ResultSet getResultsForQuery(Connection connection, String template, String path) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(template);
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
        try (Connection connection = getConnection()){
            ResultSet resultSet = getResultsForQuery(connection, template, path);
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
        try (Connection connection = getConnection()) {
            HashMap<String, String> rv = new HashMap<>();
            ResultSet resultSet = getResultsForQuery(connection, template, path);
            if (resultSet == null) {
                return rv;
            }
            do {
                Blob blob = resultSet.getBlob(2);
                if (blob != null) {
                    rv.put(resultSet.getString(1), blobToValue(blob));
                }
            } while (resultSet.next());
            return rv;
        } catch (SQLException | LZFException e) {
            throw new RuntimeException(e);
        }

    }

    protected static Blob valueToBlob(String value) throws SQLException {
        return value != null ? new SerialBlob(LZFEncoder.encode(value.getBytes())) : null;
    }

    protected static String blobToValue(Blob blob) throws SQLException, LZFException {
        return blob != null ? new String(LZFDecoder.decode(blob.getBytes(1l, (int) blob.length()))) : null;
    }

    @Override
    public void close() {
    }
}
