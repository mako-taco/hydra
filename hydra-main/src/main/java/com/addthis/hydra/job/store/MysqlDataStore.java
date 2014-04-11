package com.addthis.hydra.job.store;

import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFInputStream;
import io.netty.buffer.ByteBufInputStream;
import org.xerial.snappy.SnappyInputStream;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.util.Properties;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.zip.GZIPInputStream;

public class MysqlDataStore extends JdbcDataStore {
    private static final String description = "mysql";


    public MysqlDataStore(String host, int port, String dbName, String tableName) throws Exception {
        if (host == null || dbName == null || tableName == null) {
            throw new IllegalArgumentException("Null dbName/tableName passed to JdbcDataStore");
        }
        this.tableName = tableName;
        Class.forName("org.drizzle.jdbc.DrizzleDriver");
        Properties props = new Properties();
        props.put("user", "spawn");
        props.put("password", "pw");
        conn = DriverManager.getConnection("jdbc:mysql:thin://" + host + ":" + port + "/" + dbName, props);
        createStartupCommand().execute();
    }

    @Override
    protected PreparedStatement makeInsertStatement(String path, String value, String childId) throws SQLException {
        String insertTemplate = "insert into " + tableName +
                                "(" + pathKey + "," + valueKey + "," + childKey + ") " +
                                "values( ? , ? , ? ) on duplicate key update " + valueKey + "=values(" + valueKey + ")";
        PreparedStatement preparedStatement = conn.prepareStatement(insertTemplate);
        preparedStatement.setString(1, path);
        preparedStatement.setBlob(2, new SerialBlob(LZFEncoder.encode(value.getBytes())));
        preparedStatement.setString(3, childId != null ? childId : blankChildId);
        return preparedStatement;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
