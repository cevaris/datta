package com.cevaris.datta.common.query;

import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    private final String host;
    private final String database;
    private final Integer port;
    private final String user;
    private final String pw;

    public ConnectionFactory(String host, String database, Integer port, String user, String pw) {
        this.host = host;
        this.database = database;
        this.port = port;
        this.user = user;
        this.pw = pw;
    }

    public BaseClient newInstance(ConnectionType type) {

        switch (type) {
            case MYSQL:
                try {
                    return new MysqlClient(DriverManager.getConnection(buildFullMysqlJdbcConnection()));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            default:
                throw new RuntimeException("failed");
        }
    }

    private String buildFullMysqlJdbcConnection() {
        return String.format(
                "jdbc:mysql://%s:%d/%s?user=%s&password=%s&serverTimezone=UTC",
                host, port, database, user, pw
        );
    }
}
