package com.cevaris.datta.common.query;

import com.datastax.driver.core.Cluster;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

public class ConnectionFactory {
    private final String host;
    private final String database;
    private final Integer port;
    private final String user;
    private final String password;

    public ConnectionFactory(String host, String database, Integer port, String user, String password) {
        this.host = host;
        this.database = database;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public GenericClient newInstance(ConnectionType type) {

        switch (type) {
            case CASSANDRA:
                Cluster cluster = Cluster.builder()
                        .addContactPointsWithPorts(new InetSocketAddress(host, port))
                        .withCredentials(user, password)
                        .build();
                return new CassandraClient(cluster);
            case MONGO_DB:
                ServerAddress sa = new ServerAddress(host, port);
                MongoCredential credential = MongoCredential.createCredential(user, database, password.toCharArray());
                return new MongoDbClient(new MongoClient(sa, Collections.singletonList(credential)), database);
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
                host, port, database, user, password
        );
    }
}
