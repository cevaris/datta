package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.ResultRow;
import com.twitter.util.Function;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

public class MysqlClient implements BaseClient {
    private final Future<Connection> conn;

    public MysqlClient(Connection conn) {
        this.conn = Future.value(conn);
    }

    public Future<QueryResponse> execute(final String query) {

        return conn.flatMap(new Function<Connection, Future<QueryResponse>>() {
            public Future<QueryResponse> apply(Connection currConn) {
                Statement stmt = null;
                try {

                    stmt = currConn.createStatement();
                    stmt.execute(query);

                    ResultSet rs = stmt.executeQuery(query);
                    ResultSetMetaData rsmd = rs.getMetaData();

                    Map<Integer, String> columnMetaData = columnNames(rsmd);
                    ArrayList<String> columnNameList = new ArrayList<String>(columnMetaData.values());

                    Iterator<ResultRow> iterator = new ResultRowIterator(rs, rsmd.getColumnCount());

                    return Future.value(new QueryResponse(iterator, columnNameList));
                } catch (SQLException e) {
                    return Future.exception(e);
                } finally {
//                    try {
//                        if (stmt != null)
//                            stmt.close();
//                    } catch (SQLException se2) {
//                        // nothing we can do
//                    }
                }
            }
        });

    }


    public Future<QueryResponse> all() {
        return Future.exception(new UnsupportedOperationException());
    }


    public Future<QueryResponse> sortAllBy(String attribute) {
        return Future.exception(new UnsupportedOperationException());
    }


    public Future<QueryResponse> update(String attribute, String value) {
        return Future.exception(new UnsupportedOperationException());
    }

    public Future<Boolean> isConnected() {
        return conn.flatMap(new Function<Connection, Future<Boolean>>() {
            public Future<Boolean> apply(Connection currConn) {
                try {
                    return Future.value(currConn.isClosed());
                } catch (SQLException e) {
                    return Future.exception(e);
                }
            }
        });
    }

    public Future<BoxedUnit> close() {
        return conn.flatMap(new Function<Connection, Future<BoxedUnit>>() {
            public Future<BoxedUnit> apply(Connection currConn) {
                try {
                    currConn.close();
                    return null;
                } catch (SQLException e) {
                    return Future.exception(e);
                }
            }
        });
    }

    private Map<Integer, String> columnNames(ResultSetMetaData rsmd) throws SQLException {
        Map<Integer, String> columMetaData = new HashMap<Integer, String>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            columMetaData.put(i, rsmd.getColumnName(i));
        }
        return columMetaData;

    }

    private static final class ResultRowIterator implements Iterator<ResultRow> {
        private final ResultSet rs;
        private final int columnCount;

        public ResultRowIterator(ResultSet rs, int columnCount) throws SQLException {
            this.rs = rs;
            this.columnCount = columnCount;
        }

        public boolean hasNext() {
            try {
                return rs.next();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        public ResultRow next() {
            try {
                if (this.hasNext()) {

                    List<ByteBuffer> results = new ArrayList<ByteBuffer>();
                    for (int i = 1; i <= columnCount; i++) {
                        results.add(ByteBuffer.wrap(rs.getBytes(i)));
                    }

                    return new ResultRow(results);

                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                this.rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            
            throw new NoSuchElementException();
        }
    }

}
