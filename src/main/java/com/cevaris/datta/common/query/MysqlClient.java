package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.QueryState;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.twitter.util.Function;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

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
                try {
                    Statement stmt = currConn.createStatement();

                    QueryState queryState = stmt.execute(query) ? QueryState.SUCCESS : QueryState.FAILURE;
                    ResultSet rs = stmt.getResultSet();

                    ArrayList<String> columnNameList = Lists.newArrayList();
                    Iterator<ResultRow> iterator = null;

                    if (rs != null) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        Map<Integer, String> columnMetaData = columnNames(rsmd);

                        columnNameList = new ArrayList<String>(columnMetaData.values());
                        iterator = new ResultRowIterator(rs, rsmd.getColumnCount());
                    }

                    return Future.value(new QueryResponse(iterator, columnNameList, queryState));
                } catch (SQLException e) {
                    return Future.exception(e);
                }
            }
        });

    }

    private Future<QueryResponse> executeUpdate(final String query) {

        return conn.flatMap(new Function<Connection, Future<QueryResponse>>() {
            public Future<QueryResponse> apply(Connection currConn) {
                try {
                    Statement stmt = currConn.createStatement();
                    Boolean rs = stmt.execute(query);
                    QueryState queryState = rs ? QueryState.SUCCESS : QueryState.FAILURE;
                    return Future.value(new QueryResponse(queryState));
                } catch (SQLException e) {
                    return Future.exception(e);
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
                return !rs.isLast();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        public ResultRow next() {
            try {
                if (this.hasNext() && rs.next()) {

                    List<String> results = new ArrayList<String>();
                    for (int i = 1; i <= columnCount; i++) {
                        results.add(rs.getString(i));
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
