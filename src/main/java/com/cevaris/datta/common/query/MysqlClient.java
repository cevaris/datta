package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.QueryState;
import com.cevaris.datta.common.query.response.ResultItem;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.twitter.util.Function;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MysqlClient implements GenericClient {
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

                    Iterator<ResultRow> iterator = new ArrayList<ResultRow>().iterator();

                    if (rs != null) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        iterator = new ResultRowIterator(rs, rsmd);
                    }

                    return Future.value(new QueryResponse(iterator, queryState));
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
                    return Future.value(!currConn.isClosed());
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

    private static final class ResultRowIterator implements Iterator<ResultRow> {
        private final ResultSet rs;
        private final ResultSetMetaData rsmd;

        public ResultRowIterator(ResultSet rs, ResultSetMetaData rsmd) {
            this.rs = rs;
            this.rsmd = rsmd;
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

                    List<ResultItem> results = Lists.newArrayList();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        // TODO: Convert rsmd.getColumnType(int i) to Class
                        results.add(new ResultItem(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i))));
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
