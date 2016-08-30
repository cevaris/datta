package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.QueryState;
import com.cevaris.datta.common.query.response.ResultItem;
import com.cevaris.datta.common.query.response.ResultRow;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.twitter.util.Function;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.datastax.driver.core.ColumnDefinitions.Definition;

/**
 * https://github.com/datastax/java-driver/tree/3.0/manual
 */
public class CassandraClient implements GenericClient {
    private final Future<Cluster> conn;
    private final String keyspace;

    public CassandraClient(Cluster conn, String keyspace) {
        this.conn = Future.value(conn);
        this.keyspace = keyspace;
    }

    public Future<BoxedUnit> close() {
        return conn.flatMap(new Function<Cluster, Future<BoxedUnit>>() {
            public Future<BoxedUnit> apply(Cluster currConn) {
                currConn.close();
                return null;
            }
        });
    }

    public Future<Boolean> isConnected() {
        return conn.flatMap(new Function<Cluster, Future<Boolean>>() {
            public Future<Boolean> apply(Cluster currConn) {
                return Future.value(!currConn.isClosed());
            }
        });
    }

    public Future<QueryResponse> execute(final String query) {
        return conn.flatMap(new Function<Cluster, Future<QueryResponse>>() {
            public Future<QueryResponse> apply(Cluster currConn) {

                Session session = currConn.connect(keyspace);
                ResultSet rs = session.execute(query);

                return Future.value(new QueryResponse(new ResultRowIterator(rs), QueryState.SUCCESS));
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

    private static final class ResultRowIterator implements Iterator<ResultRow> {
        private final ResultSet rs;

        public ResultRowIterator(ResultSet rs) {
            this.rs = rs;
        }

        public boolean hasNext() {
            return !rs.isExhausted();
        }

        public ResultRow next() {
            Row row = rs.one();

            List<ResultItem> results = Lists.newArrayList();
            for (Definition definition : row.getColumnDefinitions()) {
                // TODO: Convert definition.getType() to Class
                results.add(new ResultItem(definition.getName(), row.getString(definition.getName())));
            }

            throw new NoSuchElementException();
        }
    }
}
