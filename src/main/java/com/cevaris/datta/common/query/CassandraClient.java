package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.datastax.driver.core.Cluster;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

public class CassandraClient implements GenericClient {
    private final Future<Cluster> conn;

    public CassandraClient(Cluster conn) {
        this.conn = Future.value(conn);
    }

    public Future<BoxedUnit> close() {
        return Future.exception(new UnsupportedOperationException());
    }

    public Future<Boolean> isConnected() {
        return Future.exception(new UnsupportedOperationException());
    }

    public Future<QueryResponse> execute(String query) {
        return Future.exception(new UnsupportedOperationException());
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
}
