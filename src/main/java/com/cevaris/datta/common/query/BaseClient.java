package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

public interface BaseClient {

    abstract public Future<BoxedUnit> close();

    abstract public Future<Boolean> isConnected();

    abstract public Future<QueryResponse> execute(String query);

    abstract public Future<QueryResponse> all();

    abstract public Future<QueryResponse> sortAllBy(String attribute);

    abstract public Future<QueryResponse> update(String attribute, String value);

}
