package com.cevaris.datta.common.query.response;


import java.util.Iterator;
import java.util.List;

public class QueryResponse implements Iterable<ResultRow> {
    private final Iterator<ResultRow> response;
    private final List<String> columnNames;
    private final QueryState queryState;

    public QueryResponse(Iterator<ResultRow> response, List<String> columnNames, QueryState queryState) {
        this.response = response;
        this.columnNames = columnNames;
        this.queryState = queryState;
    }

    public QueryResponse(QueryState queryState) {
        this.response = null;
        this.columnNames = null;
        this.queryState = queryState;
    }

    public Iterator<ResultRow> iterator() {
        return response;
    }
}