package com.cevaris.datta.common.query.response;


import java.util.Iterator;
import java.util.List;

public class QueryResponse implements Iterable<ResultRow> {
    private final Iterator<ResultRow> response;
    private final List<String> columnNames;

    public QueryResponse(Iterator<ResultRow> response, List<String> columnNames) {
        this.response = response;
        this.columnNames = columnNames;
    }

    public Iterator<ResultRow> iterator() {
        return response;
    }
}