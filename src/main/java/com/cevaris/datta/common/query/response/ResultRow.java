package com.cevaris.datta.common.query.response;

import java.util.List;

public class ResultRow {
    final List<ResultItem> row;

    public ResultRow(List<ResultItem> row) {
        this.row = row;
    }

    @Override
    public String toString() {
        return row.toString();
    }
}
