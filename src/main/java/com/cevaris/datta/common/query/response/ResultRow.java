package com.cevaris.datta.common.query.response;

import java.util.List;

public class ResultRow {
    final List<String> rowData;

    public ResultRow(List<String> rowData) {
        this.rowData = rowData;
    }

    @Override
    public String toString() {
        return rowData.toString();
    }
}
