package com.cevaris.datta.common.query.response;

import java.nio.ByteBuffer;
import java.util.List;

public class ResultRow {
    final List<ByteBuffer> rowData;

    public ResultRow(List<ByteBuffer> rowData) {
        this.rowData = rowData;
    }
}
