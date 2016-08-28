package com.cevaris.datta.common.query.response;

public class ResultItem {
    private final String key;
    private final String value;
    private final Class type;

    public ResultItem(String key, String value) {
        this.key = key;
        this.value = value;
        this.type = String.class;
    }

    public ResultItem(String value) {
        this.key = "";
        this.value = value;
        this.type = String.class;
    }

    public ResultItem(String key, String value, Class type) {
        this.key = key;
        this.value = value;
        this.type = type;
    }
}
