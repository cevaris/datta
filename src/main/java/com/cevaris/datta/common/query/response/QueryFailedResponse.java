package com.cevaris.datta.common.query.response;

public class QueryFailedResponse extends RuntimeException {
    public QueryFailedResponse(String message, Throwable t) {
        super(message, t);
    }
}
