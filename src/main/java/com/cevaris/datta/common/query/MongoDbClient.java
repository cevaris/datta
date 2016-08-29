package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.QueryState;
import com.cevaris.datta.common.query.response.ResultItem;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoDatabase;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.bson.Document;
import scala.runtime.BoxedUnit;

import java.util.*;

/**
 * Example Mongo Document Iterator: http://goo.gl/ACAVJ5
 * BJSON type/definition:
 * - https://docs.mongodb.com/manual/reference/mongodb-extended-json/
 * - https://docs.mongodb.com/manual/reference/bson-types/
 * Mongo 3.0 Commands: https://docs.mongodb.com/manual/reference/command
 */
public class MongoDbClient implements BaseClient {

    public static final int DEFAULT_PORT = 27017;

    private static final String BATCH_SIZE = "batchSize";
    private static final String COLLECTION = "collection";
    private static final String FIRST_BATCH = "firstBatch";
    private static final String NEXT_BATCH = "nextBatch";
    private static final String GET_MORE = "getMore";
    private static final String ID = "id";
    private static final String _ID = "_id";
    private static final String PING = "ping";
    private static final String NCOUNT = "n";
    private static final String NAMESPACE = "ns";
    private static final String RESULT = "result";
    private static final String CURSOR = "cursor";


    private final Future<MongoClient> conn;
    private final String database;

    public MongoDbClient(MongoClient conn, String database) {
        this.conn = Future.value(conn);
        this.database = database;
    }

    public Future<BoxedUnit> close() {
        return Future.exception(new UnsupportedOperationException());
    }

    public Future<Boolean> isConnected() {
        return conn.flatMap(new Function<MongoClient, Future<Boolean>>() {
            public Future<Boolean> apply(MongoClient currConn) {
                try {
                    Document ping = new Document(PING, 1);
                    currConn.getDatabase(database).runCommand(ping);
                    return Future.value(true);
                } catch (MongoException e) {
                    return Future.value(false);
                }
            }
        });
    }

    public Future<QueryResponse> execute(final String query) {
        return conn.flatMap(new Function<MongoClient, Future<QueryResponse>>() {

            public Future<QueryResponse> apply(MongoClient currConn) {
                try {
                    MongoDatabase db = currConn.getDatabase(database);
                    Document resultDoc = db.runCommand(Document.parse(query));


                    Iterator<ResultRow> resultRows = new ArrayList<ResultRow>().iterator();

                    if (resultDoc.containsKey(RESULT)) {
                        List<Document> docs = (ArrayList<Document>) resultDoc.get(RESULT);
                        resultRows = transformDocuments(docs).iterator();
                    } else if (resultDoc.containsKey(CURSOR)) {
                        Document docCursor = (Document) resultDoc.get(CURSOR);
                        resultRows = new ResultRowIterator(docCursor, db);
                    } else if (resultDoc.containsKey(NCOUNT)) {
                        resultRows = transformDocument(resultDoc).iterator();
                    }

                    return Future.value(new QueryResponse(resultRows, QueryState.FAILURE));
                } catch (MongoException e) {
                    return Future.exception(e);
                }
            }
        });
    }

    private static List<ResultRow> transformDocument(Document doc) {
        List<ResultRow> rows = Lists.newArrayList();
        List<ResultItem> row = Lists.newArrayList();
        row.add(new ResultItem(doc.toJson()));
        rows.add(new ResultRow(row));
        return rows;
    }

    private static List<ResultRow> transformDocuments(List<Document> docs) {
        List<ResultRow> rows = Lists.newArrayList();
        for (Document doc : docs) {
            List<ResultItem> row = Lists.newArrayList();
            row.add(new ResultItem(doc.toJson()));
            rows.add(new ResultRow(row));
        }
        return rows;
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
        private Document docCursor;
        private Long nextBatchId;
        private MongoNamespace namespace;
        private Integer batchSize;

        private final MongoDatabase db;
        private final Queue<ResultRow> batchBuffer;

        public ResultRowIterator(Document docCursor, MongoDatabase db) {
            this.docCursor = docCursor;
            this.nextBatchId = docCursor.getLong(ID);
            this.namespace = new MongoNamespace((String) docCursor.get(NAMESPACE));
            this.db = db;
            this.batchBuffer = Queues.newConcurrentLinkedQueue(transformDocuments((ArrayList<Document>) docCursor.get(FIRST_BATCH)));
            this.batchSize = this.batchBuffer.size();
        }

        public boolean hasNext() {

            if (batchBuffer.isEmpty()) {
                Document fetchNextBatchDoc = new Document();
                fetchNextBatchDoc.append(GET_MORE, nextBatchId);
                fetchNextBatchDoc.append(COLLECTION, namespace.getCollectionName());
                fetchNextBatchDoc.append(BATCH_SIZE, batchSize);

                Document nextBatchData = db.runCommand(fetchNextBatchDoc);
                docCursor = (Document) nextBatchData.get(CURSOR);

                nextBatchId = docCursor.getLong(ID);
                this.batchBuffer.addAll(transformDocuments((ArrayList<Document>) docCursor.get(NEXT_BATCH)));
            }

            return !batchBuffer.isEmpty();
        }

        public ResultRow next() {
            if (batchBuffer.peek() != null) {
                return batchBuffer.poll();
            }

            throw new NoSuchElementException();
        }
    }
}
