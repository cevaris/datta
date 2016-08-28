package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.twitter.util.Await;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class MongoDbClientIntegrationTest {

    private BaseClient testClient;

    @Before
    public void setUp() throws Exception {
        testClient = newTestClient();

        Await.result(testClient.execute(mongoTruncate));
    }

    @Test
    public void testIsConnectedDefaultFalse() throws Exception {
        Assert.assertTrue(Await.result(testClient.isConnected()));
    }

    @Test
    public void testQueryEmptyResultRows() throws Exception {
        QueryResponse qr = Await.result(
                testClient.execute(mongoSelectAll)
        );
        Assert.assertNotNull(qr.iterator());
    }

    @Test
    public void testQueryResultRows() throws Exception {
        Await.result(testClient.execute(mongoInsertData));

        QueryResponse qr = Await.result(
                testClient.execute(mongoSelectAll)
        );
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(2, actual.size());
    }

    @Test
    public void testQueryResultRow() throws Exception {
        Await.result(testClient.execute(mongoInsertData));

        QueryResponse qr = Await.result(
                testClient.execute(mongoSelectAll)
        );
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(2, actual.size());
    }

    private BaseClient newTestClient() {
        ConnectionFactory conn = new ConnectionFactory(
                "localhost", "test_db", 27017, "test_user", "$EcrEt0$aucE"
        );
        return conn.newInstance(ConnectionType.MONGO_DB);
    }

    private String mongoSelectAll = "{ find: \"test_table\", batchSize: 1 }";
    private String mongoCount = "{ count: \"test_table\", HH: 1 }";
    private String mongoTruncate = "{ delete: \"test_table\", deletes: [ { q: { }, limit: 0 } ],}";
    private String mongoInsertData = "{" +
            "   insert: \"test_table\" " +
            "   documents: [" +
            "      { test_string: \"abc\", test_long: NumberLong(11), test_double: 11.11, test_bool: true,  test_array: [11,11], test_bin: \"MTE=\", test_date: new Date(1321096271), }," +
            "      { test_string: \"cdf\", test_long: NumberLong(12), test_double: 12.12, test_bool: false, test_array: [12,12], test_bin: \"MTI=\", test_date: new Date(1355314332), }," +
            "   ]" +
            "}";
}
