package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.twitter.util.Await;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class CassandraClientIntegrationTest {

    private GenericClient testClient;

    @BeforeClass
    public static void setUpClass() throws Exception {
        GenericClient testClient = newTestClient();
        Await.result(testClient.execute(cqlCreateTable));
    }

    @Before
    public void setUp() throws Exception {
        testClient = newTestClient();

//        Await.result(testClient.execute(cqlTruncate));
    }

    @Test
    public void testIsConnectedDefaultFalse() throws Exception {
        Assert.assertTrue(Await.result(testClient.isConnected()));
    }

    @Test
    public void testQueryEmptyResultRows() throws Exception {
        QueryResponse qr = Await.result(
                testClient.execute(cqlSelectAll)
        );
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(0, actual.size());
    }

    @Test
    public void testQueryResultRows() throws Exception {
        Await.result(testClient.execute(cqlInsertTestData));

        QueryResponse qr = Await.result(
                testClient.execute(cqlSelectAll)
        );
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(2, actual.size());
    }
//
//    @Test
//    public void testQueryCount() throws Exception {
//        Await.result(testClient.execute(cqlInsertTestData));
//
//        QueryResponse qr = Await.result(testClient.execute(cqlCount));
//        Assert.assertNotNull(qr.iterator());
//
//        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
//        Assert.assertNotNull(actual);
//        Assert.assertEquals(1, actual.size());
//    }

    private static GenericClient newTestClient() {
        ConnectionFactory conn = new ConnectionFactory(
                "localhost", "test_db", 9042, "test_user", "$EcrEt0$aucE"
        );
        return conn.newInstance(ConnectionType.CASSANDRA);
    }

    private static String cqlSelectAll = "SELECT * FROM test_table";

    private static String cqlCount = "SELECT count(1) FROM test_table";

    private static String cqlTruncate = "TRUNCATE test_table";

    private static String cqlCreateTable = "" +
            "CREATE COLUMNFAMILY IF NOT EXISTS test_table (test_long bigint, test_text text, test_timestamp timestamp, test_decimal decimal, test_blob blob, test_boolean boolean, test_map map<int, decimal>, test_list list<bigint>, test_set set<text>, PRIMARY KEY(test_long));";

    private static String cqlInsertPrefix = "INSERT INTO test_table (test_long, test_text, test_timestamp, test_decimal, test_blob, test_boolean, test_map, test_list, test_set) VALUES ";
    private static String cqlInsertTestData = "" +
            "BEGIN BATCH " +
            cqlInsertPrefix + "(11, 'text-11', '1111111111111', 11.11, bigintAsBlob(11), true, {11: 11.11}, [11, 11], {'11', '11'});" +
            cqlInsertPrefix + "(12, 'text-12', '1212121212121', 12.12, bigintAsBlob(12), true, {12: 12.12}, [12, 12], {'12', '12'});" +
            "APPLY BATCH;";

}
