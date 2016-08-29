package com.cevaris.datta.common.query;

import com.cevaris.datta.common.query.response.QueryResponse;
import com.cevaris.datta.common.query.response.ResultRow;
import com.google.common.collect.Lists;
import com.twitter.util.Await;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class MysqlClientIntegrationTest {

    private GenericClient testClient;

    @Before
    public void setUp() throws Exception {
        testClient = newTestClient();

        Await.result(testClient.execute(sqlTruncate));
    }

    @Test
    public void testIsConnectedDefaultFalse() throws Exception {
        Assert.assertFalse(Await.result(testClient.isConnected()));
    }

    @Test
    public void testQueryEmptyResultRows() throws Exception {
        QueryResponse qr = Await.result(
                testClient.execute("select * from test_table")
        );
        Assert.assertNotNull(qr.iterator());
    }

    @Test
    public void testQueryResultRows() throws Exception {
        Await.result(testClient.execute(sqlInsertTestData));

        QueryResponse qr = Await.result(
                testClient.execute(sqlSelectAll)
        );
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(2, actual.size());
    }

    @Test
    public void testQueryCount() throws Exception {
        Await.result(testClient.execute(sqlInsertTestData));

        QueryResponse qr = Await.result(testClient.execute(sqlCount));
        Assert.assertNotNull(qr.iterator());

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
        Assert.assertEquals(1, actual.size());
    }

    private GenericClient newTestClient() {
        ConnectionFactory conn = new ConnectionFactory(
                "localhost", "test_db", 3306, "test_user", "$EcrEt0$aucE"
        );
        return conn.newInstance(ConnectionType.MYSQL);
    }

    private String sqlSelectAll = "SELECT * FROM test_table";

    private String sqlCount = "SELECT count(1) FROM test_table";

    private String sqlTruncate = "TRUNCATE test_table";

    private String sqlInsertTestData = new StringBuilder()
            .append("INSERT INTO `test_table` (`test_varchar`, `test_char`, `test_datetime`, `test_int`, `test_float`, `test_blob`) VALUES")
            .append("('abc','a','2016-08-27 19:06:26',11,1.232,X'3078303131'),")
            .append("('cdf','b','2016-08-27 19:08:48',12,1.212,X'3078303132')")
            .toString();

}
