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

    private BaseClient testClient;

    @Before
    public void setUp() {
        testClient = newTestClient();
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

        List<ResultRow> actual = Lists.newArrayList(qr.iterator());
        Assert.assertNotNull(actual);
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
    }

    private BaseClient newTestClient() {
        ConnectionFactory conn = new ConnectionFactory(
                "localhost", "test_db", 3306, "test_user", "$EcrEt0$aucE"
        );
        return conn.newInstance(ConnectionType.MYSQL);
    }

    private String sqlSelectAll = "SELECT * FROM test_table";

    private String sqlInsertTestData = new StringBuilder()
            .append("INSERT INTO `test_table` (`test_varchar`, `test_char`, `test_datetime`, `test_int`, `test_float`, `test_blob`) VALUES")
            .append("('abc','a','2016-08-27 19:06:26',11,1.232,NULL),")
            .append("('cdf','b','2016-08-27 19:08:48',12,1.212,X'3078303132')")
            .toString();

}
