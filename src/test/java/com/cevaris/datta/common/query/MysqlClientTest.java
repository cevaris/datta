package com.cevaris.datta.common.query;

import com.twitter.util.Await;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static org.easymock.EasyMock.*;

public class MysqlClientTest {

    private final Connection mockConnection = EasyMock.createMock(Connection.class);

    @Before
    public void setUp() {
        EasyMock.reset(mockConnection);
    }

    @Test
    public void testIsConnectedDefaultFalse() throws Exception {
        MysqlClient testClient = new MysqlClient(mockConnection);
        Assert.assertFalse(Await.result(testClient.isConnected()));
    }

    @Test
    public void testIsConnected() throws Exception {
        expect(mockConnection.isClosed()).andReturn(true);
        replay(mockConnection);

        MysqlClient testClient = new MysqlClient(mockConnection);
        Assert.assertTrue(Await.result(testClient.isConnected()));

        verify(mockConnection);
    }

//    @Test
//    public void testClose() throws Exception {
//        mockConnection.close();
//        expectLastCall().andVoid();
//        replay(mockConnection);
//
//        MysqlClient testClient = new MysqlClient(mockConnection);
//        Await.result(testClient.close());
//
//        verify(mockConnection);
//    }

}