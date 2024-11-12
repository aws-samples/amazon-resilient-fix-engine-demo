package com.amazonaws.fixengineonaws;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import redis.clients.jedis.Jedis;

import quickfix.AbstractMessageStoreTest;
import quickfix.ConfigError;
import quickfix.MessageStoreFactory;
import quickfix.SessionSettings;
import quickfix.SessionID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RedisStoreTest extends AbstractMessageStoreTest {
    private String initialContextFactory;
    
// TODO: generate real guid
    private static String ENGINE_ID = "ENGINE_ID_DEFAULT_THIS_TO_A_GUID";
    
    protected void setUp() throws Exception {
        initialContextFactory = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);
        initialContextFactory = System.getProperty(Context.PROVIDER_URL);
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "tyrex.naming.MemoryContextFactory");
        System.setProperty(Context.PROVIDER_URL, "TEST");
//        bindDataSource();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (initialContextFactory != null) {
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
        }
        RedisStore store = (RedisStore) getMessageStoreFactory("xsessions", "messages").create(getSessionID());
        store.reset();
        super.tearDown();
    }

//    private void bindDataSource() throws NamingException {
//        new InitialContext().rebind("TestDataSource", getDataSource());
//    }

    protected MessageStoreFactory getMessageStoreFactory() throws ConfigError, IOException {
        return getMessageStoreFactory(null, null);
    }

    private RedisStoreFactory getMessageStoreFactory(String sessionTableName, String messageTableName)
            throws ConfigError, IOException {
        SessionSettings settings = new SessionSettings();
        
//        if (settings.isSetting(sessionID, SETTING_ENGINE_ID)) {
//        	ENGINE_ID = settings.getString(sessionID, SETTING_ENGINE_ID);
//        }
 

// TODO: USE SessionSettings sessionTableName and messageTableName to pass unique IDs from factory constructor to store!
        
        settings.setString(RedisSetting.SETTING_REDIS_HOST, RedisSetting.DEFAULT_REDIS_HOST);
        settings.setString(RedisSetting.SETTING_REDIS_PORT, RedisSetting.DEFAULT_REDIS_PORT);

        if (sessionTableName != null) {
            settings.setString(RedisSetting.SETTING_REDIS_STORE_SESSIONS_TABLE_NAME, sessionTableName);
        }

        if (messageTableName != null) {
            settings.setString(RedisSetting.SETTING_REDIS_STORE_MESSAGES_TABLE_NAME, messageTableName);
        }

        return new RedisStoreFactory(settings);
    }

    public void testExplicitDataSource() throws Exception {
        RedisStoreFactory factory = new RedisStoreFactory(new SessionSettings());
        factory.setDataSource(getDataSource());
        factory.create(new SessionID("FIX4.4", "SENDER", "TARGET"));
    }

    public void testSequenceNumbersWithCustomSessionsTableName() throws Exception {
        RedisStore store = (RedisStore) getMessageStoreFactory("xsessions", "messages").create(getSessionID());
        store.reset();
        assertEquals("wrong value", 1, store.getNextSenderMsgSeqNum());
        assertEquals("wrong value", 1, store.getNextTargetMsgSeqNum());
    }

    public void testMessageStorageMessagesWithCustomMessagesTableName() throws Exception {
        RedisStore store = (RedisStore) getMessageStoreFactory("sessions", "xmessages").create(getSessionID());

        assertTrue("set failed", store.set(111, "message2"));
        assertTrue("set failed", store.set(113, "message1"));
        assertTrue("set failed", store.set(120, "message3"));

        ArrayList<String> messages = new ArrayList<>();
        store.get(100, 115, messages);
        assertEquals("wrong # of messages", 2, messages.size());
        assertEquals("wrong message", "message2", messages.get(0));
        assertEquals("wrong message", "message1", messages.get(1));
    }

    protected Jedis getDataSource() {
        try {
        	return new Jedis(RedisSetting.DEFAULT_REDIS_HOST, Integer.parseInt(RedisSetting.DEFAULT_REDIS_PORT));
        } catch (Exception e) {
// TODO: add redis local installation instructions
        	System.out.println("UNABLE TO CONSTRUCT Jedis OBJECT USING HOST " + RedisSetting.DEFAULT_REDIS_HOST + " AND PORT " + RedisSetting.DEFAULT_REDIS_PORT);
        	System.out.println("To test locally, please Install Ubuntu Linux and Redis on Windows: https://redis.io/docs/getting-started/installation/install-redis-on-windows/");
        	System.out.println("curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg");
			System.out.println("echo \"deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main\" | sudo tee /etc/apt/sources.list.d/redis.list");
			System.out.println("sudo apt-get update");
			System.out.println("sudo apt-get install redis");
			System.out.println("sudo service redis-server start");
			System.out.println("redis-cli");
			System.out.println("sudo service redis-server stop");
        	throw e;
        }
    }

    public void testCreationTime() throws Exception {
        RedisStore store = (RedisStore) getStore();
        
        Date creationTime = store.getCreationTime();

        store = (RedisStore) createStore();
        Date creationTime2 = store.getCreationTime();

        assertEquals("creation time not stored correctly", creationTime, creationTime2);
    }

    protected Class<RedisStore> getMessageStoreClass() {
        return RedisStore.class;
    }

    public void testMessageUpdate() throws Exception {
        RedisStore store = (RedisStore) getMessageStoreFactory().create(getSessionID());
        store.reset();

        assertTrue(store.set(1, "MESSAGE1"));
        assertTrue(store.set(1, "MESSAGE2"));

        List<String> messages = new ArrayList<>();
        store.get(1, 1, messages);
        assertEquals("MESSAGE2", messages.get(0));
    }
}
