//package com.amazonaws.fixengineonaws;
package com.amazonaws.fixengineonaws;


import quickfix.MessageStore;
import quickfix.MessageStoreFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import redis.clients.jedis.Jedis;

/**
 * Creates a generic JDBC message store.
 */
public class RedisStoreFactory implements MessageStoreFactory {
    private final SessionSettings settings;
    private Jedis jedis;

    /**
     * Create a factory using session settings.
     */
    public RedisStoreFactory(SessionSettings settings) {
        this.settings = settings;
    }

    /**
     * Create a JDBC message store.
     *
     * @param sessionID the sessionID for the message store.
     */
    public MessageStore create(SessionID sessionID) {
        try {
            return new RedisStore(settings, sessionID, jedis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used to support the MySQL-specific class (JNI compatibility)
     *
     * @return the session settings
     */
    protected SessionSettings getSettings() {
        return settings;
    }

    public void setDataSource(Jedis jedis) {
        this.jedis = jedis;
    }
}
