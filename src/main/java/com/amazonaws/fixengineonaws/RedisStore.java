//package com.amazonaws.fixengineonaws;
package com.amazonaws.fixengineonaws;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import redis.clients.jedis.Jedis;

import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SystemTime;

class RedisStore implements quickfix.MessageStore {
    private final SessionID sessionID;
    private final String sessionTableName;
    private final String messageTableName;
    
    private final String defaultSessionIdPropertyValue;

    private final Jedis jedisForInitRedis;
    private final Jedis jedisForGetCreationTime;
    private final Jedis jedisForGetNextSenderMsgSeqNum;
    private final Jedis jedisForGetNextTargetMsgSeqNum;
    private final Jedis jedisForSetNextSenderMsgSeqNum;
    private final Jedis jedisForSetNextTargetMsgSeqNum;
    private final Jedis jedisForReset;
    private final Jedis jedisForGet;
    private final Jedis jedisForSet;

    private static final String SESSION_CREATION_TIME_FIELD = "creation_time";
    private static final String SESSION_TARGET_INCOMING_SEQNUM_FIELD = "incoming_seqnum";
    private static final String SESSION_SENDER_OUTGOING_SEQNUM_FIELD = "outgoing_seqnum";

    public RedisStore(SessionSettings settings, SessionID sessionID, Jedis jedisParam) throws Exception {
        this.sessionID = sessionID;

        String sessionUniqueId = getSessionUniqueId();
        
        if (settings.isSetting(sessionID, RedisSetting.SETTING_REDIS_STORE_SESSIONS_TABLE_NAME)) {
            sessionTableName = settings.getString(sessionID, RedisSetting.SETTING_REDIS_STORE_SESSIONS_TABLE_NAME)+sessionUniqueId;
        } else {
            sessionTableName = "sessions"+sessionUniqueId;
        }
        
        if (settings.isSetting(sessionID, RedisSetting.SETTING_REDIS_STORE_MESSAGES_TABLE_NAME)) {
        	messageTableName = settings.getString(sessionID, RedisSetting.SETTING_REDIS_STORE_MESSAGES_TABLE_NAME)+sessionUniqueId;
        } else {
        	messageTableName = "messages"+sessionUniqueId;
        }
        
        String memoryDbHost = RedisSetting.DEFAULT_REDIS_HOST;
        if (settings.isSetting(sessionID, RedisSetting.SETTING_REDIS_HOST)) {
        	memoryDbHost = settings.getString(sessionID, RedisSetting.SETTING_REDIS_HOST);
        }

        int memoryDbPort = Integer.parseInt(RedisSetting.DEFAULT_REDIS_PORT);
        if (settings.isSetting(sessionID, RedisSetting.SETTING_REDIS_PORT)) {
        	memoryDbPort = Integer.parseInt(settings.getString(sessionID, RedisSetting.SETTING_REDIS_PORT));
        }

        defaultSessionIdPropertyValue = SessionID.NOT_SET;

// TODO: Decide if any passed in jedis store is safe to reuse - I don't think it is!  
//        jedis = jedisParam == null ? new Jedis(memoryDbHost, memoryDbPort) : jedisParam;
        jedisForInitRedis  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForGetCreationTime  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForGetNextSenderMsgSeqNum  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForGetNextTargetMsgSeqNum  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForSetNextSenderMsgSeqNum  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForSetNextTargetMsgSeqNum  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForReset  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForGet  = new Jedis(memoryDbHost, memoryDbPort);
        jedisForSet  = new Jedis(memoryDbHost, memoryDbPort);
        
//        DECIDE IF WE WANT TO KEEP THE CONNECTION OPEN FOREVER OR RECONNECT ON EVERY METHOD CALL LIKE Java did
//        If you have started one Redis service in your local machine and on default port (6379) then default constructor will just work fine. 
//        Otherwise you have to pass correct host url and port no. as an argument into the constructor.
        initRedis();
    }
    
    private String getSqlValue(String javaValue, String defaultSqlValue) {
        return !SessionID.NOT_SET.equals(javaValue) ? javaValue : defaultSqlValue;
    }

    private String getSessionUniqueId() {
    	StringBuilder sessionUniqueId = new StringBuilder();
//    	sessionUniqueId.append("ENGINE_ID=").append(		getSqlValue(ENGINE_ID, 							defaultSessionIdPropertyValue));
    	sessionUniqueId.append("beginstring=").append(		getSqlValue(sessionID.getBeginString(), 		defaultSessionIdPropertyValue));
		sessionUniqueId.append("sendercompid=").append(		getSqlValue(sessionID.getSenderCompID(), 		defaultSessionIdPropertyValue));
		sessionUniqueId.append("sendersubid=").append(		getSqlValue(sessionID.getSenderSubID(), 		defaultSessionIdPropertyValue));
		sessionUniqueId.append("senderlocid=").append(		getSqlValue(sessionID.getSenderLocationID(), 	defaultSessionIdPropertyValue));
		sessionUniqueId.append("targetcompid=").append(		getSqlValue(sessionID.getTargetCompID(), 		defaultSessionIdPropertyValue));
		sessionUniqueId.append("targetsubid=").append(		getSqlValue(sessionID.getTargetSubID(), 		defaultSessionIdPropertyValue));
		sessionUniqueId.append("targetlocid=").append(		getSqlValue(sessionID.getTargetLocationID(), 	defaultSessionIdPropertyValue));
		sessionUniqueId.append("session_qualifier=").append(getSqlValue(sessionID.getSessionQualifier(), 	defaultSessionIdPropertyValue));
        return sessionUniqueId.toString();
    }

    private void initRedis() throws IOException {
        try {
        	if (jedisForInitRedis.exists(sessionTableName)) {
            	System.out.println("*** SESSION ALREADY EXISTS! creation_time = " + jedisForInitRedis.hget(sessionTableName, SESSION_CREATION_TIME_FIELD) 
            						+ "; incoming_seqnum = " + jedisForInitRedis.hget(sessionTableName, SESSION_TARGET_INCOMING_SEQNUM_FIELD) 
            						+ "; outgoing_seqnum = " + jedisForInitRedis.hget(sessionTableName, SESSION_SENDER_OUTGOING_SEQNUM_FIELD));
            } else {
            	jedisForInitRedis.del(messageTableName); // no need to recreate this - first lpush will do that

                HashMap<String,String> sessionTableMap = new HashMap<String, String>();
                Calendar creationTime = SystemTime.getUtcCalendar();
            	sessionTableMap.put(SESSION_CREATION_TIME_FIELD,String.valueOf(creationTime.getTimeInMillis()));
            	sessionTableMap.put(SESSION_TARGET_INCOMING_SEQNUM_FIELD,"1");
            	sessionTableMap.put(SESSION_SENDER_OUTGOING_SEQNUM_FIELD,"1");
            	jedisForInitRedis.hmset(sessionTableName, sessionTableMap);
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public Date getCreationTime() throws IOException {
    	Calendar creationTime = SystemTime.getUtcCalendar();
    	creationTime.setTimeInMillis(Long.parseLong((jedisForGetCreationTime.hget(sessionTableName, SESSION_CREATION_TIME_FIELD))));
    	return creationTime.getTime();
    }

    public int getNextSenderMsgSeqNum() throws IOException {
    	String val = jedisForGetNextSenderMsgSeqNum.hget(sessionTableName, SESSION_SENDER_OUTGOING_SEQNUM_FIELD);
        return Integer.parseInt(val);
    }

    public int getNextTargetMsgSeqNum() throws IOException {
    	String val = jedisForGetNextTargetMsgSeqNum.hget(sessionTableName, SESSION_TARGET_INCOMING_SEQNUM_FIELD);
        return Integer.parseInt(val);
    }

    public void setNextSenderMsgSeqNum(int next) throws IOException {
    	jedisForSetNextSenderMsgSeqNum.hset(sessionTableName, SESSION_SENDER_OUTGOING_SEQNUM_FIELD, String.valueOf(next));
    }

    public void setNextTargetMsgSeqNum(int next) throws IOException {
    	jedisForSetNextTargetMsgSeqNum.hset(sessionTableName, SESSION_TARGET_INCOMING_SEQNUM_FIELD, String.valueOf(next));
    }
        
    public void incrNextSenderMsgSeqNum() throws IOException {
    	setNextSenderMsgSeqNum(getNextSenderMsgSeqNum()+1);
    }

    public void incrNextTargetMsgSeqNum() throws IOException {
    	setNextTargetMsgSeqNum(getNextTargetMsgSeqNum()+1);
    }

    public void reset() throws IOException {
        try {
        	jedisForReset.del(messageTableName);
        	jedisForReset.del(sessionTableName);
        	initRedis();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public void get(int startSequence, int endSequence, Collection<String> messages) throws IOException {
        try {
        	List<String> msgs = jedisForGet.lrange(messageTableName, startSequence-1, endSequence-1);
        	msgs.removeAll(Collections.singleton(""));
        	messages.addAll(msgs);
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

   
    public boolean set(int sequence, String message) throws IOException {   	
        try {
        	for (long i = jedisForSet.llen(messageTableName); i < sequence; i++) { // fill in any missing items between list end and new sequence #
        		jedisForSet.rpush(messageTableName, "");
        	}
        	jedisForSet.lset(messageTableName, sequence-1, message); //THIS IS AN UPSERT!
        } catch (Exception e) {
        	throw new IOException(e.getMessage(), e);
        }
        return true;
    }
    
    public void refresh() throws IOException {
    	return; //nothing to do since there's nothing in memory to refresh and Redis is the sole copy of the data  
    }
    
    public static void main(String[] in) {
    	System.out.println("MAIN!!");
    	try {
    		System.out.println("STARTING MAIN TEST");
    		SessionSettings settings = new SessionSettings(); 
    		System.out.println("CREATED SessionSettings");
    		SessionID sessionID = new SessionID("beginString:beginString:senderCompID:senderSubID:senderLocationID->targetCompID:targetSubID:targetLocationID:sessionQualifier");
    		System.out.println("CREATED SessionID");
    		RedisStore me = new RedisStore(settings, sessionID, null);
    		System.out.println("CREATED RedisStore");
    		me.set(1, "TEST_MESSAGE_1");
    		me.set(2, "TEST_MESSAGE_2");
    		me.set(3, "TEST_MESSAGE_3");
    		me.set(4, "TEST_MESSAGE_4");
    		me.set(5, "TEST_MESSAGE_5");
    		System.out.println("POPULATED 5 MESSAGES");
    		ArrayList<String> msgs = new ArrayList<String>();
    		me.get(2, 4, msgs);
    		System.out.println("RETREIVED MESSAGES 2-4: " + msgs);
    	} catch(Exception e) {
        	System.out.println("CAUGHT EXCEPTION " + e);
        	e.printStackTrace();
    	}
    	System.out.println("MAIN END!!");
    }
}