/*******************************************************************************
+++ * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

//package com.amazonaws.fixengineonaws;
package com.amazonaws.fixengineonaws;


/**
 * Class for storing Redis (or AWS MemoryDB) setting constants for the message store classes.
 * Cloned from JdbcSetting class (in case you want to extend it to write logs/heartbeats to Redis too) 
 */
public class RedisSetting {

    /**
     * Setting name for Redis cluster endpoint hostname
     */
    public static final String SETTING_REDIS_HOST = "MemoryDBHost";

    /**
     * Setting name for Redis cluster endpoint port
     */
    public static final String SETTING_REDIS_PORT = "MemoryDBPort";

    /**
     * Defines the table name for the messages table. Default is "messages".
     * If you use a different name, you must set up your database accordingly.
     */
    public static final String SETTING_REDIS_STORE_MESSAGES_TABLE_NAME = "RedisStoreMessagesTableName";

    /**
     * Defines the table name for the session table. Default is "sessions".
     * If you use a different name, you must set up your database accordingly.
     */
    public static final String SETTING_REDIS_STORE_SESSIONS_TABLE_NAME = "RedisStoreSessionsTableName";

    public static final String DEFAULT_REDIS_HOST = "localhost";
    public static final String DEFAULT_REDIS_PORT = "6379";

    public static final String DEFAULT_REDIS_LEADER_LOCK_NAME = "FIX_ENGINE"; // TODO: This should be replaced by a GUID that is the stack ID
    public static final String DEFAULT_REDIS_FIX_TO_APP_QUEUE_NAME = "fix_to_app_queue";
    public static final String DEFAULT_REDIS_APP_TO_FIX_QUEUE_NAME = "app_to_fix_queue";

    public static final String SETTING_REDIS_LEADER_LOCK_NAME = "MemoryDBLeaderLock";
    public static final String SETTING_REDIS_FIX_TO_APP_QUEUE_NAME = "MemoryDBFixToAppQueueName";
    public static final String SETTING_REDIS_APP_TO_FIX_QUEUE_NAME = "MemoryDBAppToFixQueueName";


    
}
