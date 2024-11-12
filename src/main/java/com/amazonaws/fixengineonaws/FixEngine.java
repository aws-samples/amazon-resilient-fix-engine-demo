package com.amazonaws.fixengineonaws;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.globalaccelerator.AWSGlobalAccelerator;
import com.amazonaws.services.globalaccelerator.AWSGlobalAcceleratorClientBuilder;
import com.amazonaws.services.globalaccelerator.model.DescribeEndpointGroupRequest;
import com.amazonaws.services.globalaccelerator.model.DescribeEndpointGroupResult;
import com.amazonaws.services.globalaccelerator.model.EndpointConfiguration;
import com.amazonaws.services.globalaccelerator.model.EndpointDescription;
import com.amazonaws.services.globalaccelerator.model.EndpointGroup;
import com.amazonaws.services.globalaccelerator.model.UpdateEndpointGroupRequest;

import quickfix.Acceptor;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.InvalidMessage;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SocketAcceptor;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

//TODO: Test debug logging
//TODO: Test with memorydb

/*
 * To test locally, please Install Ubuntu Linux and Redis on Windows: https://redis.io/docs/getting-started/installation/install-redis-on-windows/
 * curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
 * echo \"deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main\" | sudo tee /etc/apt/sources.list.d/redis.list
 * sudo apt-get update
 * sudo apt-get install redis
 * sudo service redis-server start
 * redis-cli
 * sudo service redis-server stop
 * 
 * To connect from AWS EC2 using TLS, See https://docs.aws.amazon.com/memorydb/latest/devguide/getting-startedclusters.connecttonode.html
 * 
 */


public class FixEngine implements Application {
    private Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
    private String MY_IP = "???";
    private boolean IM_AM_THE_ACTIVE_ENGINE = false; // TODO: DO WE STILL NEED THIS?
    private String REDIS_LEADER_LOCK_NAME = RedisSetting.DEFAULT_REDIS_LEADER_LOCK_NAME;
    private String REDIS_FIX_TO_APP_QUEUE_NAME = RedisSetting.DEFAULT_REDIS_FIX_TO_APP_QUEUE_NAME;
    private String REDIS_APP_TO_FIX_QUEUE_NAME = RedisSetting.DEFAULT_REDIS_APP_TO_FIX_QUEUE_NAME;
    private long messageCounter = 0;
    private Acceptor FIX_SERVER = null;
    private Initiator FIX_CLIENT = null;
    private Session FIX_SESSION = null;
    private SessionID FIX_SESSION_ID = null;

    private Jedis jedisForAppToFix = null;
    private Jedis jedisForFixToApp = null;
    private Jedis jedisForLeaderElection = null;

    private final String LEADER_STATUS_STILL_LEADER = "LEADER_STATUS_STILL_LEADER"; // TODO: DO WE STILL NEED THIS?
    private final String LEADER_STATUS_STILL_NOT_LEADER = "LEADER_STATUS_STILL_NOT_LEADER"; // TODO: DO WE STILL NEED THIS?
    private final String LEADER_STATUS_JUST_BECAME_LEADER = "LEADER_STATUS_JUST_BECAME_LEADER"; // TODO: DO WE STILL NEED THIS?

    private int HEARTBEAT_SLEEP_INTERVAL = 0; // TODO: move these to Settings?
    private boolean DROP_FIX_TO_APP_MESSAGES = false; // TODO: move these to Settings?
    private boolean DROP_APP_TO_FIX_MESSAGES = false; // TODO: move these to Settings?
    
    private boolean SLOW_DEBUG_MODE = false;

	private Date LAST_NO_MESSAGE_LOG = new Date();
	private long totalAppToFixProcessngTime = 0;
	private long totalAppToFixProcessedMessages = 0;
	private long totalFixToAppProcessngTime = 0;
	private long totalFixToAppProcessedMessages = 0;

    private FixEngineConfig fixEngineConfig;

	/**
	 * Constructor creates a LOGGER and a FixEngineConfig using the specified configfile location, in preparation for calling run()
	 * @param configfile
	 * @throws ConfigError 
	 */
	public FixEngine(String configfile) throws ConfigError {
		super();
        LOGGER.setLevel(Level.INFO);
        // LOGGER.setLevel(Level.FINE);
	    MY_IP = getMyIp();
	    LOGGER.info(MY_IP+"CONSTRUCTOR: INITIALIZING CONFIG");
	    try {
	    	fixEngineConfig = new FixEngineConfig(configfile, LOGGER);
		} catch (ConfigError e) {
		    LOGGER.severe(MY_IP+"CONSTRUCTOR: ERROR INITIALIZING CONFIG DUE TO ERROR: " + e);
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * Calls heartbeatMessageProcessingLoop and catches/logs any errors
	 */
	public void run() {
	    LOGGER.info(MY_IP+"CONSTRUCTOR: STARTING HEARTBEAT");
		try {
		    REDIS_LEADER_LOCK_NAME = fixEngineConfig.getSessionSetting(RedisSetting.SETTING_REDIS_LEADER_LOCK_NAME);
		    REDIS_FIX_TO_APP_QUEUE_NAME = fixEngineConfig.getSessionSetting(RedisSetting.SETTING_REDIS_FIX_TO_APP_QUEUE_NAME);
		    REDIS_APP_TO_FIX_QUEUE_NAME = fixEngineConfig.getSessionSetting(RedisSetting.SETTING_REDIS_APP_TO_FIX_QUEUE_NAME);

			String redisHost = fixEngineConfig.getSessionSetting(RedisSetting.SETTING_REDIS_HOST);
			String redisPort = fixEngineConfig.getSessionSetting(RedisSetting.SETTING_REDIS_PORT);
			jedisForFixToApp = new Jedis(redisHost, Integer.parseInt(redisPort));
			jedisForAppToFix = new Jedis(redisHost, Integer.parseInt(redisPort));
			heartbeatMessageProcessingLoop(fixEngineConfig);
		} catch (ConfigError e) {
			LOGGER.severe(MY_IP+"CONSTRUCTOR: ERROR IN HEARTBEAT DUE TO CONFIG ERROR: " + e);
			e.printStackTrace();
		}
	}

	/**
	 * QuickFixJ Application interface hook - called when FIX session object is first created  
	 */
    @Override
    public void onCreate(SessionID sessionID) {
        LOGGER.fine(MY_IP+"OnCreate");
    }

	/**
	 * QuickFixJ Application interface hook - called when FIX session is established
	 */
    @Override
    public void onLogon(SessionID sessionID) {
        LOGGER.info(MY_IP+"OnLogon session ID: " + sessionID);
        FIX_SESSION_ID = sessionID;
    }

	/**
	 * QuickFixJ Application interface hook - called when FIX session is logged out
	 */
    @Override
    public void onLogout(SessionID sessionID) {
        LOGGER.info(MY_IP+"OnLogout session ID: " + sessionID);
        FIX_SESSION_ID = null;
    }

	/**
	 * QuickFixJ Application interface hook
	 */
    @Override
    public void toAdmin(Message message, SessionID sessionID) {
    }

	/**
	 * QuickFixJ Application interface hook
	 */
    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

	/**
	 * QuickFixJ Application interface hook - called when a message is being sent by this process out via the FIX connection
	 */
    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        LOGGER.info(MY_IP+"%%%%%%%% TOAPP: " + message);
    }

	/**
	 * QuickFixJ Application interface hook - called when a message is received by this process from the FIX connection
	 * decodes the message and forwards it to MemoryDB queue 
	 */
    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        LOGGER.info(MY_IP+"%%%%%%%% FROMAPP: " + message);
        Date startTime = new Date();
        if(DROP_FIX_TO_APP_MESSAGES) {
            LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: DROPPING MESSAGE INSTEAD OF SENDING IT!");        	
        } else {
	        if (!IM_AM_THE_ACTIVE_ENGINE) {
	            LOGGER.fine(MY_IP+"%%%%%%%% FROMAPP: NOT ACTIVE ENGINE, DO Nothing" );
	        }
	
	        LOGGER.fine(MY_IP+"********************** counter: " + messageCounter++);
	
	        String parsedOrdStr = message.toString();
	        LOGGER.info(MY_IP+"%%%%%%%% FROMAPP: ***SERVER FIX ENGINE*** PUSHING TO QUEUE NAMED " + REDIS_FIX_TO_APP_QUEUE_NAME + " ORDER FIX STRING: " + parsedOrdStr);

	        try {
            	jedisForFixToApp.rpush(REDIS_FIX_TO_APP_QUEUE_NAME, parsedOrdStr);
	        } catch (Exception e) {
	            LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: Exception:" + e);
	            e.printStackTrace();
	        }
        }
		logStats(false, 1, startTime, new Date());
    }

    /**
     * Returns the IP address of the host running this code as a String
     * @return
     */
    private String getMyIp() {
        try {
            InetAddress inet = InetAddress.getLocalHost();
            return inet.getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.severe(MY_IP+"ERROR: Unable to find my own IP address!" + e);
            e.printStackTrace();
            return "???.???.???.???";
        }
    }

    /**
     * Sets the LOGGER's log level to INFO (for regular operation) if the parameter is false or FINE (for debugging) if the parameter is true 
     * @param enableFineGrainedLogging
     */
    public void setLogLevel(boolean enableFineGrainedLogging) {
        Level logLevel = enableFineGrainedLogging ? Level.FINE : Level.INFO;
        LOGGER.setLevel(logLevel);
        LOGGER.info(MY_IP+"MAIN: SET LOG LEVEL TO " + logLevel);
        // LOGGER.fine(MY_IP+"MAIN: A FINE LOG TEST");
    }

    /**
     * Parses FIX string to a QuickFixJ Message object
     * @param fixString
     * @return
     */
    public Message parseOrder(String fixString) {
        if(fixString == null || FIX_SESSION_ID == null) {
    		LOGGER.severe(MY_IP+"****PARSE ORDER: SOMETHING's WRONG! ONE OF THESE IS NULL! EXITING! fixString=" + fixString + "; FIX_SESSION_ID=" + FIX_SESSION_ID);
        	return null;
        }
    	try {
		    Message parsedOrd = quickfix.MessageUtils.parse(FIX_SESSION, fixString);
		    LOGGER.fine(MY_IP+"****PROCESS MEMORYDB MSGS: PARSED   MESSAGE: " + parsedOrd);
	        return parsedOrd;
	    } catch (InvalidMessage e) {
	    	LOGGER.severe(MY_IP+"ERROR PARSING MESSAGE: " + fixString);
	        e.printStackTrace();
	        return null;
	    }
    }

    /**
     * prints a 
     * @param count
     * @param firstOrderTime
     * @param lastOrderTime
     */
    private void logStats(boolean appToFix, int count, Date firstOrderTime, Date lastOrderTime) {
    	if (count > 0 && lastOrderTime!=null && firstOrderTime!=null) {
    		long totalTime = 1;
    		long totalCount = 1;
    		if(appToFix) {
    			totalAppToFixProcessngTime += lastOrderTime.getTime() - firstOrderTime.getTime();
    			totalTime = totalAppToFixProcessngTime;
    			totalAppToFixProcessedMessages += count;
    			totalCount = totalAppToFixProcessedMessages;
    		} else {
    			totalFixToAppProcessngTime += lastOrderTime.getTime() - firstOrderTime.getTime();
    			totalTime = totalFixToAppProcessngTime;
    			totalFixToAppProcessedMessages += count;
    			totalCount = totalFixToAppProcessedMessages;
    		}
	        long totalTimeInSec = (totalTime)/1000;
	        if (totalTimeInSec < 1) totalTimeInSec = 1;
	        double tps = totalCount/totalTimeInSec;
	        LOGGER.info(" ************ Order Generation Performance & Througput Results ******************* ");
//	        LOGGER.info("\n Start Time: " + DATE_FORMAT.format(firstOrderTime) + 
//	                    "\n End Time: " + DATE_FORMAT.format(lastOrderTime) + "\n Total Messages Processed: " + count 
//	                    + "\n Total Processing Time (seconds) " + totalTimeInSec + "\n TPS: " + tps);	        	
	        LOGGER.info("************* " + (appToFix?"AppToFix":"FixToApp") + ": Total Processing Time (seconds): " + totalTimeInSec + 
	        		"\t Total Messages Processed: " + totalCount + "\t TPS: " + tps);
	        LOGGER.info(" ************ ************ ************ ************ ************");
    	}
    }

    /**
     * Parses FIX-formatted message String from MemoryDB ConsumerRecord parameter into a QuickFixJ Message object, then sends that object out via the FIX connection 
     * @param appToFixMessage
     */
    private void processInboundAppToFixMessages(int maxMessagesToProcess) {
        LOGGER.fine(MY_IP+"****PROCESSING MEMORYDB MESSAGES");
        // TODO: This IM_AM_THE_ACTIVE_ENGINE business smells like a race condition we got rid of long ago. try removing it.
        if(!IM_AM_THE_ACTIVE_ENGINE || FIX_SESSION_ID == null) {
    		LOGGER.severe(MY_IP+"****PROCESS MemoryDB MSGS: SOMETHING's WRONG! EXITING! IM_AM_THE_ACTIVE_ENGINE IS FALSE? " + IM_AM_THE_ACTIVE_ENGINE + " OR FIX_SESSION_ID IS NULL? " + FIX_SESSION_ID);
        	return;
        }
        Date firstMessageTime = null;
        Date lastMessageTime = null;
        int processedMessageCount = 0;
        while(processedMessageCount < maxMessagesToProcess) {
//    		LOGGER.info(MY_IP+"****PROCESS MemoryDB MSGS: CHECKING IF IM_AM_THE_ACTIVE_ENGINE " + IM_AM_THE_ACTIVE_ENGINE + " AND FIX_SESSION_ID NOT NULL " + FIX_SESSION_ID);
    		LOGGER.fine(MY_IP+"****PROCESS MemoryDB MSGS: CHECKING QUEUE " + REDIS_APP_TO_FIX_QUEUE_NAME + " FOR NEW MESSAGES!");
    		LOGGER.fine(MY_IP+"****PROCESS MemoryDB MSGS: QUEUE HAS " + jedisForAppToFix.llen(REDIS_APP_TO_FIX_QUEUE_NAME) + " NEW MESSAGES!");
        	String appToFixMessage = jedisForAppToFix.lpop(REDIS_APP_TO_FIX_QUEUE_NAME);
        	if (appToFixMessage == null) {
        		if( (new Date().getTime() - LAST_NO_MESSAGE_LOG.getTime())/1000 > 10) {
        			LOGGER.info(MY_IP+"****PROCESS MemoryDB MSGS: NO MORE MESSAGES TO PULL FROM QUEUE NAMED " + REDIS_APP_TO_FIX_QUEUE_NAME);
        			LAST_NO_MESSAGE_LOG = new Date();
        		}
        		logStats(true, processedMessageCount, firstMessageTime, lastMessageTime);
	            return;
        	} else {
        		LOGGER.info(MY_IP+"****PROCESS MemoryDB MSGS: PULLED A MESSAGE FROM QUEUE NAMED " + REDIS_APP_TO_FIX_QUEUE_NAME + " MESSAGE: " + appToFixMessage);
        		if(processedMessageCount==0) { firstMessageTime = new Date(); }
        		processedMessageCount++;
		        Message parsedOrd = parseOrder(appToFixMessage);
		        //[CLIENT FIX ENGINE] SEND ORDER FIX TO SERVER FIX ENGINE
		        try {
		            if(DROP_APP_TO_FIX_MESSAGES) {
		                LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: DROPPING MESSAGE INSTEAD OF SENDING IT!");        	
		            } else {
			        	LOGGER.info(MY_IP+"****PROCESS MemoryDB MSGS: SENDING MESSAGE TO FIX SESSION " + FIX_SESSION_ID + " MESSAGE: " + parsedOrd);
			            Session.sendToTarget(parsedOrd, FIX_SESSION_ID);
			            lastMessageTime = new Date();
		            }
		        } catch (SessionNotFound se) {
		        	LOGGER.severe(MY_IP+"****PROCESS MemoryDB MSGS: SessionNotFound: " + se);
		            se.printStackTrace();
		        } catch (Exception e) {
		        	LOGGER.severe(MY_IP+"****PROCESS MemoryDB MSGS: Exception: " + e);
		            e.printStackTrace();
		        }
        	}
        } // while
        logStats(true, processedMessageCount, firstMessageTime, lastMessageTime);
    }
    
    /**
     * Re-points Global Accelerator endpoint (specified by myGaEndpointGroupArn) to the endpoint that's running this code (specified by myGaEndpointArn)
     * @param myGaEndpointGroupArn
     * @param myGaEndpointArn
     */
	private void updateGAEndpoints(String myGaEndpointGroupArn, String myGaEndpointArn) {
    	if(System.getProperty("os.name").contains("Windows")) {
    		LOGGER.info("FIXENGINECONFIG UPDATE GA ENDPOINTS returning because we're running on Windows not Unix");
    		return;
    	}
    	
        LOGGER.fine(MY_IP+"UPDATE GA ENDPOINT starting for myGaEndpointGroupArn: "+ myGaEndpointGroupArn + " and myGaEndpointArn: "+ myGaEndpointArn);
        String activeEndpoint = null;
        String passiveEndpoint = null;
        String tobeActiveEndpoint = null;

        tobeActiveEndpoint = myGaEndpointArn;

        // following code if rumnning on EC2
        // String ec2InstanceID = EC2MetadataUtils.getInstanceId();
        // LOGGER.info(MY_IP+" ec2InstanceID " + ec2InstanceID);
        // tobeActiveEndpoint = ec2InstanceID;

        AWSGlobalAccelerator amazonGlobalAcceleratorClient = AWSGlobalAcceleratorClientBuilder.standard().withRegion(Regions.US_WEST_2).build();

        DescribeEndpointGroupResult describeEndpointGroupResult = amazonGlobalAcceleratorClient.describeEndpointGroup(new DescribeEndpointGroupRequest().withEndpointGroupArn(myGaEndpointGroupArn));
        //System.out.println("describeEndpointGroupResult: " + describeEndpointGroupResult);
        EndpointGroup endpointGroup = describeEndpointGroupResult.getEndpointGroup();
        List<EndpointDescription> endpointDescriptions = endpointGroup.getEndpointDescriptions();
        for (int i = 0; i < endpointDescriptions.size(); i++) {
            EndpointDescription endpointDescription = endpointDescriptions.get(i);
            String endpointId = endpointDescription.getEndpointId();
            String healthState = endpointDescription.getHealthState();
            Integer weight = endpointDescription.getWeight();

            // Make the endpoint active based on instance id or NLB Arn
            if (endpointId.equals(tobeActiveEndpoint) ) {
                activeEndpoint = endpointId;
            } else {
                passiveEndpoint = endpointId;
            }

            LOGGER.fine(MY_IP+"MY ENDPOINT: ID: "+ endpointId + " HEALTH: " + healthState + " WEIGHT: " + weight);
        }
        LOGGER.fine(MY_IP+"UPDATE GA ENDPOINT activeEndpoint: "+ activeEndpoint + " passiveEndpoint: " + passiveEndpoint);
         //Update the GA endpoint configuration to flip from active to passive endpoint
        Collection<EndpointConfiguration> endpointConfiguration = new ArrayList<EndpointConfiguration> ();
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(activeEndpoint).withWeight(100));
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(passiveEndpoint).withWeight(0));
        LOGGER.info(MY_IP+"UPDATE GA ENDPOINT flipping to myGaEndpointArn: "+ myGaEndpointArn + " with endpointConfiguration: " + endpointConfiguration);
        amazonGlobalAcceleratorClient.updateEndpointGroup(new UpdateEndpointGroupRequest().withEndpointGroupArn(myGaEndpointGroupArn).withEndpointConfigurations(endpointConfiguration));
    }

	/**
	 * Calls heartbeatSprocStmt to determine if the current engine is still tht leader, still not the leader, or just became the leader.
	 * @param heartbeatSprocStmt
	 * @param iAmTheLeader
	 * @param useJdbcHeartbeat
	 * @return one of the three LEADER_STATUS_* codes
	 * @throws SQLException
	 */
    private String getLeaderStatus(boolean iAmClientFixEngine, boolean iAmTheLeader, boolean useMemoryDBLeaderLock, int leaderLockDuration) {
//      LOGGER.fine(MY_IP+"*********************HEARTBEAT********************");  
        String leaderStatus = LEADER_STATUS_STILL_NOT_LEADER;
        if(!useMemoryDBLeaderLock) {
            if(iAmTheLeader) {
                leaderStatus =  LEADER_STATUS_STILL_LEADER;
            } else {
                leaderStatus = LEADER_STATUS_JUST_BECAME_LEADER;
            }
            LOGGER.fine(MY_IP+"****HEARTBEAT: NO MEMORYDB CONNECTION. DEFAULT LEADER STATUS: " + leaderStatus);
        } else {
// TODO: Reuse these params
        	SetParams setParams = new SetParams().nx().px(leaderLockDuration + (SLOW_DEBUG_MODE?5000:0) );
        	String myLockValue = MY_IP + ":" + (iAmClientFixEngine?"CLIENT":"SERVER");
            LOGGER.fine(MY_IP+"****HEARTBEAT: MEMORYDB CONNECTION. CHECKING LOCK: " + REDIS_LEADER_LOCK_NAME + " FOR MY VALUE " + myLockValue);
            String lockValue = jedisForLeaderElection.get(REDIS_LEADER_LOCK_NAME);
            LOGGER.fine(MY_IP+"****HEARTBEAT: MEMORYDB CONNECTION. COMPARING LOCK VALUE " + lockValue + " TO MY VALUE " + myLockValue);
            if(myLockValue.equals(lockValue)) {
        		if(iAmTheLeader) {
        			return LEADER_STATUS_STILL_LEADER;
        		} else {
                    LOGGER.severe(MY_IP+"****HEARTBEAT: SOMETHING'S WEIRD: I WAS NOT LEADER EVEN THOUGH I WAS HOLDING THE LOCK!");
        			return LEADER_STATUS_JUST_BECAME_LEADER;
        		}
            } else {
	            LOGGER.fine(MY_IP+"****HEARTBEAT: MEMORYDB CONNECTION. TRYING TO SET LOCK: " + REDIS_LEADER_LOCK_NAME + " WITH VALUE " + myLockValue);
	        	String result = jedisForLeaderElection.set(REDIS_LEADER_LOCK_NAME, myLockValue, setParams);
	            LOGGER.fine(MY_IP+"****HEARTBEAT: MEMORYDB CONNECTION. GET LOCK RESULT: " + result);
	        	// If this call succeeds then the key must have expired
	        	if("OK".equals(result)) {
	        		if(iAmTheLeader) {
	        			return LEADER_STATUS_STILL_LEADER;
	        		} else {
	        			return LEADER_STATUS_JUST_BECAME_LEADER;
	        		}
	        	} else {
	        		return LEADER_STATUS_STILL_NOT_LEADER;
	        	}
	        }
        }
		return leaderStatus;
    }

    /**
     * Creates QuickFix SocketAcceptor class, calls start() on it and assigns it to global variable FIX_SERVER
     * Uses the FIX_SERVER to look up the FIX session and assigns the result to global variable FIX_SESSION
     * @param config
     * @throws ConfigError
     */
    private void startFixServer(FixEngineConfig config) throws ConfigError {
	    LOGGER.info(MY_IP+"****STARTING FIX SERVER APPLICATION");           
	    if(FIX_SERVER!=null) {
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_SERVER object already exists!");
	    } else {
	        MessageStoreFactory messageStoreFactory = null;
// TODO: Centralize these magic config setting names!
	        if("true".equals(config.getSessionSetting("UseMemoryDBMessageStore"))) {
		        messageStoreFactory = new RedisStoreFactory(config.getSessionSettings());
	        } else {
	        	messageStoreFactory = new FileStoreFactory(config.getSessionSettings());
	        }
	        LogFactory logFactory = new ScreenLogFactory(true, true, true);
	        MessageFactory messageFactory = new DefaultMessageFactory();
	    	FIX_SERVER = new SocketAcceptor(this, messageStoreFactory, config.getSessionSettings(), logFactory, messageFactory);
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_SERVER object created: " + FIX_SERVER);	    	
	    }

	    if(FIX_SESSION!=null) {
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_INBOUND_SESSION object already exists!");
	    } else {
	    	FIX_SERVER.start();
	    	FIX_SESSION = Session.lookupSession(FIX_SERVER.getSessions().get(0));
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_INBOUND_SESSION object created: " + FIX_SESSION);
	    }
    }	

    /**
     * Creates QuickFix SocketInitiator class, calls start() on it and assigns it to global variable FIX_CLIENT
     * Uses the FIX_CLIENT to look up the FIX session and assigns the result to global variable FIX_SESSION
     * @param config
     * @throws ConfigError
     */
    private void startFixClient(FixEngineConfig config) throws ConfigError {
	    LOGGER.info(MY_IP+"****STARTING FIX CLIENT APPLICATION");
	    if(FIX_CLIENT!=null) {
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_CLIENT object already exists!");
	    } else {
	        MessageStoreFactory messageStoreFactoryClient = null;
// TODO: Centralize these magic config setting names!
	        if("true".equals(config.getSessionSetting("UseMemoryDBMessageStore"))) { 
	            messageStoreFactoryClient = new RedisStoreFactory(config.getSessionSettings());
	        } else {
	            messageStoreFactoryClient = new FileStoreFactory(config.getSessionSettings());
	        }                               
	        LogFactory logFactoryClient = new ScreenLogFactory(true, true, true);
	        MessageFactory messageFactoryClient = new DefaultMessageFactory();
	
	        FIX_CLIENT = new SocketInitiator(this, messageStoreFactoryClient, config.getSessionSettings(), logFactoryClient, messageFactoryClient);
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_CLIENT object created: " + FIX_CLIENT);	    	
	    }

	    if(FIX_SESSION_ID!=null) {
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION_ID object already exists!");
	    } else {
	        FIX_CLIENT.start();
	        FIX_SESSION = Session.lookupSession(FIX_CLIENT.getSessions().get(0)); 
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION object created: " + FIX_SESSION);
	
	        while(FIX_SESSION_ID == null) {
		        LOGGER.info(MY_IP+"****QUICKFIX CLIENT START: WAITING FOR SERVER..." );
		        try {
		          Thread.sleep(500);  
		        } catch (InterruptedException ie) {
		            LOGGER.severe(MY_IP+"****QUICKFIX CLIENT START: FixEngine THREAD INTERRUPTED: " + ie);
		        }
		    }
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION_ID connection established: " + FIX_SESSION_ID);
	    }
    }

    /**
     * Stops FIX_SERVER and nulls out FIX_SESSION and FIX_SERVER
     */
    private void stopFixServer(){
	    LOGGER.info(MY_IP+"****STOPPING FIX SERVER APPLICATION");           
    	FIX_SERVER.stop();
    	FIX_SESSION=null;
	    FIX_SERVER=null;
    }

    /**
     * Stops FIX_CLIENT and nulls out FIX_SESSION and FIX_CLIENT
     */
    private void stopFixClient() {
	    LOGGER.info(MY_IP+"****STOPPING FIX CLIENT APPLICATION");
	    if(FIX_CLIENT != null) {
	    	FIX_CLIENT.stop();
	    }
	    FIX_SESSION_ID=null;
        FIX_CLIENT=null;
        FIX_SESSION=null;
    }

    /**
     * Decides whether it's a client or server based on ConnectionType setting in FixEngineConfig.
     * Calls getHeartbeatSprocStmt to create a DB connection and prepared statement. 
     * Starts a FIX server (to let Global Accelerator health checks on the FIX port pass)
     * Loops forever: 
     * 	gets leader status from MemoryDB lock object
     *  Stops FIX client if it stops being the leader
     *  If it becomes the leader, connects to MemoryDB, and the FIX client or server if they aren't already started
     *  polls for any new MemoryDB messages
     *  any new FIX messages will automatically be sent to MemoryDB by the fromApp method
     * @param config
     * @throws ConfigError
     */
    private void heartbeatMessageProcessingLoop(FixEngineConfig config) throws ConfigError {
        LOGGER.info(MY_IP+"*****STARTING MESSAGE PROCESSING AND HEARTBEAT LOOP *****");
// TODO: Use a class or something to make these magic strings into variables.
    	boolean iAmClientFixEngine = "initiator".equals(config.getSessionSetting("ConnectionType"));
    	String myGAEndpointGroupArn = iAmClientFixEngine ? null : config.getSessionSetting("GAEndpointGroupArn");
    	String myGAEndpointArn = iAmClientFixEngine ? null : config.getSessionSetting("GAEndpointArn");
    	boolean useMemoryDBConnection = "true".equals(config.getSessionSetting("UseMemoryDBLeaderLock"));
    	String memoryDbHost = config.getSessionSetting(RedisSetting.SETTING_REDIS_HOST);
        int memoryDbPort = Integer.parseInt(config.getSessionSetting(RedisSetting.SETTING_REDIS_PORT));
    	int leaderLockDuration = Integer.parseInt(config.getSessionSetting("MemoryDBLeaderLockDuration"));
        
        if(useMemoryDBConnection) {
	        jedisForLeaderElection = new Jedis(memoryDbHost, memoryDbPort);
        }
        
        if(!iAmClientFixEngine) {
        	startFixServer(config); // to let health check know we're alive
        }

        while(true) { 
			LOGGER.fine(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE);
			String leaderStatus = LEADER_STATUS_STILL_NOT_LEADER;
			try {
				leaderStatus = getLeaderStatus(iAmClientFixEngine, IM_AM_THE_ACTIVE_ENGINE, useMemoryDBConnection, leaderLockDuration);
			} catch (Exception e) {
    			LOGGER.severe(MY_IP+"****HEARTBEAT: ***ERROR GETTING LEADER STATUS!*** " + e);
    			e.printStackTrace();
    			LOGGER.severe(MY_IP+"****HEARTBEAT: ***RECREATING HEARTBEAT CONNECTION TO ATTEMPT TO RECOVER!***");
    	        if(useMemoryDBConnection) { jedisForLeaderElection = new Jedis(memoryDbHost, memoryDbPort); }
			}
			LOGGER.fine(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE + " ; leaderStatus: " + leaderStatus);
    		
    	    if(leaderStatus == LEADER_STATUS_JUST_BECAME_LEADER) {
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***I'M STILL LEADER OR JUST BECAME LEADER! ENSURING ENGINES ARE RUNNING!***");
    	        if(iAmClientFixEngine) {
    	            startFixClient(config);
    	        } else {
    	            LOGGER.fine(MY_IP+"**************** HEARTBEAT: I AM Server ENGINE***********");
    	            startFixServer(config);
   	            	updateGAEndpoints(myGAEndpointGroupArn, myGAEndpointArn);
    	        }
    	        IM_AM_THE_ACTIVE_ENGINE = true;
    	    } else if(leaderStatus == LEADER_STATUS_STILL_LEADER) { // Disconnect if connected
    			LOGGER.fine(MY_IP+"****HEARTBEAT: ***STILL LEADER! Keep listening!***");
    	    } else if(leaderStatus == LEADER_STATUS_STILL_NOT_LEADER) { // Disconnect if connected
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***STILL NOT LEADER!***");
    			stopFixClient();
//    	    } else if(leaderStatus == LEADER_STATUS_JUST_BECAME_LEADER) { // Connect!
//    			LOGGER.info(MY_IP+"****HEARTBEAT: ***I JUST BECAME THE LEADER!***");
//    	    	startEngine();
    	    }

// TODO: make this a setting!
    	    int maxMessagesToProcess = 1000;
    	    if(FIX_SESSION_ID!=null) {
    	    	processInboundAppToFixMessages(maxMessagesToProcess);
    	    }

    	    if(HEARTBEAT_SLEEP_INTERVAL > 0 || SLOW_DEBUG_MODE) {
	            try {
	                Thread.sleep(HEARTBEAT_SLEEP_INTERVAL);
	                if(SLOW_DEBUG_MODE) { Thread.sleep(1000); }
	            } catch (InterruptedException ie) {
	                LOGGER.severe(MY_IP+"HEARTBEAT THREAD INTERRUPTED: " +ie);
	            }
    	    }
        }
    }

    /**
     * Instantiates FixEngine class using configfile location from args[0], then calls run() on it
     * @param args
     * @throws ConfigError 
     */
    public static void main(String[] args) throws ConfigError {    	
//        String configfile = "config/server_test.cfg";
//        String configfile = "config/client.cfg";
        String configfile = "config/server-local.config";
        if(args.length > 0) {
            configfile = args[0];
        }
        System.out.println("***MAIN STARTING WITH CONFIG FILE: " + configfile);
    	FixEngine engine = new FixEngine(configfile);
    	engine.run();

        try {
            CountDownLatch latch = new CountDownLatch(1);
			latch.await();
		} catch (InterruptedException e) {
	        System.out.println("MAIN: FINAL WAIT INTERRUPTED: " + e);
			e.printStackTrace();
		}
        System.out.println("MAIN: GOT TO THE END! EXITING!");
    }   

}
