package com.amazonaws.fixengineonaws;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
import quickfix.JdbcStoreFactory;
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

public class FixEngine implements Application {
    private Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
    private String MY_IP = "???";
    private boolean IM_AM_THE_ACTIVE_ENGINE = false;
    private KafkaProducer<String, String> KAFKA_PRODUCER = null;
    private KafkaConsumer<String, Object> KAFKA_CONSUMER = null;
    private String KAFKA_INBOUND_TOPIC_NAME;
    private long messageCounter = 0;
    private Acceptor FIX_SERVER = null;
    private Initiator FIX_CLIENT = null;
    private Session FIX_SESSION = null;
    private SessionID FIX_SESSION_ID = null;

    private final int LEADER_STATUS_STILL_LEADER = 1;
    private final int LEADER_STATUS_STILL_NOT_LEADER = 0;
    private final int LEADER_STATUS_JUST_BECAME_LEADER = -1;

    private int HEARTBEAT_SLEEP_INTERVAL = 0;
    private boolean DROP_FIX_MESSAGES = false;
    private boolean DROP_KAFKA_MESSAGES = false;
    
    private FixEngineConfig fixEngineConfig;

    private long lastStatsLogTime = 0;
    private long logStatsEvery = 60000;
    private long totalInboundMessageProcessingTime = 0;
    private long totalInboundKafkaProcessingTime = 0;
    private long totalOutboundMessageProcessingTime = 0;
    private long totalOutboundFixProcessingTime = 0;

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
	 * decodes the message and forwards it to Kafka queue 
	 */
    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        LOGGER.info(MY_IP+"%%%%%%%% FROMAPP: " + message);
        long timeInFromAppStart = System.currentTimeMillis();
        
        if(DROP_FIX_MESSAGES) {
            LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: DROPPING MESSAGE INSTEAD OF SENDING IT!");        	
        } else {
	        if (!IM_AM_THE_ACTIVE_ENGINE) {
	            LOGGER.fine(MY_IP+"%%%%%%%% FROMAPP: NOT ACTIVE ENGINE, DO Nothing" );
	        }
	
	        LOGGER.fine(MY_IP+"********************** counter: " + messageCounter++);
	
	        String parsedOrdStr = message.toString();
	        LOGGER.fine(MY_IP+"%%%%%%%% FROMAPP: ***SERVER FIX ENGINE*** PARSED ORDER FIX STRING: " + parsedOrdStr);
	
	    //  Object[] array = getKafkaProducer();
	    //     KafkaProducer<String, String> producer = (KafkaProducer) array[0];
	    //     String topicName = (String )array[1];
	
	        try {
	            long timeInFromAppKafkaSendStart = System.currentTimeMillis();
            	if(timeInFromAppKafkaSendStart - lastStatsLogTime > logStatsEvery) {
            		LOGGER.info(MY_IP+"@@@@@@@@@@ INBOUND TIMING STATISTICS: RESETTING TOTALS SINCE IT'S BEEN OVER A MINUTE SINCE THE LAST MESSAGE");
            		totalInboundKafkaProcessingTime = 0;
            		totalInboundMessageProcessingTime = 0;
            	}
            	KAFKA_PRODUCER.send(new ProducerRecord<String, String>(KAFKA_INBOUND_TOPIC_NAME, parsedOrdStr)).get();
	            long timeInFromAppKafkaSendEnd = System.currentTimeMillis();

                totalInboundKafkaProcessingTime += timeInFromAppKafkaSendEnd - timeInFromAppKafkaSendStart;
                totalInboundMessageProcessingTime += timeInFromAppKafkaSendEnd - timeInFromAppStart;
                LOGGER.info(MY_IP+"@@@@@@@@@@ INBOUND TIMING STATISTICS:\ttotalInboundKafkaProcessingTime:\t" + totalInboundKafkaProcessingTime + "\ttotalInboundMessageProcessingTime:\t" + totalInboundMessageProcessingTime);
	        } catch (Exception e) {
	            LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: Exception:" + e);
	            e.printStackTrace();
	        }       
        }
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
     * Tries to load the supplied class by name, prints an exception if it's unable to. 
     * @param jdbcDriver
     */
    private void loadJdbcClass(String jdbcDriver) {
        try {
            Class.forName(jdbcDriver); 
        } catch (ClassNotFoundException e) {
            LOGGER.severe(MY_IP+"UNABLE TO LOAD JDBC DRIVER:" + jdbcDriver);
            e.printStackTrace();
            return;
        }
        LOGGER.fine(MY_IP+"LOADED JDBC DRIVER:" + jdbcDriver);
    }

    /**
     * Creates and returns a JDBC connection
     * @param jdbcUrl
     * @param jdbcUser
     * @param jdbcPass
     * @return
     */
    private Connection getSqlDbConnection(String jdbcUrl, String jdbcUser, String jdbcPass) {        
        LOGGER.info(MY_IP+"*********************GET SQL DB CONNECTION starting, using JDBC URL " + jdbcUrl+ " WITH USER " + jdbcUser + " AND PASSWORD WHICH IS A SECRET ");
        
        // System.out.println("CONNECTING TO URL " + jdbcUrl + " WITH USER " + jdbcUser + " AND PASS " + jdbcPass);
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
            LOGGER.fine(MY_IP+"****GET SQL DB CONNECTION: GOT SQL CONNECTION");

            if (conn != null) {
                LOGGER.info(MY_IP+"****GET SQL DB CONNECTION: Database connection established");
                return conn;
            }
        } catch (Exception e) {
            LOGGER.severe(MY_IP+"****GET SQL DB CONNECTION: EXCEPTION: " + e);
            e.printStackTrace();
        }
        return null;    
    }

	/**
	 * Creates a KafkaProducer connection to the specified broker
	 * @param kafkaBrokerString
	 * @return
	 */
    private KafkaProducer<String, String> startKafkaProducer(String kafkaBrokerString) {
		LOGGER.info(MY_IP+"****START KAFKA OUTBOUND PRODUCER START*****");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokerString);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }

	/**
	 * Creates a KafkaProducer connection to the specified broker, group and topic
	 * @param kafkaBrokerString
	 * @return
	 */
    private KafkaConsumer<String, Object> startKafkaConsumer(String kafkaBrokerString, String consumerGroupId, String topicName) {
        LOGGER.info(MY_IP+"****KAFKA INBOUND CONSUMER START*****");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokerString);
        properties.setProperty("group.id", consumerGroupId);
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // When a group is first created, it has no offset stored to start reading from. This tells it to start
        // with the earliest record in the stream.
        properties.setProperty("auto.offset.reset","earliest");
        KafkaConsumer<String, Object> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        return kafkaConsumer;
    }

    /**
     * Parses FIX string to a QuickFixJ Message object
     * @param fixString
     * @return
     */
    public Message parseOrder(String fixString) {
	    try {
		    Message parsedOrd = quickfix.MessageUtils.parse(FIX_SESSION, fixString);
		    LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: PARSED   MESSAGE: " + parsedOrd);
//		    LOGGER.fine(MY_IP+"****PROCESS KAFKA MSGS: PARSED    HEADER: " + parsedOrd.getHeader());
	        return parsedOrd;
	    } catch (InvalidMessage e) {
	    	LOGGER.severe(MY_IP+"ERROR PARSING MESSAGE: " + fixString);
	        e.printStackTrace();
	        return null;
	    }
    }
    
    /**
     * Parses FIX-formatted message String from Kafka ConsumerRecord parameter into a QuickFixJ Message object, then sends that object out via the FIX connection 
     * @param kafkaMessage
     */
    private void processOneInboundKafkaMessage(ConsumerRecord<String, Object> kafkaMessage) {
        LOGGER.info(MY_IP+"****PROCESS ONE KAFKA MESSAGE: processing " + kafkaMessage.value().toString());
        if(DROP_KAFKA_MESSAGES) {
            LOGGER.severe(MY_IP+"****PROCESS ONE KAFKA MESSAGE: DROPPING MESSAGE INSTEAD OF SENDING IT!");        	
        } else {
	        Message parsedOrd = parseOrder(kafkaMessage.value().toString());
	        //[CLIENT FIX ENGINE] SEND ORDER FIX TO SERVER FIX ENGINE
	        try {
	        	LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: SENDING MESSAGE TO FIX: " + parsedOrd);
	            Session.sendToTarget(parsedOrd, FIX_SESSION_ID);
	        } catch (SessionNotFound se) {
	        	LOGGER.severe(MY_IP+"****PROCESS KAFKA MSGS: SessionNotFound: " + se);
	            se.printStackTrace();
	        } catch (Exception e) {
	        	LOGGER.severe(MY_IP+"****PROCESS KAFKA MSGS: Exception: " + e);
	            e.printStackTrace();
	        }
        }
    }

    /**\
     * Polls Kafka queue for new messages and calls processOneInboundKafkaMessage on each to send it out via the FIX connection 
     * @param kafkaConsumer
     */
    private void processInboundKafkaMsgs(KafkaConsumer<String, Object> kafkaConsumer) {
        LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: ************* after calling getKafkaConsumer ");
        int count = 0;
        if(IM_AM_THE_ACTIVE_ENGINE && FIX_SESSION_ID != null) {
            //Test code
   //       NewOrderSingle newOrder = new NewOrderSingle(new ClOrdID("12345"), new HandlInst('1'), new Symbol("6758.T"), new Side(Side.BUY), new TransactTime(), new OrdType(OrdType.MARKET));
            // try {
            //     FIX_OUTBOUND_SESSION.sendToTarget(newOrder, FIX_OUTBOUND_SESSION_ID);
            //     Thread.sleep(5000);
            // } catch (Exception e) {
            //     e.printStackTrace();
            // }
            //Test COde 
            // Poll for records
        	
            ConsumerRecords<String, Object> records = kafkaConsumer.poll(Duration.ofMillis(50));
            long timeOutKafkaPollEnd = System.currentTimeMillis();
        	if(timeOutKafkaPollEnd - lastStatsLogTime > logStatsEvery) {
        		LOGGER.info(MY_IP+"@@@@@@@@@@ OUTBOUND TIMING STATISTICS: RESETTING TOTALS SINCE IT'S BEEN OVER A MINUTE SINCE THE LAST MESSAGE");
        		totalOutboundFixProcessingTime = 0;
        		totalOutboundMessageProcessingTime= 0;
        	}

            //LOGGER.fine(MY_IP+" After polling consumer records.count() : " + records.count());
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
	                LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: nothing to read from Kafka");
            } else {
	                LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: got some messages from Kafka");
                // Yes, loop over records
                // for(ConsumerRecord<String, String> record: records) {
                for(ConsumerRecord<String, Object> record: records) {
                    // Display record and count
                    count += 1;
                    LOGGER.fine(MY_IP+ count + ": " + record.value());
                    long timeOutKafkaMessageProcessStart = System.currentTimeMillis();
                    processOneInboundKafkaMessage(record);
                    long timeOutKafkaMessageProcessEnd = System.currentTimeMillis();

                    totalOutboundFixProcessingTime += timeOutKafkaMessageProcessEnd - timeOutKafkaMessageProcessStart;
                    totalOutboundMessageProcessingTime += timeOutKafkaMessageProcessEnd - timeOutKafkaPollEnd;
                    LOGGER.info(MY_IP+"@@@@@@@@@@ OUTBOUND TIMING STATISTICS:\tmessageCount:\t" + count + "\ttotalOutboundFixProcessingTime:\t" + totalOutboundFixProcessingTime + "\ttotalOutboundMessageProcessingTime:\t" + totalOutboundMessageProcessingTime);                
                }
            }
        }
    }

    /**
     * Re-points Global Accelerator endpoint (specified by myGaEndpointGroupArn) to the endpoint that's running this code (specified by myGaEndpointArn)
     * @param myGaEndpointGroupArn
     * @param myGaEndpointArn
     */
	private void updateGAEndpoints(String myGaEndpointGroupArn, String myGaEndpointArn) {
        LOGGER.info(MY_IP+"UPDATE GA ENDPOINT starting for myGaEndpointGroupArn: "+ myGaEndpointGroupArn + " and myGaEndpointArn: "+ myGaEndpointArn);
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

            LOGGER.info(MY_IP+"MY ENDPOINT: ID: "+ endpointId + " HEALTH: " + healthState + " WEIGHT: " + weight);
        }
        LOGGER.info(MY_IP+"UPDATE GA ENDPOINT activeEndpoint: "+ activeEndpoint + " passiveEndpoint: " + passiveEndpoint);
         //Update the GA endpoint configuration to flip from active to passive endpoint
        Collection<EndpointConfiguration> endpointConfiguration = new ArrayList<EndpointConfiguration> ();
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(activeEndpoint).withWeight(100));
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(passiveEndpoint).withWeight(0));
        LOGGER.info(MY_IP+"UPDATE GA ENDPOINT flipping to myGaEndpointArn: "+ myGaEndpointArn + " with endpointConfiguration: " + endpointConfiguration);
        amazonGlobalAcceleratorClient.updateEndpointGroup(new UpdateEndpointGroupRequest().withEndpointGroupArn(myGaEndpointGroupArn).withEndpointConfigurations(endpointConfiguration));
    }

	/**
	 * Constructs a SQL Connection and uses it to construct a prepared statemetn to call EngineStatus stored procedure used by getLeaderStatus
	 * @param jdbcDriver
	 * @param jdbcUrl
	 * @param jdbcUser
	 * @param jdbcPass
	 * @return
	 */
	private CallableStatement getHeartbeatSprocStmt(String jdbcDriver, String jdbcUrl, String jdbcUser, String jdbcPass) {
        LOGGER.fine(MY_IP+"*********************GET HEARTBEAT PROC STATEMENT*********************");              
        String query = "{CALL EngineStatus(?, ?, ?, ?, ?, ?)}";
        loadJdbcClass(jdbcDriver);
        LOGGER.fine(MY_IP+"*****GETHEARTBEATPROCSTATEMENT: Making SQL connection");
        Connection sqlDbConnection = getSqlDbConnection(jdbcUrl, jdbcUser, jdbcPass);
        LOGGER.fine(MY_IP+"*****GETHEARTBEATPROCSTATEMENT: connected to SQL DB");
        try {
            CallableStatement stmt = sqlDbConnection.prepareCall(query);
			stmt.setString(1, MY_IP);
            stmt.registerOutParameter(2, java.sql.Types.INTEGER);
            stmt.registerOutParameter(3, java.sql.Types.VARCHAR);
            stmt.registerOutParameter(4, java.sql.Types.TIMESTAMP);
            stmt.registerOutParameter(5, java.sql.Types.TIMESTAMP);
            stmt.registerOutParameter(6, java.sql.Types.INTEGER);
            LOGGER.fine(MY_IP+"****GETHEARTBEATPROCSTATEMENT: SPROC PREPARED STATEMENT CREATED");             
            return stmt;
        } catch (SQLException e) {
            LOGGER.severe(MY_IP+"****GET HEARTBEAT PROC STATEMENT: EXCEPTION: " + e);
            e.printStackTrace();
        }
        return null;
    }

	/**
	 * Calls heartbeatSprocStmt to determine if the curren engine is still teh leader, still not the leader, or just became the leader.
	 * @param heartbeatSprocStmt
	 * @param iAmTheLeader
	 * @param useJdbcHeartbeat
	 * @return one of the three LEADER_STATUS_* codes
	 * @throws SQLException
	 */
    private int getLeaderStatus(CallableStatement heartbeatSprocStmt, boolean iAmTheLeader, boolean useJdbcHeartbeat) throws SQLException {
//      LOGGER.fine(MY_IP+"*********************HEARTBEAT********************");  
        int leaderStatus = LEADER_STATUS_STILL_NOT_LEADER;
        String lastIpAdd = "";
        Timestamp lastTimestamp = null;
        Timestamp timeNow = null;
        int timeDiffSec = 0;
//      LOGGER.fine(MY_IP+"****HEARTBEAT: USE_JDBC: " + USE_JDBC + "; heartbeatSprocStmt = " + heartbeatSprocStmt);
        if(!useJdbcHeartbeat) {
            if(iAmTheLeader) {
                leaderStatus =  LEADER_STATUS_STILL_LEADER;
            } else {
                leaderStatus = LEADER_STATUS_JUST_BECAME_LEADER;
            }
            LOGGER.info(MY_IP+"****HEARTBEAT: NO SQL CONNECTION. DEFAULT LEADER STATUS: " + leaderStatus);
        } else {    
            try {
                heartbeatSprocStmt.executeQuery();
                leaderStatus = heartbeatSprocStmt.getInt(2);
                lastIpAdd = heartbeatSprocStmt.getString(3);
                lastTimestamp = heartbeatSprocStmt.getTimestamp(4);
                timeNow = heartbeatSprocStmt.getTimestamp(5);
                timeDiffSec = heartbeatSprocStmt.getInt(6);
				LOGGER.info(MY_IP+"****HEARTBEAT: SQL SPROC SAYS: leaderStatus: " + leaderStatus + "; lastIpAdd: " + lastIpAdd + "; lastTimestamp: " + lastTimestamp + "; timeNow: " + timeNow + "; timeDiffSec: " + timeDiffSec);
            } catch (SQLException e) {
                LOGGER.severe(MY_IP+"HEARTBEAT: Exception executing SQL SPROC: " + e);
                e.printStackTrace();
                throw e;
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
	        if("true".equals(config.getSessionSetting("UseJdbcMessageStore"))) {
	        	messageStoreFactory = new JdbcStoreFactory(config.getSessionSettings());
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
	        if("true".equals(config.getSessionSetting("UseJdbcMessageStore"))) { 
	            messageStoreFactoryClient = new JdbcStoreFactory(config.getSessionSettings());
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
		            LOGGER.info(MY_IP+"****QUICKFIX CLIENT START: FixEngine THREAD INTERRUPTED: " + ie);
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
     * 	gets leader status from sproc
     *  Stops FIX client if it stops being the leader
     *  If it becomes the leader, starts Kafka consumer and producer, and the FIX client or server if they aren't already started
     *  polls for any new Kafka messages
     *  any new FIX messages will automatically be sent to Kafka by the fromApp method
     * @param config
     * @throws ConfigError
     */
    private void heartbeatMessageProcessingLoop(FixEngineConfig config) throws ConfigError {
        LOGGER.info(MY_IP+"*****START HEARTBEAT *****");
    	boolean iAmClientFixEngine = "initiator".equals(config.getSessionSetting("ConnectionType"));
    	String kafkaBrokerString = config.getSessionSetting("kafkaBootstrapBrokerString");
    	String consumerGroupId = config.getSessionSetting("KafkaConsumerGroupID");
    	String kafkaOutboundTopicName = config.getSessionSetting("KafkaOutboundTopicName");
    	KAFKA_INBOUND_TOPIC_NAME = config.getSessionSetting("KafkaInboundTopicName");
    	String myGAEndpointGroupArn = iAmClientFixEngine ? null : config.getSessionSetting("GAEndpointGroupArn");
    	String myGAEndpointArn = iAmClientFixEngine ? null : config.getSessionSetting("GAEndpointArn");
    	boolean useJdbcConnection = "true".equals(config.getSessionSetting("UseJdbcHeartbeat"));

    	CallableStatement heartbeatSprocStmt = null;
        if(useJdbcConnection) {
        	config.addSqlDbConnectionCoordinatesToSettings(config.getSessionSetting("RDSClusterSecretArn"));
            heartbeatSprocStmt = getHeartbeatSprocStmt(config.getSessionSetting("JdbcDriver"), config.getSessionSetting("JdbcURL"), config.getSessionSetting("JdbcUser"), config.getSessionSetting("JdbcPassword"));
        }

        if(!iAmClientFixEngine) {
        	startFixServer(config); // to let health check know we're alive
        }

        while(true) { 
			LOGGER.info(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE);
			int leaderStatus = LEADER_STATUS_STILL_NOT_LEADER;
			try {
				leaderStatus = getLeaderStatus(heartbeatSprocStmt, IM_AM_THE_ACTIVE_ENGINE, useJdbcConnection);
			} catch (SQLException e) {
				
    			LOGGER.severe(MY_IP+"****HEARTBEAT: ***ERROR GETTING LEADER STATUS!*** " + e);
    			e.printStackTrace();
    			LOGGER.severe(MY_IP+"****HEARTBEAT: ***RECREATING HEARTBEAT CONNECTION TO ATTEMPT TO RECOVER!***");
                heartbeatSprocStmt = getHeartbeatSprocStmt(config.getSessionSetting("JdbcDriver"), config.getSessionSetting("JdbcURL"), config.getSessionSetting("JdbcUser"), config.getSessionSetting("JdbcPassword"));
			}
			LOGGER.info(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE + " ; leaderStatus: " + leaderStatus);
    		
    	    if(leaderStatus == LEADER_STATUS_JUST_BECAME_LEADER) {
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***I'M STILL LEADER OR JUST BECAME LEADER! ENSURING ENGINES ARE RUNNING!***");
    	        if(KAFKA_CONSUMER == null) { KAFKA_CONSUMER = startKafkaConsumer(kafkaBrokerString, consumerGroupId, kafkaOutboundTopicName); }
    	        if(KAFKA_PRODUCER == null) { KAFKA_PRODUCER = startKafkaProducer(kafkaBrokerString); }
    	        if(iAmClientFixEngine) {
    	            startFixClient(config);
    	        } else {
    	            LOGGER.info(MY_IP+"**************** HEARTBEAT: I AM Server ENGINE***********");
    	            startFixServer(config);
    	            if("10.130.0.66".equals(MY_IP)) {
    	            	LOGGER.severe(MY_IP+"**************** HEARTBEAT: NOT UPDATING GLOBAL ACCELERATOR ENDPOINT BECAUSE WE DONT HAVE ACCESS FROM THIS MACHINE!!!***********");
    	            } else {
    	            	updateGAEndpoints(myGAEndpointGroupArn, myGAEndpointArn);
    	            }
    	        }
    	        IM_AM_THE_ACTIVE_ENGINE = true;
    	    } else if(leaderStatus == LEADER_STATUS_STILL_LEADER) { // Disconnect if connected
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***STILL LEADER! Keep listening!***");
    	    } else if(leaderStatus == LEADER_STATUS_STILL_NOT_LEADER) { // Disconnect if connected
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***STILL NOT LEADER!***");
    			KAFKA_CONSUMER = null;
    			KAFKA_PRODUCER = null;
    			stopFixClient();
//    	    } else if(leaderStatus == LEADER_STATUS_JUST_BECAME_LEADER) { // Connect!
//    			LOGGER.info(MY_IP+"****HEARTBEAT: ***I JUST BECAME THE LEADER!***");
//    	    	startEngine();
    	    }

    	    processInboundKafkaMsgs(KAFKA_CONSUMER);

    	    if(HEARTBEAT_SLEEP_INTERVAL > 0) {
	            try {
	                Thread.sleep(HEARTBEAT_SLEEP_INTERVAL);
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
        String configfile = "config/server_test.cfg";
//        String configfile = "config/client.cfg";
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
