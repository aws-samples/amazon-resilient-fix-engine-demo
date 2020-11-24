package com.amazonaws.fixengineonaws;

import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import quickfix.*;
import quickfix.field.AvgPx;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.fix42.ExecutionReport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Arrays;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.globalaccelerator.AWSGlobalAccelerator;
import com.amazonaws.services.globalaccelerator.AWSGlobalAcceleratorClientBuilder;
import com.amazonaws.services.globalaccelerator.model.DescribeEndpointGroupRequest;
import com.amazonaws.services.globalaccelerator.model.DescribeEndpointGroupResult;
import com.amazonaws.services.globalaccelerator.model.EndpointConfiguration;
import com.amazonaws.services.globalaccelerator.model.EndpointDescription;
import com.amazonaws.services.globalaccelerator.model.EndpointGroup;
import com.amazonaws.services.globalaccelerator.model.UpdateEndpointGroupRequest;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.util.EC2MetadataUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Collection;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import java.util.logging.Logger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;

/**
 * Fix engine launcher, leader election and queue polling class
 *
 * <p>Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.</p>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

public class FixEngine implements Application {
//	private static Logger LOGGER = LoggerFactory.getLogger(FixEngine.class);
//	private final static Logger x = Logger.getLogger(FixEngine.class.getName());
//	x.setLevel(Level.INFO);
//	LogManager.getLogManager().getLogger(Logger.GLOBAL_LOGGER_NAME).setLevel(Level.FINE);
//	private static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	private static Logger LOGGER = Logger.getLogger(FixEngine.class.getName());

    //private static KafkaProducer<String, String> producer;
    //private static String topicName;
    long messageCounter = 0;
    private static String JDBC_DRIVER;
    private static String JDBC_URL;
    private static String JDBC_USER;
    private static String JDBC_PASS;
	private static boolean USE_JDBC_HEARTBEAT = false;
	private static boolean USE_JDBC_MESSAGE_STORE = false;
	
	private static boolean IM_AM_THE_ACTIVE_ENGINE = false;
	private static boolean IM_AM_THE_CLIENT_ENGINE = false;

	private static KafkaProducer<String, String> KAFKA_PRODUCER; 
	private static String KAFKA_OUTBOUND_TOPIC_NAME;
	private static String KAFKA_BROKER_STRING;
	private static KafkaConsumer<String, Object> KAFKA_CONSUMER;
	private static String KAFKA_INBOUND_TOPIC_NAME;
	private static String KAFKA_INBOUND_CONSUMER_GORUP_ID;
	private static String KAFKA_USE_TLS;
	private static String KAFKA_TLS_PORT;
	private static String KAFKA_NON_TLS_PORT;
	
	// private static String KAFKA_INBOUND_BROKER_STRING;
	private static String GA_ENDPOINT_GROUP_ARN;
	private static String GA_MY_ENDPOINT_ARN;
	
	private static SessionSettings FIX_SESSION_SETTINGS; 
	private static boolean FIX_SERVER_ACCEPTOR_THREAD_STARTED = false;
	private static boolean FIX_CLIENT_INITIATOR_THREAD_STARTED = false;
	private static Session FIX_OUTBOUND_SESSION;
    private static volatile SessionID FIX_OUTBOUND_SESSION_ID;
	private static boolean FIX_INIT_STARTED = false;
		
	private static AWSSimpleSystemsManagement ssmClient = System.getProperty("os.name").contains("Windows") ? null : AWSSimpleSystemsManagementClientBuilder.standard().build();
	
    @Override
    public void onCreate(SessionID sessionID) {
        LOGGER.fine("OnCreate");
    }

    @Override
    public void onLogon(SessionID sessionID) {
        LOGGER.info("OnLogon session ID: " + sessionID);
        FIX_OUTBOUND_SESSION_ID = sessionID;
    }

    @Override
    public void onLogout(SessionID sessionID) {
        LOGGER.info("OnLogout session ID: " + sessionID);
        FIX_OUTBOUND_SESSION_ID = null;
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        LOGGER.info("%%%%%%%% TOAPP: " + message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        LOGGER.info("%%%%%%%% FROMAPP: " + message);
        
        if (!IM_AM_THE_ACTIVE_ENGINE) {
            LOGGER.fine("%%%%%%%% FROMAPP: NOT ACTIVE ENGINE, DO Nothing" );
        }
        
        LOGGER.fine("********************** counter: " + messageCounter++);
        
        
        String parsedOrdStr = message.toString();
    	LOGGER.fine("%%%%%%%% FROMAPP: ***SERVER FIX ENGINE*** PARSED ORDER FIX STRING: " + parsedOrdStr);
    	
    // 	Object[] array = getKafkaProducer();
    //     KafkaProducer<String, String> producer = (KafkaProducer) array[0];
         
    //     String topicName = (String )array[1];

        try {
            KAFKA_PRODUCER.send(new ProducerRecord<String, String>(KAFKA_INBOUND_TOPIC_NAME, parsedOrdStr)).get();
        } catch (Exception e) {
            LOGGER.severe("%%%%%%%% FROMAPP: Exception:" + e);
            e.printStackTrace();
        }
        
  //      try {
  //          //Session.sendToTarget(parsedOrd, FIX_OUTBOUND_SESSION_ID);
  //          Session.sendToTarget(generateExecution(System.currentTimeMillis()), FIX_OUTBOUND_SESSION_ID);
  //      } catch (SessionNotFound se) {
		// 	LOGGER.severe("****QUICKFIX SERVER fromApp: SessionNotFound: " + se);
		// 	se.printStackTrace();
		// } catch (Exception e) {
		//     LOGGER.severe("****QUICKFIX SERVER fromApp: Exception: " + e);
		// 	e.printStackTrace();
		// }
        
       
        
    }
    
    private static synchronized void startKafkaProducer() {
		if (KAFKA_PRODUCER == null) {
			LOGGER.fine("****START KAFKA OUTBOUND PRODUCER: Creating new kafka Producer");

	        LOGGER.fine(" ************ KAFKA_INBOUND_TOPIC_NAME: " + KAFKA_INBOUND_TOPIC_NAME);
	        
	        Properties properties = new Properties();
	        properties.setProperty("bootstrap.servers", KAFKA_BROKER_STRING);
	        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	        // specify the protocol for Domain Joined clusters
	        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
	
	        // KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
	        KAFKA_PRODUCER = new KafkaProducer<>(properties);
		} else {
			LOGGER.fine("****START KAFKA OUTBOUND PRODUCER: KAFKA OUTBOUND PRODUCER already exists. doning nothing.");
		}
    }
    
    private static synchronized void startKafkaConsumer() {
        LOGGER.fine("****KAFKA INBOUND CONSUMER START*****");
		if (KAFKA_CONSUMER == null) {
			LOGGER.fine("****START KAFKA INBOUND CONSUMER: Creating new kafka Consumer");
        
	        // Configure the consumer
	        Properties properties = new Properties();
	        // Point it to the brokers
	        properties.setProperty("bootstrap.servers", KAFKA_BROKER_STRING);
	        // Set the consumer group (all consumers must belong to a group).
	        properties.setProperty("group.id", KAFKA_INBOUND_CONSUMER_GORUP_ID);
	        // Set how to serialize key/value pairs
	        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	        // When a group is first created, it has no offset stored to start reading from. This tells it to start
	        // with the earliest record in the stream.
	        properties.setProperty("auto.offset.reset","earliest");
	
	        // specify the protocol for Domain Joined clusters
	        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
	
	        KAFKA_CONSUMER = new KafkaConsumer<>(properties);
	
	        // Subscribe to the 'test' topic
	        KAFKA_CONSUMER.subscribe(Arrays.asList(KAFKA_OUTBOUND_TOPIC_NAME));
	        // Harman 11/02
	        //processInboundKafkaMsgs();
		} else {
			LOGGER.fine("****START KAFKA INBOUND CONSUMER: KAFKA INBOUND CONSUMER already exists. doning nothing.");
		}	        
    }
    
    private static void processKafkaMsgs() {
        LOGGER.fine("****QUICKFIX CLIENT START: ************* after calling getKafkaConsumer ");
        // Loop until ctrl + c
        // Harman: create a thread
        int count = 0;
        while(IM_AM_THE_ACTIVE_ENGINE) {
        	//Test code
   //     	NewOrderSingle newOrder = new NewOrderSingle(new ClOrdID("12345"), new HandlInst('1'), new Symbol("6758.T"), new Side(Side.BUY), new TransactTime(), new OrdType(OrdType.MARKET));
			// try {
			//     FIX_OUTBOUND_SESSION.sendToTarget(newOrder, FIX_OUTBOUND_SESSION_ID);
			//     Thread.sleep(5000);
			// } catch (Exception e) {
			//     e.printStackTrace();
			// }
        	//Test COde 
        	if (FIX_OUTBOUND_SESSION_ID != null) {
            // Poll for records
	            ConsumerRecords<String, Object> records = KAFKA_CONSUMER.poll(200);
	            //LOGGER.fine(" After polling consumer records.count() : " + records.count());
	            // Did we get any?
	            if (records.count() == 0) {
	                // timeout/nothing to read
	                LOGGER.fine("nothing to read from Kafka");
	            } else {
	                // Yes, loop over records
	                // for(ConsumerRecord<String, String> record: records) {
	                for(ConsumerRecord<String, Object> record: records) {
	                    // Display record and count
	                    count += 1;
	                    LOGGER.fine( count + ": " + record.value());
	                    String ordStr = record.value().toString();
	                    Message parsedOrd = null;
	                    try {
	            			parsedOrd = quickfix.MessageUtils.parse(FIX_OUTBOUND_SESSION, ordStr);
	            		} catch (InvalidMessage e) {
	            			LOGGER.severe("ERROR PARSING MESSAGE: " + ordStr);
	            			e.printStackTrace();
	            		}
	                    LOGGER.info("****QUICKFIX CLIENT START: ***CLIENT FIX ENGINE*** PARSED   MESSAGE: " + parsedOrd);
	                    LOGGER.fine("****QUICKFIX CLIENT START: ***CLIENT FIX ENGINE*** PARSED    HEADER: " + parsedOrd.getHeader());
	                                
	                    //[CLIENT FIX ENGINE] SEND ORDER FIX TO SERVER FIX ENGINE
	        	        try {
	        	            Session.sendToTarget(parsedOrd, FIX_OUTBOUND_SESSION_ID);
	        	        } catch (SessionNotFound se) {
	            			LOGGER.severe("****QUICKFIX CLIENT START: SessionNotFound: " + se);
	            			se.printStackTrace();
	            		} catch (Exception e) {
	            		    LOGGER.severe("****QUICKFIX CLIENT START: Exception: " + e);
	            			e.printStackTrace();
	            		}
	        	        
	                    //Thread.sleep(5000);
	                }
            	}
        	} // if FIX_OUTBOUND_SESSION_ID != null
        } //while loop
        
    }

	private static Connection getSqlDbConnection() {
		LOGGER.fine("*********************GET SQL DB CONNECTION********************");	
		// System.out.println("CONNECTING TO URL " + JDBC_URL + " WITH USER " + JDBC_USER + " AND PASS " + JDBC_PASS);
		try {
			Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
			LOGGER.fine("****GET SQL DB CONNECTION: GOT SQL CONNECTION");

			if (conn != null) {
				LOGGER.info("****GET SQL DB CONNECTION: Database connection established");
				return conn;
			}
		} catch (Exception e) {
			LOGGER.severe("****GET SQL DB CONNECTION: EXCEPTION: " + e);
			e.printStackTrace();
		}
		return null;
	}
	
	private static CallableStatement getHeartbeatSprocStmt(Connection sqlDbConnection, String myIp) {
		LOGGER.fine("*********************GET HEARTBEAT PROC STATEMENT*********************");				
		String query = "{CALL EngineStatus(?, ?, ?, ?, ?, ?)}";
		try {
			CallableStatement stmt = sqlDbConnection.prepareCall(query);
			stmt.setString(1, myIp);
			stmt.registerOutParameter(2, java.sql.Types.INTEGER);
			stmt.registerOutParameter(3, java.sql.Types.VARCHAR);
			stmt.registerOutParameter(4, java.sql.Types.TIMESTAMP);
			stmt.registerOutParameter(5, java.sql.Types.TIMESTAMP);
			stmt.registerOutParameter(6, java.sql.Types.INTEGER);
			LOGGER.fine("****GETHEARTBEATPROCSTATEMENT: SPROC PREPARED STATEMENT CREATED");				
			return stmt;
		} catch (SQLException e) {
			LOGGER.severe("****GET HEARTBEAT PROC STATEMENT: EXCEPTION: " + e);
			e.printStackTrace();
		}
		return null;
	}
	
	private static void loadJdbcClass() {
		try {
			if(USE_JDBC_HEARTBEAT || USE_JDBC_MESSAGE_STORE) { Class.forName (JDBC_DRIVER); } 
		} catch (ClassNotFoundException e) {
			LOGGER.severe("UNABLE TO LOAD JDBC DRIVER:" + JDBC_DRIVER);
			e.printStackTrace();
			return;
		}
		LOGGER.fine("LOADED JDBC DRIVER:" + JDBC_DRIVER);
	}	

	private static String getMyIp() {
		String myIp = null;
		try {
			InetAddress inet = InetAddress.getLocalHost();
			myIp = inet.getHostAddress();
			LOGGER.fine("****GET SQL DB CONNECTION: My IP: " + myIp);
			String hostname = inet.getHostName();
			LOGGER.fine("****GET SQL DB CONNECTION: My Hostname: " + hostname);
		} catch (UnknownHostException e) {
			LOGGER.severe("ERROR: Unable to find my own IP address!" + e);
			e.printStackTrace();
		}
		return myIp;
	}
	
	private static synchronized void startFixServerThread() {
		if (FIX_SERVER_ACCEPTOR_THREAD_STARTED) {
			LOGGER.fine("****START FIX SERVER THREAD: FIX server already exists. doning nothing.");
		} else {			
			LOGGER.info("****START FIX SERVER THREAD: Creating new thread and FIX server");
			FIX_SERVER_ACCEPTOR_THREAD_STARTED = true;
			new Thread() {
			    @Override
			    public void run() {
					LOGGER.fine("****START FIX SERVER INSIDE THREAD");	
					loadJdbcClass();
					Application application = new FixEngine();
				    MessageStoreFactory messageStoreFactory = null;
				    if(USE_JDBC_MESSAGE_STORE) { 
				    	messageStoreFactory = new JdbcStoreFactory(FIX_SESSION_SETTINGS);
				    } else {
				    	messageStoreFactory = new FileStoreFactory(FIX_SESSION_SETTINGS);
				    }       
				    LogFactory logFactory = new ScreenLogFactory( true, true, true);
				    MessageFactory messageFactory = new DefaultMessageFactory();
					try {
						Acceptor acceptor = new SocketAcceptor(application, messageStoreFactory, FIX_SESSION_SETTINGS, logFactory, messageFactory);
						acceptor.start();
						FIX_OUTBOUND_SESSION = Session.lookupSession(acceptor.getSessions().get(0)); 
					} catch (ConfigError e) {
						LOGGER.severe("****START FIX SERVER: Unable to start Acceptor due to Config error: " + e);
						e.printStackTrace();
					}
					// Harman 11/02
			        processKafkaMsgs();
			    }
			}.start();
		}
	}
	
	private static synchronized void startFixClientThread() {
		if (FIX_CLIENT_INITIATOR_THREAD_STARTED) {
			LOGGER.fine("****START FIX CLIENT THREAD: FIX client already exists. doning nothing.");
		} else {
			LOGGER.info("****START FIX CLIENT THREAD: Creating new thread and FIX client");
			FIX_CLIENT_INITIATOR_THREAD_STARTED = true;
		    new Thread() {
			    @Override
			    public void run() {
					LOGGER.fine("****STARTING FIX CLIENT APPLICATION INSIDE THREAD");			
			        try {
						loadJdbcClass();
			            Application applicationClient = new FixEngine();
//			            MessageStoreFactory messageStoreFactoryClient = new FileStoreFactory(FIX_SESSION_SETTINGS);
					    MessageStoreFactory messageStoreFactoryClient = null;
					    if(USE_JDBC_MESSAGE_STORE) { 
					    	messageStoreFactoryClient = new JdbcStoreFactory(FIX_SESSION_SETTINGS);
					    } else {
					    	messageStoreFactoryClient = new FileStoreFactory(FIX_SESSION_SETTINGS);
					    }       			            
			            LogFactory logFactoryClient = new ScreenLogFactory( true, true, true);
			            MessageFactory messageFactoryClient = new DefaultMessageFactory();
			    
			            Initiator initiator = new SocketInitiator(applicationClient, messageStoreFactoryClient, FIX_SESSION_SETTINGS, logFactoryClient, messageFactoryClient);
			            initiator.start();
			            FIX_OUTBOUND_SESSION = Session.lookupSession(initiator.getSessions().get(0)); 
			        } catch (Exception e) {
			            LOGGER.severe("****QUICKFIX CLIENT START: Exception: " + e);
			            e.printStackTrace();
			        }
			        
			        
			        while(FIX_OUTBOUND_SESSION_ID == null) {
			        	LOGGER.info("****QUICKFIX CLIENT START: WAITING FOR SERVER..." );
			            try {
					      Thread.sleep(1000);  
			            } catch (InterruptedException ie) {
			        		LOGGER.severe("****QUICKFIX CLIENT START: FixEngine THREAD INTERRUPTED: " + ie);
			        	}
			        }
			        // Harman 11/02
			        processKafkaMsgs();
			    }
			}.start();
		}
	}
	
	private static void UpdateGAEndpoints() {
        String activeEndpoint = null;
        String passiveEndpoint = null;
        String tobeActiveEndpoint = null;

        tobeActiveEndpoint = GA_MY_ENDPOINT_ARN;
        
        // following code if rumnning on EC2
        // String ec2InstanceID = EC2MetadataUtils.getInstanceId();
        // LOGGER.info(" ec2InstanceID " + ec2InstanceID);
        // tobeActiveEndpoint = ec2InstanceID;

        AWSGlobalAccelerator amazonGlobalAcceleratorClient = AWSGlobalAcceleratorClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
        
        DescribeEndpointGroupResult describeEndpointGroupResult = amazonGlobalAcceleratorClient.describeEndpointGroup(new DescribeEndpointGroupRequest().withEndpointGroupArn(GA_ENDPOINT_GROUP_ARN));
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
   
        	LOGGER.info("MY ENDPOINT: ID: "+ endpointId + " HEALTH: " + healthState + " WEIGHT: " + weight);
        }
        LOGGER.info("activeEndpoint: "+ activeEndpoint + " passiveEndpoint: " + passiveEndpoint);
         //Update the GA endpoint configuration to flip from active to passive endpoint
        Collection<EndpointConfiguration> endpointConfiguration = new ArrayList<EndpointConfiguration> ();
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(activeEndpoint).withWeight(100));
        endpointConfiguration.add(new EndpointConfiguration().withEndpointId(passiveEndpoint).withWeight(0));
		amazonGlobalAcceleratorClient.updateEndpointGroup(new UpdateEndpointGroupRequest().withEndpointGroupArn(GA_ENDPOINT_GROUP_ARN).withEndpointConfigurations(endpointConfiguration));
    }

	private static synchronized void heartbeat(CallableStatement heartbeatSprocStmt) {
//		LOGGER.fine("*********************HEARTBEAT********************");	
		int leaderStatus = 0;
		String lastIpAdd = "";
		Timestamp lastTimestamp = null;
		Timestamp timeNow = null;
		int timeDiffSec = 0;
//		LOGGER.fine("****HEARTBEAT: USE_JDBC: " + USE_JDBC + "; heartbeatSprocStmt = " + heartbeatSprocStmt);
		if(!USE_JDBC_HEARTBEAT || heartbeatSprocStmt == null) {
			if(IM_AM_THE_ACTIVE_ENGINE) {
				leaderStatus = 1;
			} else {
				leaderStatus = -1;
			}
			LOGGER.fine("****HEARTBEAT: NO SQL CONNECTION. DEFAULT LEADER STATUS: " + leaderStatus);
		} else {	
			try {
				heartbeatSprocStmt.executeQuery();
				leaderStatus = heartbeatSprocStmt.getInt(2);
				lastIpAdd = heartbeatSprocStmt.getString(3);
				lastTimestamp = heartbeatSprocStmt.getTimestamp(4);
				timeNow = heartbeatSprocStmt.getTimestamp(5);
				timeDiffSec = heartbeatSprocStmt.getInt(6);
//				LOGGER.fine("****HEARTBEAT: SQL SPROC SAYS: leaderStatus: " + leaderStatus + "; lastIpAdd: " + lastIpAdd + "; lastTimestamp: " + lastTimestamp + "; timeNow: " + timeNow + "; timeDiffSec: " + timeDiffSec);
			} catch (SQLException e) {
				LOGGER.severe("HEARTBEAT: Exception executing SQL SPROC: " + e);
				e.printStackTrace();
				return;
			}			
		}
		
		if(leaderStatus == 1) { // Stay connected
			LOGGER.fine("****HEARTBEAT: ***I'M STILL LEADER!*** sproc: " + leaderStatus + "; lastIp: " + lastIpAdd + "; lastTS: " + lastTimestamp + "; Now: " + timeNow + "; Diff: " + timeDiffSec);
			if(!FIX_INIT_STARTED) {
				FIX_INIT_STARTED = true;
				leaderStatus = -1;
			}
		}
		
		if(leaderStatus == 0) { // Disconnect if connected
			LOGGER.fine("****HEARTBEAT: ***STILL NOT LEADER!*** sproc: " + leaderStatus + "; lastIp: " + lastIpAdd + "; lastTS: " + lastTimestamp + "; Now: " + timeNow + "; Diff: " + timeDiffSec);
			IM_AM_THE_ACTIVE_ENGINE = false;
			KAFKA_CONSUMER = null;
			KAFKA_PRODUCER = null;
			FIX_OUTBOUND_SESSION = null;
			FIX_OUTBOUND_SESSION_ID = null;
		}
		
		if(leaderStatus == -1) { // Connect!
			LOGGER.info("****HEARTBEAT: ***JUST BECAME LEADER!*** sproc: " + leaderStatus + "; lastIp: " + lastIpAdd + "; lastTS: " + lastTimestamp + "; Now: " + timeNow + "; Diff: " + timeDiffSec);
			
			// Harman: Commented out, call to kafka consumer and producer not needed here 
			startKafkaConsumer();
			startKafkaProducer();
	        LOGGER.fine("************* after calling getKafkaProducer() ");
			if(IM_AM_THE_CLIENT_ENGINE) {
				LOGGER.info("**************** I AM Client ENGINE***********");
				startFixClientThread();
			} else {
				LOGGER.info("**************** I AM Server ENGINE***********");
				UpdateGAEndpoints();
				startFixServerThread();
			}
			IM_AM_THE_ACTIVE_ENGINE = true;
		}
	}

	private static synchronized void startHeartbeatThread() {
		LOGGER.fine("*****START HEARTBEAT THREAD*****");
		new Thread() {
			@Override
			public void run() {
				LOGGER.info("*****HEARTBEAT THREAD RUNNING*****");
		    	CallableStatement heartbeatSprocStmt = null;
		    	if(USE_JDBC_HEARTBEAT) {
			    	loadJdbcClass();
			    	String myIp = getMyIp();
					LOGGER.fine("*****HEARTBEAT THREAD: Making SQL connection");
			    	Connection sqlDbConnection = getSqlDbConnection();
					LOGGER.fine("*****HEARTBEAT THREAD: connected to SQL DB");
			    	heartbeatSprocStmt = getHeartbeatSprocStmt(sqlDbConnection, myIp);
		    	}
		    	
				while(true) { 
					// heartbeat every second
					heartbeat(heartbeatSprocStmt);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ie) {
						LOGGER.severe("HEARTBEAT THREAD INTERRUPTED: " +ie);
					}
				}
			}
		}.start();
	}
	
	// below method is for testing only
	public static ExecutionReport generateExecution(long id) {
		String orderIdStr = "ORDER_ID_" + id;
		String execIdStr = "EXEC_ID_" + 1;
		String symbolStr = "GOOG";
		char side = Side.BUY;
//		char orderType = OrdType.MARKET;
//		char timeInForce = TimeInForce.DAY;
		ExecutionReport newExec = new ExecutionReport(new OrderID(orderIdStr), new ExecID(execIdStr), new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.PARTIAL_FILL), 
				new OrdStatus(OrdStatus.PARTIALLY_FILLED), new Symbol(symbolStr), new Side(side), new LeavesQty(250), new CumQty(50), new AvgPx(123.34));
		return newExec;
	}

    public static synchronized String getSsmParameter(String key) {
    	LOGGER.fine("GET SSM PARAMETER fetching key : " + key);
    	String stackNameEnvVar = "APPLICATION_STACK_NAME";
    	String stackName = System.getenv(stackNameEnvVar);
    	LOGGER.fine("GET SSM PARAMETER got stack name env var  : [" + stackNameEnvVar + "] value [" + stackName + "]");
    	if(stackName == null) {
    		LOGGER.severe("GET SSM PARAMETER unable to find System Environment Variable (that should contain the CloudFormation stack name that created all SSM parameters) called: " + stackNameEnvVar);
    		return null;
    	}
    	key = "/fixengine/" + stackName + "/" + key;
		try {
	    	GetParameterRequest parametersRequest = new GetParameterRequest().withName(key).withWithDecryption(false);
	    	GetParameterResult parameterResult = ssmClient.getParameter(parametersRequest);
	    	String value = parameterResult.getParameter().getValue();
		    LOGGER.fine("GET SSM PARAMETER got key : [" + key + "] value [" + value + "]");
	    	return value;
		} catch (Exception e) {
		    LOGGER.fine("GET SSM PARAMETER unable to get key : [" + key + "] : " + e);
		    return null;
	    }
    }

    private static void getDbCoordinates(String secretArn) {    	
    	LOGGER.info("GET DB PARAMETERS starting, using ARN: " + secretArn);

        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard().build();

        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretArn);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (Exception e) {
			LOGGER.severe("****GET DB COORDINATES: EXCEPTION with secretArn [" + secretArn + "]: " + e);
			e.printStackTrace();
        }

        String secret = getSecretValueResult.getSecretString();
//        System.out.println("SECRET JSON: " + secret);
        JSONParser parser = new JSONParser();
        try {
        	JSONObject parseResult = (JSONObject)parser.parse(secret);
        	JDBC_USER = parseResult.get("username").toString();
        	JDBC_PASS = parseResult.get("password").toString();
        	JDBC_URL = "jdbc:mysql://" + parseResult.get("host").toString() + ":" + parseResult.get("port").toString() + "/quickfix";
        	LOGGER.info("GET DB COORDINATES: RETREIVED DB CONNECTION " + JDBC_URL+ " WITH USER " + JDBC_USER + " AND PASSWORD WHICH IS A SECRET ");
		} catch (ParseException e) {
			LOGGER.severe("GET DB PARAMETERS: ERROR: unable to parse JSON: " + secret + " : " + e);
			e.printStackTrace();
		}
    }

    public static void deleteParam(String param) throws ConfigError {
		for (Iterator<SessionID> sections = FIX_SESSION_SETTINGS.sectionIterator(); sections.hasNext(); ) {
			SessionID s = sections.next();
			Properties props = FIX_SESSION_SETTINGS.getSessionProperties(s);
//			LOGGER.fine("SECTION = " + s + " PROPS: " + props);
			if(props.containsKey(param)) {
        		LOGGER.info("DELETE PARAM Found [" + param + "] in FIX_SESSION_SETTINGS section " + s + "! Removing!");
				props.remove(param);
			}
		}
    }

    public static void overrideParam(String param, String val) throws ConfigError {
		if (FIX_SESSION_SETTINGS.isSetting(param)) {
			LOGGER.info("OVERRIDE PARAMETERS Found [" + param + "] in FIX_SESSION_SETTINGS! Overriding with value [" + val + "]");
	    	FIX_SESSION_SETTINGS.setString(param, val);
		} else {
			for (Iterator<SessionID> sections = FIX_SESSION_SETTINGS.sectionIterator(); sections.hasNext(); ) {
				SessionID s = sections.next();
				Properties props = FIX_SESSION_SETTINGS.getSessionProperties(s);
	//			LOGGER.fine("SECTION = " + s + " PROPS: " + props);
				if(props.containsKey(param)) {
	        		LOGGER.info("OVERRIDE PARAMETERS Found [" + param + "] in FIX_SESSION_SETTINGS section " + s + "! Overriding with value [" + val + "]");
					props.setProperty(param, val);
				}
			}
		}
    }
    
    public static void overrideParameters() throws ConfigError {
    	String[] params = {"ApplicationID","FileStorePath","ConnectionType","StartTime","EndTime","HeartBtInt","UseDataDictionary","DataDictionary","ValidateUserDefinedFields","ValidateIncomingMessage","RefreshOnLogon","JdbcDriver","JdbcLogHeartBeats","JdbcStoreMessagesTableName","JdbcStoreSessionsTableName","JdbcLogIncomingTable","JdbcLogOutgoingTable","JdbcLogEventTable","JdbcSessionIdDefaultPropertyValue","setMaximumActiveTime","UseJdbcHeartbeat","UseJdbcMessageStore","KafkaOutboundTopicName","KafkaConsumerGroupID","KafkaInboundTopicName","KafkaConnTLS","NonTLSKafkaPort","TLSKafkaPort","DebugLogging","BeginString","SocketConnectHost","SocketAcceptPort","SenderCompID","TargetCompID","AcceptorTemplate"};
		LOGGER.info("**** OVERRIDE PARAMETERS STARTING");

		LOGGER.fine("OVERRIDE PARAMETERS: FIX_SESSION_SETTINGS BEFORE: " + FIX_SESSION_SETTINGS);

    	for (String param : params){
    		String val = getSsmParameter(param);
    		if(val == null) {
//    			LOGGER.fine("OVERRIDE PARAMETERS No such param in FIX_SESSION_SETTINGS: " + param);
    		} else {
    			overrideParam(param, val);
    		}
    	}

    	KAFKA_USE_TLS = FIX_SESSION_SETTINGS.getString("KafkaConnTLS");
    	String kafkaConnTLS = getSsmParameter("KafkaConnTLS");
    	if(kafkaConnTLS != null) {
    		FIX_SESSION_SETTINGS.setString("KafkaConnTLS", KAFKA_USE_TLS);
    		KAFKA_USE_TLS = kafkaConnTLS;
    	}

//    	String kafkaBootstrapBrokerString = "b-1.fixengine-msk2-m5-4x.sd3uuq.c10.kafka.us-east-1.amazonaws.com:9092,b-2.fixengine-msk2-m5-4x.sd3uuq.c10.kafka.us-east-1.amazonaws.com:9092";
    	String primaryMSKEndpoint = getSsmParameter("PrimaryMSKEndpoint");
    	String failoverMSKEndpoint = getSsmParameter("FailoverMSKEndpoint");
    	String tlsKafkaPort = getSsmParameter("TLSKafkaPort");
    	if (tlsKafkaPort != null) {
    		KAFKA_TLS_PORT = tlsKafkaPort;
    	}
    	String nonTLSKafkaPort = getSsmParameter("NonTLSKafkaPort");
    	if (nonTLSKafkaPort != null) {
    		KAFKA_NON_TLS_PORT = nonTLSKafkaPort;
    	}
		String kafkaPort = "true".equals(KAFKA_USE_TLS) ? KAFKA_TLS_PORT : KAFKA_NON_TLS_PORT;
		if(primaryMSKEndpoint==null || failoverMSKEndpoint==null || kafkaPort==null) {
			LOGGER.fine("OVERRIDE PARAMETERS: Unable to construct Kafka broker string from SSM parameters: PrimaryMSKEndpoint: " + primaryMSKEndpoint + ", FailoverMSKEndpoint: " + failoverMSKEndpoint + ", KAFKA_TLS_PORT: " + KAFKA_TLS_PORT + ", KAFKA_NON_TLS_PORT: " + KAFKA_NON_TLS_PORT + ", KAFKA_USE_TLS: " + KAFKA_USE_TLS);
		} else {
			String kafkaBootstrapBrokerString = primaryMSKEndpoint+":"+kafkaPort+","+failoverMSKEndpoint+":"+kafkaPort;
	    	FIX_SESSION_SETTINGS.setString("KafkaBootstrapBrokerString", kafkaBootstrapBrokerString);
	    	KAFKA_BROKER_STRING = kafkaBootstrapBrokerString;
		}
    	LOGGER.fine("############################ PARAMETER KafkaBootstrapBrokerString: " + KAFKA_BROKER_STRING);
    	
		LOGGER.info("OVERRIDE PARAMETERS: FIX_SESSION_SETTINGS AFTER OVERRIDES: " + FIX_SESSION_SETTINGS);
    }
    
    public static void setLogLevel() {
   		Level logLevel = Level.INFO;
        try {
	    	if("true".equals(FIX_SESSION_SETTINGS.getString("DebugLogging"))) {
	    		logLevel = Level.FINE;
	    	}
    	} catch(ConfigError e) {
    		LOGGER.info("INITIALIZE PARAMETERS: Did not find a DebugLogging parameter in config file, so assuming INFO level");
    	}
    	LOGGER.setLevel(logLevel);
//        ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(logLevel);
//        LOGGER.addHandler(handler);	        	
		LOGGER.info("MAIN: SET LOG LEVEL TO " + logLevel);
		// LOGGER.fine("MAIN: A FINE LOG TEST");
    }
    
    public static void initializeParameters(String configfile) throws ConfigError {    	
    	try {
			FIX_SESSION_SETTINGS = new SessionSettings(configfile);
		} catch (ConfigError e) {
    		LOGGER.info("INITIALIZE PARAMETERS: Unable to create new SessionSettings from config file " + configfile);
    		e.printStackTrace();
    		throw e;
		}
		
		setLogLevel();

        try { KAFKA_BROKER_STRING = FIX_SESSION_SETTINGS.getString("KafkaBootstrapBrokerString"); } catch (ConfigError e) {}
        try { KAFKA_USE_TLS = FIX_SESSION_SETTINGS.getString("KafkaConnTLS"); } catch (ConfigError e) {}
        try { KAFKA_TLS_PORT = FIX_SESSION_SETTINGS.getString("TLSKafkaPort"); } catch (ConfigError e) {}
        try { KAFKA_NON_TLS_PORT = FIX_SESSION_SETTINGS.getString("NonTLSKafkaPort"); } catch (ConfigError e) {}
		
    	overrideParameters();
    	
    	try {
    		IM_AM_THE_CLIENT_ENGINE = ("initiator".equals(FIX_SESSION_SETTINGS.getString("ConnectionType")));
    	} catch(ConfigError e) {
    		LOGGER.info("INITIALIZE PARAMETERS: Did not find a ConnectionType parameter in config file, so assuming I'll be the server");
    		IM_AM_THE_CLIENT_ENGINE = false;
    	}
    	
    	if(IM_AM_THE_CLIENT_ENGINE) {
    		deleteParam("AcceptorTemplate");
    		deleteParam("SocketAcceptPort");
    	}

		setLogLevel();

    	try {
			JDBC_DRIVER = FIX_SESSION_SETTINGS.getString("JdbcDriver");
			USE_JDBC_HEARTBEAT = "true".equals(FIX_SESSION_SETTINGS.getString("UseJdbcHeartbeat"));
			USE_JDBC_MESSAGE_STORE = "true".equals(FIX_SESSION_SETTINGS.getString("UseJdbcMessageStore"));    		
	        KAFKA_OUTBOUND_TOPIC_NAME = FIX_SESSION_SETTINGS.getString("KafkaOutboundTopicName");
	        KAFKA_INBOUND_CONSUMER_GORUP_ID = FIX_SESSION_SETTINGS.getString("KafkaConsumerGroupID");
	        KAFKA_INBOUND_TOPIC_NAME = FIX_SESSION_SETTINGS.getString("KafkaInboundTopicName");
    	} catch(ConfigError e) {
    		LOGGER.severe("MAIN: error retreiving required config file propoerty:" + e);
    		e.printStackTrace();
    		throw e;
    	}
    	
    	String dbSecretArn = getSsmParameter("RDSClusterNonAdminSecretArn");
    	LOGGER.fine("############################ PARAMETER RDSClusterSecretArn: " + dbSecretArn);
		getDbCoordinates(dbSecretArn);
		if(JDBC_USER==null || JDBC_PASS==null || JDBC_URL==null) {
			throw new ConfigError("UNABLE TO START: JDBC_USER, JDBC_PASS or JDBC_URL is NULL! ( from RDSClusterNonAdminSecretArn: " + dbSecretArn);
		}
    	FIX_SESSION_SETTINGS.setString("JdbcUser", JDBC_USER);
    	FIX_SESSION_SETTINGS.setString("JdbcPassword", JDBC_PASS);
    	FIX_SESSION_SETTINGS.setString("JdbcURL", JDBC_URL);
    	
//    	String gaEndpointGroupArn = "arn:aws:globalaccelerator::015331511911:accelerator/0a63fdd3-a83a-4049-ab49-3b9a96075be7/listener/1307d1f8/endpoint-group/7cc66b930002";
    	String globalAcceleratorEndpointGroupArnParamName = "GlobalAcceleratorEndpointGroupArn";
    	GA_ENDPOINT_GROUP_ARN = getSsmParameter(globalAcceleratorEndpointGroupArnParamName);
    	LOGGER.fine("############################ PARAMETER " + globalAcceleratorEndpointGroupArnParamName + " : " + GA_ENDPOINT_GROUP_ARN);
		if(GA_ENDPOINT_GROUP_ARN==null) {
			throw new ConfigError("UNABLE TO START: " + globalAcceleratorEndpointGroupArnParamName + " is NULL!");
		}
//    	FIX_SESSION_SETTINGS.setString("GAEndpointGroupArn", globalAcceleratorEndpointArn);

        String gaEndpointArnEnvVar = "GLOBAL_ACCELERATOR_ENDPOINT_ARN";
		GA_MY_ENDPOINT_ARN = System.getenv(gaEndpointArnEnvVar);
		LOGGER.fine("\"############################ PARAMETER [" + gaEndpointArnEnvVar + "] value [" + GA_MY_ENDPOINT_ARN + "]");
		if(GA_MY_ENDPOINT_ARN == null) {
			throw new ConfigError("Unable to get System Environment Variable (that should be contained in the CloudFormation stack template with Env Variable) called: " + gaEndpointArnEnvVar);
		}		
    }

    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {
    	LOGGER.setLevel(Level.INFO);
    	// LOGGER.setLevel(Level.FINE);
    	
    	// getDbCoordinates("arn:aws:secretsmanager:us-east-1:015331511911:secret:RDSClusterAdminSecret-CIdlaHSCXP5c-ZifY9k");
    	// getDbCoordinates("arn:aws:secretsmanager:us-east-1:015331511911:secret:RDSClusterNonAdminSecret-iMVUaYcWI9sS-fkEV9c");
    	// if (1==1) 
    	// 	System.exit(0);
    	
    	String configfile = "config/server.cfg";
    	if(args.length > 0) {
    		configfile = args[0];
    	}
		LOGGER.info("***MAIN STARTING WITH CONFIG FILE: " + configfile);

    	initializeParameters(configfile);
    	
    	//IM_AM_THE_CLIENT_ENGINE = true;
    	    	
    	LOGGER.info("MAIN: STARTING HEARTBEAT");
    	startHeartbeatThread();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }   
}