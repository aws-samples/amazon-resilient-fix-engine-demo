package com.amazonaws.fixengineonaws;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParametersByPathResult;
import com.amazonaws.services.simplesystemsmanagement.model.Parameter;

import quickfix.Acceptor;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldConvertError;
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
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;

public class FixEngine implements Application {
    private static Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
    private static String MY_IP = getMyIp();
    private static volatile SessionID FIX_OUTBOUND_SESSION_ID = null;
    private static boolean IM_AM_THE_ACTIVE_ENGINE = false;
    private static KafkaProducer<String, String> KAFKA_PRODUCER = null;
    private static KafkaConsumer<String, Object> KAFKA_CONSUMER = null;
    private static String KAFKA_INBOUND_TOPIC_NAME;
    private static long messageCounter = 0;
    private static Acceptor FIX_SERVER = null;
    private static Initiator FIX_CLIENT = null;
    private static Session FIX_OUTBOUND_SESSION = null;
    private static Session FIX_INBOUND_SESSION = null;
    private static AWSSimpleSystemsManagement SSM_CLIENT = null;

    private static final int LEADER_STATUS_STILL_LEADER = 1;
    private static final int LEADER_STATUS_STILL_NOT_LEADER = 0;
    private static final int LEADER_STATUS_JUST_BECAME_LEADER = -1;

    @Override
    public void onCreate(SessionID sessionID) {
        LOGGER.fine(MY_IP+"OnCreate");
    }

    @Override
    public void onLogon(SessionID sessionID) {
        LOGGER.info(MY_IP+"OnLogon session ID: " + sessionID);
        FIX_OUTBOUND_SESSION_ID = sessionID;
    }

    @Override
    public void onLogout(SessionID sessionID) {
        LOGGER.info(MY_IP+"OnLogout session ID: " + sessionID);
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
        LOGGER.info(MY_IP+"%%%%%%%% TOAPP: " + message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        LOGGER.info(MY_IP+"%%%%%%%% FROMAPP: " + message);
        
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
            KAFKA_PRODUCER.send(new ProducerRecord<String, String>(KAFKA_INBOUND_TOPIC_NAME, parsedOrdStr)).get();
        } catch (Exception e) {
            LOGGER.severe(MY_IP+"%%%%%%%% FROMAPP: Exception:" + e);
            e.printStackTrace();
        }       
    }

    public static String getMyIp() {
        try {
            InetAddress inet = InetAddress.getLocalHost();
            return inet.getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.severe(MY_IP+"ERROR: Unable to find my own IP address!" + e);
            e.printStackTrace();
            return "???.???.???.???";
        }
    }

    public static void setLogLevel(boolean enableFineGrainedLogging) {
        Level logLevel = enableFineGrainedLogging ? Level.FINE : Level.INFO;
        LOGGER.setLevel(logLevel);
        LOGGER.info(MY_IP+"MAIN: SET LOG LEVEL TO " + logLevel);
        // LOGGER.fine(MY_IP+"MAIN: A FINE LOG TEST");
    }

    private static void loadJdbcClass(String jdbcDriver) {
        try {
            Class.forName(jdbcDriver); 
        } catch (ClassNotFoundException e) {
            LOGGER.severe(MY_IP+"UNABLE TO LOAD JDBC DRIVER:" + jdbcDriver);
            e.printStackTrace();
            return;
        }
        LOGGER.fine(MY_IP+"LOADED JDBC DRIVER:" + jdbcDriver);
    }   

    public static String getSsmParameterPath() {
	    LOGGER.info(MY_IP+"**********GET SSM PARAMETER PATH starting");
		String stackNameEnvVar = "APPLICATION_STACK_NAME";
	    String stackName = System.getenv(stackNameEnvVar);
	    LOGGER.fine(MY_IP+"GET SSM PARAMETER PATH got stack name env var  : [" + stackNameEnvVar + "] value [" + stackName + "]");
	    if(stackName == null) {
	        LOGGER.severe(MY_IP+"GET SSM PARAMETER unable to find System Environment Variable (that should contain the CloudFormation stack name that created all SSM parameters) called: " + stackNameEnvVar);
	        return null;
	    }
	    String path = "/fixengine/" + stackName;
	    return path;
    }
    
    public static String getSsmParameter(String parameterPath, String parameterName) throws ConfigError {
	    LOGGER.info(MY_IP+"**********GET SSM PARAMETER looking up " + parameterPath + "/" + parameterName + " using " + SSM_CLIENT);
    	if(System.getProperty("os.name").contains("Windows")) {
    		LOGGER.info(MY_IP+"GET SSM PARAMETER PATH returning dummy value because we're running on Windows not Unix");
//    		return new HashMap<String, String>();
    		if("GLOBAL_ACCELERATOR_ENDPOINT_ARN".equals(parameterName)) {
    			return "arn:aws:elasticloadbalancing:us-east-1:015331511911:loadbalancer/net/FixEn-Prima-JV82REH1OXV5/44c96ca1cc0dceec";
    		}
    		HashMap<String, String> ret = new HashMap<String, String>();
    		ret.put("TLSKafkaPort","9094");
    		ret.put("SenderCompID","client");
    		ret.put("ConnectionType","initiator");
    		ret.put("PrimaryMSKEndpoint","b-1.fixengineonaws-client.pupo46.c6.kafka.us-east-1.amazonaws.com");
    		ret.put("KafkaConnTLS","false");
    		ret.put("TargetCompID","server");
    		ret.put("NonTLSKafkaPort","9092");
    		ret.put("DebugLogging","true");
    		ret.put("FIXServerPort","9877");
    		ret.put("FailoverMSKEndpoint","b-2.fixengineonaws-client.pupo46.c6.kafka.us-east-1.amazonaws.com");
    		ret.put("FIXServerDNSName","a98128cf808f6358e.awsglobalaccelerator.com");
    		ret.put("ApplicationID","client");
    		ret.put("RDSClusterSecretArn","arn:aws:secretsmanager:us-east-1:015331511911:secret:RDSClusterAdminSecret-L9C42cRuF7p2-L7HsvC");
    		ret.put("RDSClusterNonAdminSecretArn","arn:aws:secretsmanager:us-east-1:015331511911:secret:RDSClusterNonAdminSecret-waqyosb9knZt-adX48c");
    		ret.put("GlobalAcceleratorEndpointGroupArn", "arn:aws:elasticloadbalancing:us-east-1:015331511911:loadbalancer/net/FixEn-Failo-BM0E1KC5AQ2K/4df267784903750a");
    		return(ret.get(parameterName));
    	}

    	if("GLOBAL_ACCELERATOR_ENDPOINT_ARN".equals(parameterName)) {
		    String GAEndpointArn = System.getenv(parameterName);
		    LOGGER.fine(MY_IP+"GET SSM PARAMETER PATH got GA endpoint env var  : [" + parameterName + "] value [" + GAEndpointArn + "]");
		    if(GAEndpointArn == null) {
		        LOGGER.severe(MY_IP+"GET SSM PARAMETER unable to find System Environment Variable (that should contain the CloudFormation stack name that created all SSM parameters) called: " + parameterName);
		    }
		    return GAEndpointArn;
    	}
    	
    	if(SSM_CLIENT == null) {
    		SSM_CLIENT = AWSSimpleSystemsManagementClientBuilder.standard().build();
    		if(SSM_CLIENT == null) {
    			LOGGER.severe(MY_IP+"GET SSM PARAMETER unable to create an AWSSimpleSystemsManagementClientBuilder! Check IAM privileges to see if your process has enough access to read params!");
    			throw new ConfigError(MY_IP+"GET SSM PARAMETER unable to create an AWSSimpleSystemsManagementClientBuilder! Check IAM privileges to see if your process has enough access to read params!");
    		}
    	}
        String key = parameterPath + "/" + parameterName;
        try {
            GetParameterRequest parametersRequest = new GetParameterRequest().withName(key).withWithDecryption(false);
            GetParameterResult parameterResult = SSM_CLIENT.getParameter(parametersRequest);
            String value = parameterResult.getParameter().getValue();
            LOGGER.fine(MY_IP+"GET SSM PARAMETER got key : [" + key + "] value [" + value + "]");
            return value;
        } catch (Exception e) {
            LOGGER.fine(MY_IP+"GET SSM PARAMETER unable to get key : [" + key + "] : " + e);
            throw e;
        }
    }

    private static void addSqlDbConnectionCoordinatesToSettings(String secretArn, SessionSettings sessionSettings) {        
        LOGGER.info(MY_IP+"*********************GET SQL DB CONNECTION starting, using ARN: " + secretArn);
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard().build();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretArn);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (Exception e) {
            LOGGER.severe(MY_IP+"****GET DB COORDINATES: EXCEPTION with secretArn [" + secretArn + "]: " + e);
            e.printStackTrace();
        }

        String secret = getSecretValueResult.getSecretString();
//        System.out.println("SECRET JSON: " + secret);
        JSONParser parser = new JSONParser();
        try {
            JSONObject parseResult = (JSONObject)parser.parse(secret);
            sessionSettings.setString("JdbcUser", parseResult.get("username").toString());
            sessionSettings.setString("JdbcPassword", parseResult.get("password").toString());
            sessionSettings.setString("JdbcURL", "jdbc:mysql://" + parseResult.get("host").toString() + ":" + parseResult.get("port").toString() + "/quickfix");            
        } catch (ParseException e) {
            LOGGER.severe(MY_IP+"GET DB PARAMETERS: ERROR: unable to parse JSON: " + secret + " : " + e);
            e.printStackTrace();
        }

    }
    
    private static Connection getSqlDbConnection(String jdbcUrl, String jdbcUser, String jdbcPass) {        
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

    private static void overrideConfigFromSsmParameters(SessionSettings sessionSettings) throws ConfigError {
    	LOGGER.info(MY_IP+"****OVERRIDE CONFIG FROM SSM PARAMETERS starting");
        String ssmParameterPath = getSsmParameterPath();
    	ArrayList<Properties> allProperties = new ArrayList<Properties>();
    	allProperties.add(sessionSettings.getDefaultProperties());
        for (Iterator<SessionID> sections = sessionSettings.sectionIterator(); sections.hasNext(); ) {
            SessionID section = sections.next();
	        try {
            	allProperties.add(sessionSettings.getSessionProperties(section));
	        } catch (ConfigError e) {
	        	LOGGER.severe(MY_IP+"OVERRIDE PARAMETERS Unable to process section [" + section + "]");
	        	e.printStackTrace();
	        }
        }
//        LOGGER.info(MY_IP+"OVERRIDE CONFIG FROM SSM PARAMETERS got session params: " + allProperties);
        for (int i = 0; i < allProperties.size(); i++) {
        	Properties sessionProperties = allProperties.get(i);
            LOGGER.info(MY_IP+"PROPS: " + sessionProperties);
            for (Iterator sessionPropertyIter = sessionProperties.entrySet().iterator(); sessionPropertyIter.hasNext(); ) {
            	Map.Entry<String, String> sessionProperty = (Map.Entry<String, String>)sessionPropertyIter.next();
//            	String propertyKey = (String)sessionProperty.getKey();
            	String propertyVal = (String)sessionProperty.getValue();
//                LOGGER.info(MY_IP+"OVERRIDE PARAMETERS in FIX_SESSION_SETTINGS looking at property [" + propertyKey + "] with value [" + propertyVal + "]");

            	Matcher m = Pattern.compile("<(.+?)>").matcher(propertyVal);
            	while (m.find()) {
            		String token = m.group();
            		String ssmParameterName = token.replace("<", "").replace(">", "");
                	String ssmParamVal = getSsmParameter(ssmParameterPath, ssmParameterName);
                	String newValue = propertyVal.replace(token,ssmParamVal);
                	System.out.println("OVERRIDE CONFIG FROM SSM PARAMETERS in [" + propertyVal + "] replaced [" + token + "] with [" + ssmParamVal + "] to get [" + newValue + "]");
                	propertyVal = newValue;
                	sessionProperty.setValue(newValue);            		
            	}
            }
        }
    }

    public static SessionSettings initializeParameters(String configfile) throws ConfigError {
    	LOGGER.info(MY_IP+"****INITIALIZE PARAMETERS starting");    	
    	SessionSettings sessionSettings = null;
    	try {
            sessionSettings = new SessionSettings(configfile);
        } catch (ConfigError e) {
            LOGGER.info(MY_IP+"INITIALIZE PARAMETERS: Unable to create new SessionSettings from config file " + configfile);
            e.printStackTrace();
            throw e;
        }

        setLogLevel(true);

        overrideConfigFromSsmParameters(sessionSettings);
        if ("<DebugLogging>".equals(sessionSettings.getString("DebugLogging"))) {
        	sessionSettings.setString("DebugLogging","false");
        }        
    	LOGGER.info(MY_IP+"INITIALIZE PARAMETERS finished overriding params and got: " + sessionSettings);    	
//        checkConfigHealthy(configfile);
        return sessionSettings;
    }

    private static KafkaProducer<String, String> startKafkaProducer(String kafkaBrokerString) {
		LOGGER.info(MY_IP+"****START KAFKA OUTBOUND PRODUCER START*****");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokerString);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }

    private static KafkaConsumer<String, Object> startKafkaConsumer(String kafkaBrokerString, String consumerGroupId, String topicName) {
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

    private static void processOneInboundKafkaMessage(ConsumerRecord<String, Object> kafkaMessage) {
        LOGGER.info(MY_IP+"****PROCESS ONE KAFKA MESSAGE: processing " + kafkaMessage.value().toString());
        String ordStr = kafkaMessage.value().toString();
        Message parsedOrd = null;
        try {
            parsedOrd = quickfix.MessageUtils.parse(FIX_OUTBOUND_SESSION, ordStr);
        } catch (InvalidMessage e) {
        	LOGGER.severe(MY_IP+"ERROR PARSING MESSAGE: " + ordStr);
            e.printStackTrace();
        }
        LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: PARSED   MESSAGE: " + parsedOrd);
        LOGGER.fine(MY_IP+"****PROCESS KAFKA MSGS: PARSED    HEADER: " + parsedOrd.getHeader());
                    
        //[CLIENT FIX ENGINE] SEND ORDER FIX TO SERVER FIX ENGINE
        try {
        	LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: SENDING MESSAGE TO FIX: " + parsedOrd);	        	        	
            Session.sendToTarget(parsedOrd, FIX_OUTBOUND_SESSION_ID);
        } catch (SessionNotFound se) {
        	LOGGER.severe(MY_IP+"****PROCESS KAFKA MSGS: SessionNotFound: " + se);
            se.printStackTrace();
        } catch (Exception e) {
        	LOGGER.severe(MY_IP+"****PROCESS KAFKA MSGS: Exception: " + e);
            e.printStackTrace();
        }        
    }

    private static void processInboundKafkaMsgs(KafkaConsumer<String, Object> kafkaConsumer) {
        LOGGER.info(MY_IP+"****PROCESS KAFKA MSGS: ************* after calling getKafkaConsumer ");
        int count = 0;
        if(IM_AM_THE_ACTIVE_ENGINE && FIX_OUTBOUND_SESSION_ID != null) {
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
            ConsumerRecords<String, Object> records = kafkaConsumer.poll(50);
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
                    processOneInboundKafkaMessage(record);
                }
            }
        } //while loop
    }

	private static void updateGAEndpoints(String myGaEndpointArn) {
        LOGGER.info(MY_IP+"UPDATE GA ENDPOINT starting for myGaEndpointArn: "+ myGaEndpointArn);
        String activeEndpoint = null;
        String passiveEndpoint = null;
        String tobeActiveEndpoint = null;

        tobeActiveEndpoint = myGaEndpointArn;

        // following code if rumnning on EC2
        // String ec2InstanceID = EC2MetadataUtils.getInstanceId();
        // LOGGER.info(MY_IP+" ec2InstanceID " + ec2InstanceID);
        // tobeActiveEndpoint = ec2InstanceID;

        AWSGlobalAccelerator amazonGlobalAcceleratorClient = AWSGlobalAcceleratorClientBuilder.standard().withRegion(Regions.US_WEST_2).build();

        DescribeEndpointGroupResult describeEndpointGroupResult = amazonGlobalAcceleratorClient.describeEndpointGroup(new DescribeEndpointGroupRequest().withEndpointGroupArn(myGaEndpointArn));
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
        amazonGlobalAcceleratorClient.updateEndpointGroup(new UpdateEndpointGroupRequest().withEndpointGroupArn(myGaEndpointArn).withEndpointConfigurations(endpointConfiguration));
    }

	private static CallableStatement getHeartbeatSprocStmt(String jdbcDriver, String jdbcUrl, String jdbcUser, String jdbcPass) {
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

    private static synchronized int getLeaderStatus(CallableStatement heartbeatSprocStmt, boolean iAmTheLeader, boolean useJdbcHeartbeat) {
//      LOGGER.fine(MY_IP+"*********************HEARTBEAT********************");  
        int leaderStatus = 0;
        String lastIpAdd = "";
        Timestamp lastTimestamp = null;
        Timestamp timeNow = null;
        int timeDiffSec = 0;
//      LOGGER.fine(MY_IP+"****HEARTBEAT: USE_JDBC: " + USE_JDBC + "; heartbeatSprocStmt = " + heartbeatSprocStmt);
        if(!useJdbcHeartbeat) {
            if(iAmTheLeader) {
                leaderStatus = 1;
            } else {
                leaderStatus = -1;
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
                return LEADER_STATUS_STILL_NOT_LEADER;
            }
        }
		return leaderStatus;
    }

    private static void startFixServer(SessionSettings sessionSettings) throws ConfigError {
	    LOGGER.info(MY_IP+"****STARTING FIX SERVER APPLICATION");           
	    if(FIX_SERVER!=null) {
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_SERVER object already exists!");
	    } else {
	        Application application = new FixEngine();
	        MessageStoreFactory messageStoreFactory = null;
	        if("true".equals(sessionSettings.getString("UseJdbcMessageStore"))) { 
	        	messageStoreFactory = new JdbcStoreFactory(sessionSettings);
	        } else {
	        	messageStoreFactory = new FileStoreFactory(sessionSettings);
	        }
	        LogFactory logFactory = new ScreenLogFactory(true, true, true);
	        MessageFactory messageFactory = new DefaultMessageFactory();
	    	FIX_SERVER = new SocketAcceptor(application, messageStoreFactory, sessionSettings, logFactory, messageFactory);
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_SERVER object created: " + FIX_SERVER);	    	
	    }

	    if(FIX_INBOUND_SESSION!=null) {
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_INBOUND_SESSION object already exists!");
	    } else {
	    	FIX_SERVER.start();
	    	FIX_INBOUND_SESSION = Session.lookupSession(FIX_SERVER.getSessions().get(0));
		    LOGGER.info(MY_IP+"START FIX SERVER: FIX_INBOUND_SESSION object created: " + FIX_INBOUND_SESSION);
	    }
    }

    private static void startFixClient(SessionSettings sessionSettings) throws ConfigError {
	    LOGGER.info(MY_IP+"****STARTING FIX CLIENT APPLICATION");
	    if(FIX_CLIENT!=null) {
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_CLIENT object already exists!");
	    } else {
	        Application applicationClient = new FixEngine();
	//                      MessageStoreFactory messageStoreFactoryClient = new FileStoreFactory(FIX_SESSION_SETTINGS);
	        MessageStoreFactory messageStoreFactoryClient = null;
	        if("true".equals(sessionSettings.getString("UseJdbcMessageStore"))) { 
	            messageStoreFactoryClient = new JdbcStoreFactory(sessionSettings);
	        } else {
	            messageStoreFactoryClient = new FileStoreFactory(sessionSettings);
	        }                               
	        LogFactory logFactoryClient = new ScreenLogFactory(true, true, true);
	        MessageFactory messageFactoryClient = new DefaultMessageFactory();
	
	        FIX_CLIENT = new SocketInitiator(applicationClient, messageStoreFactoryClient, sessionSettings, logFactoryClient, messageFactoryClient);
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_CLIENT object created: " + FIX_CLIENT);	    	
	    }

	    if(FIX_OUTBOUND_SESSION_ID!=null) {
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION_ID object already exists!");
	    } else {
	        FIX_CLIENT.start();
	        FIX_OUTBOUND_SESSION = Session.lookupSession(FIX_CLIENT.getSessions().get(0)); 
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION object created: " + FIX_OUTBOUND_SESSION);
	
	        while(FIX_OUTBOUND_SESSION_ID == null) {
		        LOGGER.info(MY_IP+"****QUICKFIX CLIENT START: WAITING FOR SERVER..." );
		        try {
		          Thread.sleep(500);  
		        } catch (InterruptedException ie) {
		            LOGGER.info(MY_IP+"****QUICKFIX CLIENT START: FixEngine THREAD INTERRUPTED: " + ie);
		        }
		    }
		    LOGGER.info(MY_IP+"START FIX CLIENT: FIX_OUTBOUND_SESSION_ID connection established: " + FIX_OUTBOUND_SESSION_ID);
	    }
    }

    private static void stopFixServer(){
	    LOGGER.info(MY_IP+"****STOPPING FIX SERVER APPLICATION");           
    	FIX_SERVER.stop();
	    FIX_OUTBOUND_SESSION=null;
	    FIX_SERVER=null;
    }

    private static void stopFixClient() {
	    LOGGER.info(MY_IP+"****STOPPING FIX CLIENT APPLICATION");
	    if(FIX_CLIENT != null) {
	    	FIX_CLIENT.stop();
	    }
	    FIX_OUTBOUND_SESSION_ID=null;
        FIX_CLIENT=null;
        FIX_OUTBOUND_SESSION_ID=null;
    }

    private static void heartbeatMessageProcessingLoop(SessionSettings sessionSettings) throws ConfigError {
        LOGGER.info(MY_IP+"*****START HEARTBEAT *****");
    	boolean iAmClientFixEngine = "initiator".equals(sessionSettings.getString("ConnectionType"));
    	String kafkaBrokerString = sessionSettings.getString("kafkaBootstrapBrokerString");
    	String consumerGroupId = sessionSettings.getString("KafkaConsumerGroupID");
    	String kafkaOutboundTopicName = sessionSettings.getString("KafkaOutboundTopicName");
    	KAFKA_INBOUND_TOPIC_NAME = sessionSettings.getString("KafkaInboundTopicName");
    	String myGAEndpointArn = iAmClientFixEngine ? null : sessionSettings.getString("GAEndpointArn");    	
    	boolean useJdbcConnection = "true".equals(sessionSettings.getString("UseJdbcHeartbeat"));
        
        addSqlDbConnectionCoordinatesToSettings(sessionSettings.getString("RDSClusterSecretArn"), sessionSettings);
        CallableStatement heartbeatSprocStmt = getHeartbeatSprocStmt(sessionSettings.getString("JdbcDriver"), sessionSettings.getString("JdbcURL"), sessionSettings.getString("JdbcUser"), sessionSettings.getString("JdbcPassword"));

//        startFixServer(sessionSettings); // to let health check know we're alive

        while(true) { 
			LOGGER.info(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE);
    		int leaderStatus = getLeaderStatus(heartbeatSprocStmt, IM_AM_THE_ACTIVE_ENGINE, useJdbcConnection);
			LOGGER.info(MY_IP+"**************** HEARTBEAT: iAmClientFixEngine: " + iAmClientFixEngine + " ; IM_AM_THE_ACTIVE_ENGINE: " + IM_AM_THE_ACTIVE_ENGINE + " ; leaderStatus: " + leaderStatus);
    		
    	    if(leaderStatus == LEADER_STATUS_JUST_BECAME_LEADER) {
    			LOGGER.info(MY_IP+"****HEARTBEAT: ***I'M STILL LEADER OR JUST BECAME LEADER! ENSURING ENGINES ARE RUNNING!***");
    	        if(KAFKA_CONSUMER == null) { KAFKA_CONSUMER = startKafkaConsumer(kafkaBrokerString, consumerGroupId, kafkaOutboundTopicName); }
    	        if(KAFKA_PRODUCER == null) { KAFKA_PRODUCER = startKafkaProducer(kafkaBrokerString); }
    	        if(iAmClientFixEngine) {
    	            startFixClient(sessionSettings);
    	        } else {
    	            LOGGER.info(MY_IP+"**************** HEARTBEAT: I AM Server ENGINE***********");
    	            startFixServer(sessionSettings);
    	            if("10.130.0.66".equals(MY_IP)) {
    	            	LOGGER.severe(MY_IP+"**************** HEARTBEAT: NOT UPDATING GLOBAL ACCELERATOR ENDPOINT BECAUSE WE DONT HAVE ACCESS FROM THIS MACHINE!!!***********");
    	            } else {
    	            	updateGAEndpoints(myGAEndpointArn);
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

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                LOGGER.severe(MY_IP+"HEARTBEAT THREAD INTERRUPTED: " +ie);
            }
        }
    }

    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {
        LOGGER.setLevel(Level.INFO);
        // LOGGER.setLevel(Level.FINE);

        String configfile = "config/server.cfg";
        if(args.length > 0) {
            configfile = args[0];
        }
        LOGGER.info(MY_IP+"***MAIN STARTING WITH CONFIG FILE: " + configfile);

//        Map<String, String> params = getSsmParameters();
//        System.out.println("*************** SSM PARAMS: " + params);

        SessionSettings sessionSettings = initializeParameters(configfile);

//        //IM_AM_THE_CLIENT_ENGINE = true;

        LOGGER.info(MY_IP+"MAIN: STARTING HEARTBEAT");
        heartbeatMessageProcessingLoop(sessionSettings);

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    	LOGGER.info(MY_IP+"MAIN: GOT TO THE END! EXITING!");
    }   

}
