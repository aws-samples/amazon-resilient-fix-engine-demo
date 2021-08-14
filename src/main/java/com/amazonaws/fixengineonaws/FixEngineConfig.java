package com.amazonaws.fixengineonaws;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;

import quickfix.ConfigError;
import quickfix.SessionID;
import quickfix.SessionSettings;

/**
 * FixEngineConfig: helper class that wraps populates and validates a quickfix.SessionSettings object, overriding any tokens (denoted by <token> syntax) with values from SSM Parameters
 * Retrieves GLOBAL_ACCELERATOR_ENDPOINT_ARN and APPLICATION_STACK_NAME from the Java runtime's OS ENV variables
 * SSM paths are expected to look like: /fixengine/<APPLICATION_STACK_NAME>/<token name>
 */
public class FixEngineConfig {
    private Logger LOGGER = null;
    private SessionSettings sessionSettings = null;
    private AWSSimpleSystemsManagement SSM_CLIENT = null;
    private String parameterPath = null;
    private final String[] requiredClientConfigFields = {"ApplicationID","FileStorePath","ConnectionType","StartTime","EndTime","HeartBtInt","UseDataDictionary","DataDictionary","ValidateUserDefinedFields","ValidateIncomingMessage","RefreshOnLogon","JdbcDriver","JdbcLogHeartBeats","JdbcStoreMessagesTableName","JdbcStoreSessionsTableName","JdbcLogIncomingTable","JdbcLogOutgoingTable","JdbcLogEventTable","JdbcSessionIdDefaultPropertyValue","setMaximumActiveTime","UseJdbcHeartbeat","UseJdbcMessageStore","KafkaOutboundTopicName","KafkaConsumerGroupID","KafkaInboundTopicName","kafkaBootstrapBrokerString","RDSClusterSecretArn","DebugLogging","BeginString","SenderCompID","TargetCompID","SocketConnectHost","SocketConnectPort"};
    private final String[] requiredServerConfigFields = {"ApplicationID","FileStorePath","ConnectionType","StartTime","EndTime","HeartBtInt","UseDataDictionary","DataDictionary","ValidateUserDefinedFields","ValidateIncomingMessage","RefreshOnLogon","JdbcDriver","JdbcLogHeartBeats","JdbcStoreMessagesTableName","JdbcStoreSessionsTableName","JdbcLogIncomingTable","JdbcLogOutgoingTable","JdbcLogEventTable","JdbcSessionIdDefaultPropertyValue","setMaximumActiveTime","UseJdbcHeartbeat","UseJdbcMessageStore","KafkaOutboundTopicName","KafkaConsumerGroupID","KafkaInboundTopicName","kafkaBootstrapBrokerString","RDSClusterSecretArn","DebugLogging","BeginString","SenderCompID","TargetCompID","GAEndpointGroupArn","GAEndpointArn","SocketAcceptPort","AcceptorTemplate"};
    
    /**
     * Constructor initializes a quickfix.SessionSettings object from YAML .cfg file denoted by <configfile> parameter, overrides it with SSM parameters and validates it, throwing an error for any required fields that are missing or can't be detokenized
     * @param configfile
     * @param logger java.util.logging.Logger from the caller class (or creates a new one if the parameter is null)
     * @throws ConfigError
     */
	public FixEngineConfig(String configfile, Logger logger) throws ConfigError {
		LOGGER = (logger!=null) ? logger : Logger.getLogger(FixEngine.class.getName());
	    LOGGER.info("FIXENGINECONFIG CONSTRUCTOR: GETTING SETINGS FROM CONFIG FILE: " + configfile);
	    parameterPath = getSsmParameterPath();
	    LOGGER.info("FIXENGINECONFIG CONSTRUCTOR: GOT SSM PARAMETER PREFIX: " + parameterPath);
	    sessionSettings = initializeParameters(configfile);
	    LOGGER.info("FIXENGINECONFIG CONSTRUCTOR: VALIDATING PARAMETERS: " + sessionSettings);
	    String validationErrors = validateSessionSettings();
	    if(!"none".equals(validationErrors)) {
		    LOGGER.severe("FIXENGINECONFIG CONSTRUCTOR: UNABLE TO START DUE TO CONFIG VALIDATION ERROR: " + validationErrors);
		    throw new ConfigError(validationErrors);
	    }
	}
	
	/**
	 * Returns populated, detokenized and validated quickfix.SessionSettings object
	 * @return
	 */
	public SessionSettings getSessionSettings() {
		return sessionSettings;
	}
	
	/**
	 * Returns a clone of the current populated, detokenized and validated quickfix.SessionSettings object, but with a new FIX port value substituted into the FIXServerPort token
	 * @return
	 */
	public SessionSettings cloneSessionSettingsWithNewPort(String port) {
		SessionSettings temp = cloneSessionSettings(sessionSettings);
		setSessionSetting("SocketAcceptPort", port, temp);
		setSessionSetting("SocketConnectPort", port, temp);
		temp = cloneSessionSettings(temp);
		return temp;
	}

	/**
	 * Returns a new SessionSettings object with the exact same content as the original 
	 * This is a wourkaround for a bug in sessionSettings where simply setting properties in the "session" section
	 * gets reflected in the underlying Hashtables and toString() but not in the date it exposes to the Fix SocketAcceptor/SocketInitiator constructor
	 * resulting in token strings like "<TargetCompID>" being used by the resulting FIX engine instead of the overridden values
	 */
	public SessionSettings cloneSessionSettings(SessionSettings sessionSettings) {
        try {
            ByteArrayOutputStream oos = new ByteArrayOutputStream();
            sessionSettings.toStream(oos);
			oos.flush();
	        oos.close();
	        InputStream is = new ByteArrayInputStream(oos.toByteArray());
	        return new SessionSettings(is);
		} catch (Exception e) {
        	LOGGER.severe("FIXENGINECONFIG OVERRIDE PARAMETERS Unable to rewrite sessionSettings " + sessionSettings);
			e.printStackTrace();
		}
        return null;
	}
	
	/**
	 * Retrieves APPLICATION_STACK_NAME from the Java runtime's OS ENV variables
	 * SSM paths are expected to look like: /fixengine/<APPLICATION_STACK_NAME>/<token name>
	 * @return
	 */
    private String getSsmParameterPath() throws ConfigError {
	    LOGGER.info("FIXENGINECONFIG **********GET SSM PARAMETER PATH starting");
    	if(System.getProperty("os.name").contains("Windows")) {
    		LOGGER.info("FIXENGINECONFIG GET SSM PARAMETER PATH returning dummy value because we're running on Windows not Unix");
    		return "/fixengine/fake-stack-name";
    	}	    
		String stackNameEnvVar = "APPLICATION_STACK_NAME";
	    String stackName = System.getenv(stackNameEnvVar);
	    LOGGER.info("FIXENGINECONFIG GET SSM PARAMETER PATH got stack name env var  : [" + stackNameEnvVar + "] value [" + stackName + "]");
	    if(stackName == null) {
	        String message = "FIXENGINECONFIG GET SSM PARAMETER unable to find System Environment Variable (that should contain the CloudFormation stack name that created all SSM parameters) called: " + stackNameEnvVar;
	    	LOGGER.severe(message);
	        throw new ConfigError(message);
	    }
	    String path = "/fixengine/" + stackName;
	    LOGGER.info("FIXENGINECONFIG GET SSM PARAMETER PATH got stack name env var  : [" + stackNameEnvVar + "] value [" + stackName + "]");
	    return path;
    }

    /**
     * Retrieves SSM parameter by name (using prefix returned by getSsmParameterPath()).
     * If run on Windows, returns dummy values hard coded at the start of the method instead of connecting to SSM.
     * SSM paths are expected to look like: /fixengine/<APPLICATION_STACK_NAME>/<parameterName>
     * <GLOBAL_ACCELERATOR_ENDPOINT_ARN> is a special case that is retrieved from form the local system environment variable GLOBAL_ACCELERATOR_ENDPOINT_ARN instead of SSM 
     * @param parameterName
     * @return
     */
    private String getSsmParameter(String parameterName) {
	    LOGGER.info("FIXENGINECONFIG **********GET SSM PARAMETER looking up parameterPath/parameterName: " + parameterPath + "/" + parameterName + " using SSM_CLIENT: " + SSM_CLIENT);
    	if(System.getProperty("os.name").contains("Windows")) {
    		LOGGER.info("FIXENGINECONFIG GET SSM PARAMETER PATH returning dummy value because we're running on Windows not Unix");
//    		return new HashMap<String, String>();
    		if("GLOBAL_ACCELERATOR_ENDPOINT_ARN".equals(parameterName)) {
    			return "arn:aws:elasticloadbalancing:us-east-1:XXXXXXXXXXXX:loadbalancer/net/FixEn-Prima-XXXXXXXXXXXX/XXXXXXXXXXXXXXXX";
    		}
//          update arn with account number- replace XXXXXXXXXXXX with numbers
    		HashMap<String, String> ret = new HashMap<String, String>();
    		ret.put("SenderCompID","client");
    		ret.put("ConnectionType","initiator");
    		ret.put("PrimaryMSKEndpoint","b-1.fixengineonaws-client.pupo46.c6.kafka.us-east-1.amazonaws.com");
    		ret.put("KafkaConnTLS","false");
    		ret.put("TargetCompID","server");
    		ret.put("KafkaPort","9092");
    		ret.put("DebugLogging","true");
    		ret.put("FIXServerPort","9877");
    		ret.put("FailoverMSKEndpoint","b-2.fixengineonaws-client.pupo46.c6.kafka.us-east-1.amazonaws.com");
    		ret.put("FIXServerDNSName","XXXXXXXXXXXXXXXXX.awsglobalaccelerator.com");
    		ret.put("ApplicationID","client");
    		ret.put("RDSClusterSecretArn","arn:aws:secretsmanager:us-east-1:XXXXXXXXXXXX:secret:RDSClusterAdminSecret-XXXXXXXXXXXX-XXXXXX");
    		ret.put("RDSClusterNonAdminSecretArn","arn:aws:secretsmanager:us-east-1:XXXXXXXXXXXX:secret:RDSClusterNonAdminSecret-XXXXXXXXXXXX-XXXXXX");
    		ret.put("GlobalAcceleratorEndpointGroupArn", "arn:aws:elasticloadbalancing:us-east-1:XXXXXXXXXXXX:loadbalancer/net/FixEn-Failo-XXXXXXXXXXXX/XXXXXXXXXXXXXXXX");
    		return(ret.get(parameterName));
    	}

    	if("GLOBAL_ACCELERATOR_ENDPOINT_ARN".equals(parameterName)) {
		    String GAEndpointArn = System.getenv(parameterName);
		    LOGGER.fine("FIXENGINECONFIG GET SSM PARAMETER PATH got GA endpoint env var  : [" + parameterName + "] value [" + GAEndpointArn + "]");
		    if(GAEndpointArn == null) {
		        LOGGER.severe("FIXENGINECONFIG GET SSM PARAMETER unable to find System Environment Variable (that should contain the CloudFormation stack name that created all SSM parameters) called: " + parameterName);
		    }
		    return GAEndpointArn;
    	}
    	
    	if(SSM_CLIENT == null) {
    		LOGGER.info("FIXENGINECONFIG SSM_CLIENT is null, so creating it now...");
    		SSM_CLIENT = AWSSimpleSystemsManagementClientBuilder.standard().build();
    		if(SSM_CLIENT == null) {
    			LOGGER.severe("FIXENGINECONFIG GET SSM PARAMETER unable to create an AWSSimpleSystemsManagementClientBuilder! Check IAM privileges to see if your process has enough access to read params!");
    		}
    	}
        String key = parameterPath + "/" + parameterName;
        try {
            GetParameterRequest parametersRequest = new GetParameterRequest().withName(key).withWithDecryption(false);
            GetParameterResult parameterResult = SSM_CLIENT.getParameter(parametersRequest);
            String value = parameterResult.getParameter().getValue();
            LOGGER.fine("FIXENGINECONFIG GET SSM PARAMETER got key : [" + key + "] value [" + value + "]");
            return value;
        } catch (Exception e) {
            LOGGER.fine("FIXENGINECONFIG GET SSM PARAMETER unable to get key : [" + key + "] : " + e);
            throw e;
        }
    }

    /**
     * Enriches sessionSettings object with additional JDBC attributes retrieved from SSM Secrets Manager using the provided ARN (fully qualified, e.g.: "arn:aws:secretsmanager:us-east-1:XXXXXXXXXXXX:secret:RDSClusterAdminSecret-vdIe7YLT6JM2-aw18Xq")
     * @param secretArn
     */
    public void addSqlDbConnectionCoordinatesToSettings(String secretArn) {        
        LOGGER.info("FIXENGINECONFIG *********************GET SQL DB CONNECTION starting, using ARN: " + secretArn);
//      AWSSecretsManager client  = System.getProperty("os.name").contains("Windows") ? AWSSecretsManagerClientBuilder.standard().withRegion(Regions.US_EAST_1).build() : AWSSecretsManagerClientBuilder.standard().build();
        AWSSecretsManager client  = AWSSecretsManagerClientBuilder.standard().build();
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretArn);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (Exception e) {
            LOGGER.severe("FIXENGINECONFIG ****GET DB COORDINATES: EXCEPTION with secretArn [" + secretArn + "]: " + e);
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
            LOGGER.severe("FIXENGINECONFIG GET DB PARAMETERS: ERROR: unable to parse JSON: " + secret + " : " + e);
            e.printStackTrace();
        }
    }

    /**
     * Searches SessionSettings for a setting with the given name and returns the sessionProperty tuple containing the first instance it finds (regardless of what section it's found in). 
     * @param settingName
     * @return
     */    
    private Map.Entry<String, String> findSessionSetting(String settingName) {
    	return findSessionSetting(settingName, true, sessionSettings);
    }
    
    /**
     * Searches SessionSettings for a setting with the given name (or substring if the "exact" parameter is false) and returns the sessionProperty tuple containing the first instance it finds (regardless of what section it's found in). 
     * @param settingName
     * @return
     */
    private Map.Entry<String, String> findSessionSetting(String settingName, boolean exact, SessionSettings settings) {
    	if(settingName == null) {
    		return null;
    	}
    	ArrayList<Properties> allProperties = new ArrayList<Properties>();
    	allProperties.add(sessionSettings.getDefaultProperties());
        for (Iterator<SessionID> sections = sessionSettings.sectionIterator(); sections.hasNext(); ) {
            SessionID section = sections.next();
	        try {
            	allProperties.add(sessionSettings.getSessionProperties(section));
	        } catch (ConfigError e) {
	        	LOGGER.severe("FIXENGINECONFIG GET SESSION SETTING Unable to process section [" + section + "]");
	        	e.printStackTrace();
	        }
        }

        for (int i = 0; i < allProperties.size(); i++) {
        	Properties sessionProperties = allProperties.get(i);
//            LOGGER.info("FIXENGINECONFIG PROPS: " + sessionProperties);
            for (Iterator sessionPropertyIter = sessionProperties.entrySet().iterator(); sessionPropertyIter.hasNext(); ) {
            	Map.Entry<String, String> sessionProperty = (Map.Entry<String, String>)sessionPropertyIter.next();
            	String propertyKey = (String)sessionProperty.getKey();
            	String propertyVal = (String)sessionProperty.getValue();
            	if ((exact && settingName.equals(propertyKey)) || (!exact && propertyVal.contains(settingName))) {
            		return sessionProperty;
            	}
            }
        }
        return null;    
    }

    /**
     * Searches SessionSettings for a setting with the given name and returns the value of the first instance it finds (regardless of what section it's found in). 
     * @param settingName
     * @return
     */
    public String getSessionSetting(String settingName) {
    	LOGGER.fine("FIXENGINECONFIG ****GET SESSION SETTING <" + settingName + ">");
    	Map.Entry<String, String> sessionProperty = findSessionSetting(settingName);
    	if(sessionProperty == null) {
        	LOGGER.fine("FIXENGINECONFIG SET SESSION SETTING <" + settingName + "> NOT FOUND");
    		return null; 
    	} else {
    		String val = (String)sessionProperty.getValue();
        	LOGGER.fine("FIXENGINECONFIG SET SESSION SETTING <" + settingName + "> FOUND! RETURNING VALUE <" + val + ">");
    		return val;
    	}
    }

    /**
     * Searches SessionSettings for a setting with the given name and sets the value of the first instance it finds (regardless of what section it's found in) to the supplied new value. 
     * @param settingName
     * @return boolean true on success, false if the setting by that name was not found 
     */
    public boolean setSessionSetting(String settingName, String newValue, SessionSettings settings) {
    	LOGGER.fine("FIXENGINECONFIG ****SET SESSION SETTING <" + settingName + "> TO <" + newValue + ">");
    	Map.Entry<String, String> sessionProperty = findSessionSetting(settingName, true, settings);
    	if(sessionProperty == null) {
        	LOGGER.fine("FIXENGINECONFIG SET SESSION SETTING <" + settingName + "> NOT FOUND");
    		return false;
    	} else {
        	LOGGER.fine("FIXENGINECONFIG SET SESSION SETTING <" + settingName + "> FOUND! SETTING TO <" + newValue + ">");
    		sessionProperty.setValue(newValue);
    		return true;
    	}
    }

    /**
     * Searches SessionSettings for values with tokens (formatted as <TOKEN_NAME>) and replaces them with parameters with the same name 
     * from SSM parameter store (using the prefix generated by getSsmParameterPath())
     */
    private void overrideConfigFromSsmParameters() {
    	LOGGER.info("FIXENGINECONFIG ****OVERRIDE CONFIG FROM SSM PARAMETERS starting");
    	Map.Entry<String, String> sessionProperty = findSessionSetting("<", false, sessionSettings);
    	while(sessionProperty != null) {
    		String propertyVal = (String)sessionProperty.getValue();
	    	Matcher m = Pattern.compile("<(.+?)>").matcher(propertyVal);
	    	while (m.find()) {
	    		String token = m.group();
	    		String ssmParameterName = token.replace("<", "").replace(">", "");
	    		LOGGER.info("FIXENGINECONFIG OVERRIDE CONFIG FROM SSM PARAMETERS in [" + propertyVal + "] getting ready to replace [" + token + "]");
	        	String ssmParamVal = getSsmParameter(ssmParameterName);
	        	String newValue = propertyVal.replace(token,ssmParamVal);
	        	LOGGER.info("FIXENGINECONFIG OVERRIDE CONFIG FROM SSM PARAMETERS in [" + propertyVal + "] replaced [" + token + "] with [" + ssmParamVal + "] to get [" + newValue + "]");
	        	propertyVal = newValue;
	        	sessionProperty.setValue(newValue);            		
	    	}
	    	sessionProperty = findSessionSetting("<", false, sessionSettings);
	    }
    	
        // This is a wourkaround for a bug in sessionSettings where simply setting properties in the "session" section 
        // gets reflected in the underlying Hashtables and toString() but not in the date it exposes to the Fix SocketAcceptor/SocketInitiator constructor
        // resulting in token strings like "<TargetCompID>" being used by the resulting FIX engine instead of the overridden values
        sessionSettings = cloneSessionSettings(sessionSettings);
        
//        LOGGER.info("FIXENGINECONFIG INITIALIZE PARAMETERS: rewrote new SessionSettings after overriding: " + sessionSettings);
    }

    /**
     * Use the file at the specified location to create a SessionSettings object, then call overrideConfigFromSsmParameters() to replace any tokens with values from SSM Param Store
     * @param configfile
     * @return SessionSettings
     * @throws ConfigError
     */
    private SessionSettings initializeParameters(String configfile) throws ConfigError {
    	LOGGER.info("FIXENGINECONFIG ****INITIALIZE PARAMETERS starting");
    	try {
            sessionSettings = new SessionSettings(configfile);
        } catch (ConfigError e) {
            LOGGER.info("FIXENGINECONFIG INITIALIZE PARAMETERS: Unable to create new SessionSettings from config file " + configfile);
            e.printStackTrace();
            throw e;
        }

        LOGGER.setLevel(Level.FINE);

        overrideConfigFromSsmParameters();

        if ("<DebugLogging>".equals(getSessionSetting("DebugLogging"))) {
        	sessionSettings.setString("DebugLogging","false");
        }        
    	LOGGER.info("FIXENGINECONFIG INITIALIZE PARAMETERS finished overriding params and got: " + sessionSettings);    	
//        checkConfigHealthy(configfile);
    	return sessionSettings;
    }

    /**
     * Ensure that all the settings listed in the required<Client|Server>ConfigFields variable for the current ConnectionType (initiator/accpetor) are present and no longer contain any tokens
     * @return
     */
    private String validateSessionSettings() {
    	String[] requiredFields = "initiator".equals(getSessionSetting("ConnectionType")) ? requiredClientConfigFields : requiredServerConfigFields;
    	String errors = "none";
    	for(int i=0; i<requiredFields.length; i++) {
    		String name = requiredFields[i];
    		String val = getSessionSetting(name);
    		if(val==null) {
    			errors += "UNABLE TO FIND REQUIRED SETTING <" + name +"> ";
    		} else if (val.contains("<") && val.contains(">")) {
    			errors += "REQUIRED SETTING <" + name +"> STILL CONTAINS TOKENS: <" + val +">";    			
    		}
    	}
    	return errors;
    }


}