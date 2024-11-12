package com.amazonaws.fixengineonaws;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import quickfix.ConfigError;
import quickfix.SessionNotFound;
import quickfix.field.ClOrdID;
import quickfix.field.HandlInst;
import quickfix.field.OrdType;
import quickfix.field.SenderCompID;
import quickfix.field.SenderSubID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix42.NewOrderSingle;  //update fix version to the version the application will be using
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.text.SimpleDateFormat;

public class TestClient {
   	private static Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
	
	private static SimpleDateFormat DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static Date LAST_NO_MESSAGE_LOG = new Date();
	private static long totalAppToFixProcessngTime = 0;
	private static long totalAppToFixProcessedMessages = 0;
	private static long totalFixToAppProcessngTime = 0;
	private static long totalFixToAppProcessedMessages = 0;

	private static Jedis jedis;
   	
   	private static NewOrderSingle generateOrder(int id) {
		//String orderIdStr = "ORDER_ID_" + System.currentTimeMillis();
		String orderIdStr = "ORDER_ID_" + (1000000 + id);
		String accountIdStr = "TEST_SENDER_COMP_ID";
		String senderSubIdStr = "TEST_SENDER_SUB_ID";
		String targetIdStr = "TEST_SENDER_COMP_ID";
		String symbolStr = "AMZN";
		char side = Side.BUY;
		char orderType = OrdType.MARKET;
		char timeInForce = TimeInForce.DAY;
		NewOrderSingle newOrder = new NewOrderSingle(new ClOrdID(orderIdStr), new HandlInst('1'), new Symbol(symbolStr), new Side(side), new TransactTime(), new OrdType(orderType));
			/*
			Choose message constructor based on FIX version
			4.2 - NewOrderSingle(ClOrdID clOrdID, HandlInst handlInst, Symbol symbol, Side side, TransactTime transactTime, OrdType ordType)
			4.3 - NewOrderSingle(ClOrdID clOrdID, HandlInst handlInst, Side side, TransactTime transactTime, OrdType ordType) 
			4.4 - NewOrderSingle(ClOrdID clOrdID, Side side, TransactTime transactTime, OrdType ordType) 
			5.0 - NewOrderSingle(ClOrdID clOrdID, Side side, TransactTime transactTime, OrdType ordType) 
		 */
		quickfix.Message.Header header = newOrder.getHeader();
		header.setField(new SenderCompID(accountIdStr));
		header.setField(new SenderSubID(senderSubIdStr));
		header.setField(new TargetCompID(targetIdStr));
        // newOrder.setChar(59, new TimeInForce(timeInForce).getValue());
		newOrder.setChar(59, timeInForce);
		int quantitiyInt = 300;
		newOrder.setInt(38, quantitiyInt);
		double priceDouble = 123.45;
		newOrder.setDouble(44, priceDouble);
		return newOrder;
	}
    
    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {
        LOGGER.setLevel(Level.INFO);
        //LOGGER.setLevel(Level.WARNING);

    	String configfile = "config/test-client.config";
    	if(args.length > 0) {
            configfile = args[0];
        }
        System.out.println("***MAIN STARTING WITH CONFIG FILE: " + configfile);

        String memoryDBHost              = RedisSetting.DEFAULT_REDIS_HOST;
        String memoryDBPort              = RedisSetting.DEFAULT_REDIS_PORT;
        String memoryDBAppToFixQueueName = RedisSetting.DEFAULT_REDIS_APP_TO_FIX_QUEUE_NAME;
        String memoryDBFixToAppQueueName = RedisSetting.DEFAULT_REDIS_FIX_TO_APP_QUEUE_NAME;
        int    noOfMessages              = 10*1000;
        int    waitBetweenMessages       = 0;
        
        FileReader reader = new FileReader(configfile);  
        Properties config = new Properties(); 
        try {
			config.load(reader);
		} catch (IOException e) {
	        LOGGER.severe("ERROR LOADING CONFIG FILE " + configfile);
			e.printStackTrace();
		}
        memoryDBHost              = config.getProperty("MemoryDBHost");
        memoryDBPort              = config.getProperty("MemoryDBPort");
        memoryDBAppToFixQueueName = config.getProperty("MemoryDBAppToFixQueueName");
        memoryDBFixToAppQueueName = config.getProperty("MemoryDBFixToAppQueueName");
        noOfMessages              = Integer.parseInt(config.getProperty("NoOfMessages"));
        waitBetweenMessages       = Integer.parseInt(config.getProperty("WaitBetweenMessages"));
		
        System.out.println("***MAIN STARTING WITH memoryDBHost: " + memoryDBHost + "; memoryDBPort:" + memoryDBPort + "; memoryDBAppToFixQueueName:" + memoryDBAppToFixQueueName + "; memoryDBFixToAppQueueName:" + memoryDBFixToAppQueueName + "; noOfMessages:" + noOfMessages + "; waitBetweenMessages:" + waitBetweenMessages);
        
        jedis = new Jedis(memoryDBHost, Integer.parseInt(memoryDBPort));
        
        generateOrders(noOfMessages, waitBetweenMessages, memoryDBAppToFixQueueName);
        
        pollForReplies(waitBetweenMessages, memoryDBFixToAppQueueName);

    }

    private static void logStats(boolean appToFix, int count, Date firstOrderTime, Date lastOrderTime) {
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

    
    private static void generateOrders(int numberOfMessages, int waitBetweenMessages, String queueName) throws InterruptedException{
        LOGGER.info(" NO_OF_MESSAGES : " + numberOfMessages + "  WAIT_BETWEEN_MESSAGES: " + waitBetweenMessages);

    	Date firstOrderGeneratedTime = new Date();
    	Date lastOrderGeneratedTime = null;

        for(int orderId=1;orderId<=numberOfMessages;orderId++) {
            String ordStr = null;
            
            Thread.sleep(waitBetweenMessages);
           	LOGGER.info("***main() I_AM_TEST_CLIENT orderId # " + orderId);
            
            NewOrderSingle ord = generateOrder(orderId);
            ordStr = ord.toString();
//    		LOGGER.info("*********** ORDER Client to Server *****************************************************************************************");
    		LOGGER.fine("************GENERATED ORDER FIX STRING: " + ordStr);
    		
            try {
            	jedis.rpush(queueName, ordStr);
        		LOGGER.info("************PUSHED ORDER FIX STRING: " + ordStr + " TO QUEUE: " + queueName);
            } catch (Exception ex) {
                LOGGER.severe(ex.getMessage());
            }
    
            lastOrderGeneratedTime = new Date();
            logStats(false, orderId, firstOrderGeneratedTime, lastOrderGeneratedTime);
        }
    }
    
    private static void pollForReplies(int waitBetweenMessages, String inboundQueueName) {
        LOGGER.fine("****pollForOrders: Start ");
    	Date firstOrderRcvdTime = null;
    	Date lastOrderRcvdTime = null;

        // Loop until ctrl + c
        int count = 0;        
        while(true) {
            // Poll for records
        	String appToFixMessage = jedis.lpop(inboundQueueName);
            //LOGGER.fine(" After polling consumer records.count() : " + records.count());
            // Did we get any?
            if (appToFixMessage == null) {
                // timeout/nothing to read! Log stats if we had processed some messages up until now!
        		if( (new Date().getTime() - LAST_NO_MESSAGE_LOG.getTime())/1000 > 10) {
                	LOGGER.info("nothing to read from MemoryDB QUEUE: " + inboundQueueName);
        			LAST_NO_MESSAGE_LOG = new Date();
        		}
                logStats(true, count, firstOrderRcvdTime, lastOrderRcvdTime);
                count=0;
            } else {                
                // if this is the first message in the batch, restart the TPS timer 
                if (count == 0) firstOrderRcvdTime = new Date();
                count += 1;
                LOGGER.fine( count + ": " + appToFixMessage);
//                LOGGER.info("*********** ORDER RCVD from Client or Server *****************************************************************************************");
                LOGGER.info("*** RECEIVED ORDER STRING: " + appToFixMessage + " FROM QUEUE: " + inboundQueueName);

                lastOrderRcvdTime = new Date();
        	} // if (appToFixMessage == null)

            try {
            	Thread.sleep(waitBetweenMessages);
            } catch (InterruptedException e) {
            	LOGGER.severe(" InterruptedException : " + e.getMessage());
			}
        } //while loop       
    }
}