package com.amazonaws.fixengineonaws;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import quickfix.ConfigError;
import quickfix.SessionNotFound;
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
import quickfix.fix42.ExecutionReport; //update fix version to the version the application will be using
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.text.SimpleDateFormat;


public class TestServer {
   	private static Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
	
	private static SimpleDateFormat DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static Date LAST_NO_MESSAGE_LOG = new Date();
	private static long totalAppToFixProcessngTime = 0;
	private static long totalAppToFixProcessedMessages = 0;
	private static long totalFixToAppProcessngTime = 0;
	private static long totalFixToAppProcessedMessages = 0;
	
	private static Jedis jedis;
	
	private static ExecutionReport generateExecution(int id) {
        // String orderIdStr = "ORDER_ID_" + System.currentTimeMillis();
        String orderIdStr = "ORDER_ID_" + 1000000 + id;
		String execIdStr = "EXEC_ID_" + 1;
		String symbolStr = "AMZN";
		char side = Side.BUY;
		ExecutionReport newExec = new ExecutionReport(new OrderID(orderIdStr), new ExecID(execIdStr), new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.PARTIAL_FILL), 
				new OrdStatus(OrdStatus.PARTIALLY_FILLED), new Symbol(symbolStr), new Side(side), new LeavesQty(250), new CumQty(50), new AvgPx(123.34));
				 // Choose ExecutionReport constructor based on FIX version
			     // 4.2 - ExecutionReport(OrderID orderID, ExecID execID, ExecTransType execTransType, ExecType execType, OrdStatus ordStatus, Symbol symbol, Side side, LeavesQty leavesQty, CumQty cumQty, AvgPx avgPx) 
                 // 4.3 - ExecutionReport(OrderID orderID, ExecID execID, ExecType execType, OrdStatus ordStatus, Side side, LeavesQty leavesQty, CumQty cumQty, AvgPx avgPx) 
                 // 4.4 - ExecutionReport(OrderID orderID, ExecID execID, ExecType execType, OrdStatus ordStatus, Side side, LeavesQty leavesQty, CumQty cumQty, AvgPx avgPx) 
                 // 5.0 - ExecutionReport(OrderID orderID, ExecID execID, ExecType execType, OrdStatus ordStatus, Side side, LeavesQty leavesQty, CumQty cumQty) 
		return newExec;
	}
    
    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {

        LOGGER.setLevel(Level.INFO);
        //LOGGER.setLevel(Level.WARNING);
        
    	String configfile = "config/test-server.config";
        if(args.length > 0) {
            configfile = args[0];
        }
        System.out.println("***MAIN STARTING WITH CONFIG FILE: " + configfile);
        
        String MemoryDBHost              = RedisSetting.DEFAULT_REDIS_HOST;
        String MemoryDBPort              = RedisSetting.DEFAULT_REDIS_PORT;
        String MemoryDBAppToFixQueueName = RedisSetting.DEFAULT_REDIS_APP_TO_FIX_QUEUE_NAME;
        String MemoryDBFixToAppQueueName = RedisSetting.DEFAULT_REDIS_FIX_TO_APP_QUEUE_NAME;
        int    WaitBetweenMessages       = 0;
        boolean SendExecReport			 = true;
        
        FileReader reader = new FileReader(configfile);  
        Properties config = new Properties();
        try {
			config.load(reader);
		} catch (IOException e) {
	        LOGGER.severe("ERROR LOADING CONFIG FILE " + configfile);
			e.printStackTrace();
		}
        MemoryDBHost              = config.getProperty("MemoryDBHost");
        MemoryDBPort              = config.getProperty("MemoryDBPort");
        MemoryDBAppToFixQueueName = config.getProperty("MemoryDBAppToFixQueueName");
        MemoryDBFixToAppQueueName = config.getProperty("MemoryDBFixToAppQueueName");
        WaitBetweenMessages       = Integer.parseInt(config.getProperty("WaitBetweenMessages"));
        SendExecReport			  = "true".equals(config.getProperty("SendExecReport"));
		
		jedis = new Jedis(MemoryDBHost, Integer.parseInt(MemoryDBPort));
                
		pollForOrders(WaitBetweenMessages, MemoryDBFixToAppQueueName, MemoryDBAppToFixQueueName, SendExecReport);
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

    private static void pollForOrders(int waitBetweenMessages, String inboundQueueName, String outboundQueueName, boolean replyWithExecution) {
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
                	LOGGER.info("nothing to read from MemoryDB");
        			LAST_NO_MESSAGE_LOG = new Date();
        		}
                logStats(false, count, firstOrderRcvdTime, lastOrderRcvdTime);
                count=0;
            } else {                
                // if this is the first message in the batch, restart the TPS timer 
                if (count == 0) firstOrderRcvdTime = new Date();
                count += 1;
                LOGGER.fine( count + ": " + appToFixMessage);
                LOGGER.info("*********** ORDER RCVD from Client or Server *****************************************************************************************");
                LOGGER.info("*** ordStr : " + appToFixMessage);

                if (replyWithExecution) { // send the execution report back to client Fix Engine
                    ExecutionReport newExec = generateExecution(count);
                    String ordStr = newExec.toString();                    
                    try {
                    	jedis.rpush(outboundQueueName, ordStr);
                        LOGGER.info("*********** Generated ExecutionReport from Server to Client ********************************************************");
                        LOGGER.info("ExecutionReport : " + ordStr);
                    } catch (Exception ex) {
                         LOGGER.severe(" Exception : " + ex.getMessage());
                    }
                }
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