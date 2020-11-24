package com.amazonaws.fixengineonaws;

import java.io.FileNotFoundException;

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
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.AvgPx;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.HandlInst;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.SenderCompID;
import quickfix.field.SenderSubID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.NewOrderSingle;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Arrays;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import java.util.logging.Logger;
import java.util.logging.LogManager;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;

import java.io.InputStream;

/**
 * Fix engine tester and dummy order generator demo class
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

public class TestClient {

   	private static boolean I_AM_TEST_CLIENT = false;
   	private static Logger LOGGER = Logger.getLogger(FixEngine.class.getName());
   	
   	private static KafkaProducer<String, String> KAFKA_PRODUCER; 
	private static String KAFKA_INBOUND_TOPIC_NAME;
	private static String KAFKA_BROKER_STRING;

	private static KafkaConsumer<String, Object> KAFKA_CONSUMER; 
	private static String KAFKA_OUTBOUND_TOPIC_NAME;
	private static String KAFKA_OUTBOUND_CONSUMER_GORUP_ID = "test-client-app";
	
	private static int NO_OF_MESSAGES = 1;
	private static int WAIT_BETWEEN_MESSAGES = 1000;
   	
   	public static NewOrderSingle generateOrder(int id) {
		String orderIdStr = "ORDER_ID_" + System.currentTimeMillis();
		String accountIdStr = "TEST_SENDER_COMP_ID";
		String senderSubIdStr = "TEST_SENDER_SUB_ID";
		String targetIdStr = "TEST_SENDER_COMP_ID";
		String symbolStr = "MSFT";
		char side = Side.BUY;
		char orderType = OrdType.MARKET;
		char timeInForce = TimeInForce.DAY;
		NewOrderSingle newOrder = new NewOrderSingle(new ClOrdID(orderIdStr), new HandlInst('1'), new Symbol(symbolStr), new Side(side), new TransactTime(), new OrdType(orderType));
		quickfix.Message.Header header = newOrder.getHeader();
		header.setField(new SenderCompID(accountIdStr));
		header.setField(new SenderSubID(senderSubIdStr));
		header.setField(new TargetCompID(targetIdStr));
//		newOrder.setChar(59, new TimeInForce(timeInForce).getValue());
		newOrder.setChar(59, timeInForce);
		int quantitiyInt = 300;
		newOrder.setInt(38, quantitiyInt);
		double priceDouble = 123.45;
		newOrder.setDouble(44, priceDouble);
		return newOrder;
	}
    
	public static ExecutionReport generateExecution(int id) {
		String orderIdStr = "ORDER_ID_" + System.currentTimeMillis();
		String execIdStr = "EXEC_ID_" + 1;
		String symbolStr = "MSFT";
		char side = Side.BUY;
		char orderType = OrdType.MARKET;
		char timeInForce = TimeInForce.DAY;
		ExecutionReport newExec = new ExecutionReport(new OrderID(orderIdStr), new ExecID(execIdStr), new ExecTransType(ExecTransType.NEW), new ExecType(ExecType.PARTIAL_FILL), 
				new OrdStatus(OrdStatus.PARTIALLY_FILLED), new Symbol(symbolStr), new Side(side), new LeavesQty(250), new CumQty(50), new AvgPx(123.34));
		return newExec;
	}
    
    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {

        LOGGER.setLevel(Level.INFO);
        //LOGGER.setLevel(Level.WARNING);
        
        String configfile = "config/server.cfg";
    	if(args.length > 0) {
    		configfile = args[0];
    	} 
		LOGGER.info("***MAIN STARTING WITH CONFIG FILE: " + configfile);

        // setup kafka producer and consumefr
        setupKafka(configfile);
        
        if (I_AM_TEST_CLIENT) {
            LOGGER.info(" NO_OF_MESSAGES : " + NO_OF_MESSAGES + "WAIT_BETWEEN_MESSAGES: " + WAIT_BETWEEN_MESSAGES);
            
            
            //for(int orderId=1;orderId<1000;orderId++) {
            for(int orderId=1;orderId<NO_OF_MESSAGES;orderId++) {
                String ordStr = null;
                
                Thread.sleep(WAIT_BETWEEN_MESSAGES);
                // [CLIENT LAMBDA] CREATE NEW ORDER:
               	LOGGER.info("***main() I_AM_TEST_CLIENT " + I_AM_TEST_CLIENT + " orderId # " + orderId);
                //if (I_AM_TEST_CLIENT) {
                    
                    NewOrderSingle ord = generateOrder(orderId);
                    ordStr = ord.toString();
                
            		 LOGGER.info("*********** ORDER Client to Server *****************************************************************************************");
            		 LOGGER.info("************GENERATED ORDER FIX STRING: " + ordStr);
            		 //LOGGER.info("****************************************************************************************************");
            		
            		// Pick a sentence at random
                    //String sentence = "testing from fix server";
                    // Send the sentence to the test topic
                    try
                    {
                        KAFKA_PRODUCER.send(new ProducerRecord<String, String>(KAFKA_OUTBOUND_TOPIC_NAME, ordStr)).get();
                        //producer.send(new ProducerRecord<String, String>(topicName, message.toString())).get();
                    } catch (Exception ex) {
                        LOGGER.severe(ex.getMessage());
                    }
    
                //} // I_AM_TEST_CLIENT
            }
        }
        
        processKafkaMsgs();
    }
    
    
    public static void setupKafka(String configfile) {
        
        getKafkaProperties(configfile);
        getKafkaProducer();
        getKafkaConsumer();
        
    }
    
    public static void getKafkaProducer() {

		Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BROKER_STRING);
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        KAFKA_PRODUCER = new KafkaProducer<>(properties);
    }
    
     private static void getKafkaConsumer() {
        LOGGER.fine("****getKafkaConsumer START*****");

	    // Configure the consumer
        Properties properties = new Properties();
        // Point it to the brokers
        properties.setProperty("bootstrap.servers", KAFKA_BROKER_STRING);
        // Set the consumer group (all consumers must belong to a group).
        properties.setProperty("group.id", KAFKA_OUTBOUND_CONSUMER_GORUP_ID);
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
        KAFKA_CONSUMER.subscribe(Arrays.asList(KAFKA_INBOUND_TOPIC_NAME));
    }
    
    private static void processKafkaMsgs() {
        LOGGER.fine("****processInboundKafkaMsgs: Start ");
        // Loop until ctrl + c
        // Harman: create a thread
        int count = 0;
        
        while(true) {

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
                    LOGGER.info("*********** ORDER RCVD from Client *****************************************************************************************");
                    LOGGER.info("*** processKafkaMsgs() ordStr : " + ordStr);
                    
                    // LOGGER.info("processInboundKafkaMsgs() I_AM_TEST_CLIENT : " + I_AM_TEST_CLIENT);
                
                    if (!I_AM_TEST_CLIENT) {
                        // send the execution report back to client Fix Engine
                        ExecutionReport newExec = generateExecution(count);
                        ordStr = newExec.toString();
                        
                        try {
                            KAFKA_PRODUCER.send(new ProducerRecord<String, String>(KAFKA_OUTBOUND_TOPIC_NAME, ordStr)).get();
                            LOGGER.info("*********** Generated ExecutionReport from Server to Client ********************************************************");
                            LOGGER.info("ExecutionReport : " + ordStr);
                            //producer.send(new ProducerRecord<String, String>(topicName, message.toString())).get();
                        } catch (Exception ex) {
                             LOGGER.severe(" Exception : " + ex.getMessage());
                        }
                    }
                            
                } // for end
        	} // if (records.count() == 0)
        	
        } //while loop
        
    }

    
    private static Properties getKafkaProperties(String configfile) {
        LOGGER.fine("****GETTING KAFKA PROPERTIES 11: " + configfile);
         
        
        Properties kafkaprop = new Properties();  
            
        try {
            // FileReader reader = new FileReader(currentDir + "config/kafka-server.properties");  
            // kafkaprop.load(reader);
        //     InputStream is = TestClient.class.getResourceAsStream(configfile);
        // 	kafkaprop.load(is);
        	
        	
        	FileReader reader = new FileReader(configfile);  
        	kafkaprop.load(reader);
       
        	
        	KAFKA_OUTBOUND_TOPIC_NAME = kafkaprop.getProperty("KafkaOutboundTopicName");;
        	KAFKA_INBOUND_TOPIC_NAME = kafkaprop.getProperty("KafkaInboundTopicName");
	        KAFKA_BROKER_STRING = kafkaprop.getProperty("KafkaBootstrapBrokerString");
	        
	        NO_OF_MESSAGES = Integer.parseInt(kafkaprop.getProperty("NoOfMessages"));
	        WAIT_BETWEEN_MESSAGES = Integer.parseInt(kafkaprop.getProperty("WaitBetweenMessages")) * 1000;
		
            
            LOGGER.info(" KAFKA_BROKER_STRING: " + KAFKA_BROKER_STRING);
        	I_AM_TEST_CLIENT = kafkaprop.getProperty("ConnectionType").equals("initiator");
        	LOGGER.info(" I_AM_TEST_CLIENT: " + I_AM_TEST_CLIENT);
        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			 LOGGER.severe(e.getMessage());
		}
        return kafkaprop;
    }
}