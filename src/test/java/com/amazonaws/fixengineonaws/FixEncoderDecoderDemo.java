package com.amazonaws.fixengineonaws;

import java.io.InputStream;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.DefaultMessageFactory;
import quickfix.InvalidMessage;
import quickfix.MessageFactory;
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
import quickfix.fix42.NewOrderSingle; //update fix version to the version the application will be using

/**
 * Fix encoding and decoding demo class
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

public class FixEncoderDecoderDemo {
	
   	public static NewOrderSingle generateOrder() {
		String orderIdStr = "ORDER_ID_" + System.currentTimeMillis();
		String accountIdStr = "TEST_SENDER_COMP_ID";
		String senderSubIdStr = "TEST_SENDER_SUB_ID";
		String targetIdStr = "TEST_SENDER_COMP_ID";
		String symbolStr = "MSFT";
		char side = Side.BUY;
		char orderType = OrdType.MARKET;
		char timeInForce = TimeInForce.DAY;
		NewOrderSingle newOrder = new NewOrderSingle(new ClOrdID(orderIdStr), new HandlInst('1'), new Symbol(symbolStr), new Side(side), new TransactTime(), new OrdType(orderType));
		/*
			Choose NewOrderSingle constructor based on FIX version
			4.2 - NewOrderSingle(ClOrdID clOrdID, HandlInst handlInst, Symbol symbol, Side side, TransactTime transactTime, OrdType ordType)
			4.3 - NewOrderSingle(ClOrdID clOrdID, HandlInst handlInst, Side side, TransactTime transactTime, OrdType ordType) 
			4.4 - NewOrderSingle(ClOrdID clOrdID, Side side, TransactTime transactTime, OrdType ordType) 
			5.0 - NewOrderSingle(ClOrdID clOrdID, Side side, TransactTime transactTime, OrdType ordType) 
		 */
		
		
		quickfix.Message.Header header = newOrder.getHeader();
		header.setField(new SenderCompID(accountIdStr));
		header.setField(new SenderSubID(senderSubIdStr));
		header.setField(new TargetCompID(targetIdStr));
		newOrder.setChar(59, timeInForce);
		int quantitiyInt = 300;
		newOrder.setInt(38, quantitiyInt);
		double priceDouble = 123.45;
		newOrder.setDouble(44, priceDouble);
		return newOrder;
	}

   	public static String orderToFixString(NewOrderSingle order) {
        return order.toString();
   	}
   	
   	public static NewOrderSingle fixStringToOrder(String orderStr) {
    	MessageFactory messageFactory = new DefaultMessageFactory();
    	String fixFormatXml = "FIX42.xml"; //update to desired FIX version
    	//Can update InputStream object name to desired FIX version
    	InputStream fix42Input = FixEngine.class.getClassLoader().getResourceAsStream(fixFormatXml); // This pulls the XML file from quickfix-messages-all-2.2.0.jar
    	DataDictionary dataDictionary;
		try {
			dataDictionary = new DataDictionary(fix42Input); //if changing name, update here as well
		} catch (ConfigError e) {
			System.out.println("ERROR: Unable to find FIX Format XML file (in quickfix-messages-all jar file): " + fixFormatXml);
			System.out.println(e);
			e.printStackTrace();
			return null;
		}
		
        try {
        	NewOrderSingle parsedOrd = (NewOrderSingle)quickfix.MessageUtils.parse(messageFactory, dataDictionary, orderStr);
        	return parsedOrd;
		} catch (InvalidMessage e) {
			System.out.println("ERROR: Invalid FIX message: " + orderStr);
			System.out.println(e);
			e.printStackTrace();
			return null;
		}
   	}
   	
    public static void main(String[] args) {
        NewOrderSingle order = generateOrder();
        String fixEncodedOrderString = orderToFixString(order);
		System.out.println("*************** GENERATED ORDER FIX STRING   : " + fixEncodedOrderString );
		NewOrderSingle decodedOrder = fixStringToOrder(fixEncodedOrderString);
		System.out.println("*************** DECODED ORDER FROM FIX STRING: " + decodedOrder );
    }
}