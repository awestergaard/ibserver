package com.awestergaard.ibserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.ib.client.ComboLeg;
import com.ib.client.Contract;
import com.ib.client.EClientSocket;
import com.ib.client.UnderComp;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


public class App {
	public static void main(String[] args) {
		try {
		    RabbitMQWrapper wrapper = new RabbitMQWrapper();
		    EClientSocket eClient = new EClientSocket(wrapper);
		    String host = "localhost";
		    int port = 7496;
		    int clientId = 0;
		    eClient.eConnect(host, port, clientId);
			
			ConnectionFactory factory = new ConnectionFactory();
	        factory.setHost("localhost");
	        Connection connection = factory.newConnection();
	        Channel channel = connection.createChannel();
	        MessagePack msgpack = new MessagePack();
	        msgpack.register(ComboLeg.class);
	        msgpack.register(UnderComp.class);
	        msgpack.register(Contract.class);
			channel.queueDeclare("requests", false, false, false, null);
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume("requests", true, consumer);
			
	    	while (true) {
	    		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	    		ByteArrayInputStream in = new ByteArrayInputStream(delivery.getBody());
	    		Unpacker unpacker = msgpack.createUnpacker(in);
	    		String methodName = unpacker.readString();
	    		System.out.println(" [x] Received '" + methodName + "'");
	    		Contract contract;
	    		switch(methodName) {
	    			case "reqMktData":
	    				int id = unpacker.readInt();
	    				contract = unpacker.read(Contract.class);
	    				String genericTickList = "";
	    				boolean snapshot = true;
	    				eClient.reqMktData(id, contract, genericTickList, snapshot);
	    				System.out.println("reqMktData");
	    				break;
	    			case "reqHistoricalData":
	    				int tickerId = unpacker.readInt();
	    				contract = unpacker.read(Contract.class);
	    				String endDateTime = unpacker.readString();
	    				String durationStr = unpacker.readString();
	    				String barSizeSetting = unpacker.readString();
	    				String whatToShow = unpacker.readString();
	    				int useRTH = unpacker.readInt();
	    				int formatDate = unpacker.readInt();
	    				eClient.reqHistoricalData(tickerId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate);
	    				System.out.println("reqHistoricalData");
	    				break;
	    			case "reqContractDetails":
	    				int reqId = unpacker.readInt();
    					contract = unpacker.read(Contract.class);
    					eClient.reqContractDetails(reqId, contract);
    					System.out.println("reqContractDetails");
    					break;
	    		}
	    	}
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}