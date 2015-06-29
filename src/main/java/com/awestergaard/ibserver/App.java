package com.awestergaard.ibserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

import com.ib.client.Contract;
import com.ib.client.EClientSocket;
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
//		    eClient.eConnect(host, port, clientId);
			
			ConnectionFactory factory = new ConnectionFactory();
	        factory.setHost("localhost");
	        Connection connection = factory.newConnection();
	        Channel channel = connection.createChannel();
	        MessagePack msgpack = new MessagePack();
			channel.queueDeclare("requests", false, false, false, null);
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume("requests", true, consumer);
			
	    	while (true) {
	    		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	    		ByteArrayInputStream in = new ByteArrayInputStream(delivery.getBody());
	    		Unpacker unpacker = msgpack.createUnpacker(in);
	    		String methodName = unpacker.readString();
	    		System.out.println(" [x] Received '" + methodName + "'");
	    		switch(methodName) {
	    			case "reqMktData":
	    				int id = unpacker.readInt();
	    				Contract contract = new Contract();
	    				contract.m_symbol = unpacker.readString();
	    				contract.m_exchange = unpacker.readString();
	    				contract.m_expiry = unpacker.readString();
	    				contract.m_secType = unpacker.readString();
	    				contract.m_strike = unpacker.readDouble();
	    				contract.m_right = unpacker.readString();
	    				String genericTickList = "";
	    				boolean snapshot = false;
	    				eClient.reqMktData(id, contract, genericTickList, snapshot);
	    				
	    		}
	    		
	    		channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	    	}
//		    
//		    int tickerId = 1;
//		    Contract contract = new Contract();
//		    contract.m_symbol = "ES";
//		    contract.m_exchange = "GLOBEX";
//		    contract.m_expiry = "201506";
//		    contract.m_secType = "FUT";
//		    String genericTickList = "";
//		    boolean snapshot = false;
//		    eClient.reqMktData(tickerId, contract, genericTickList, snapshot);
//		    
//		    tickerId++;
//		    contract.m_symbol = "ES";
//		    contract.m_exchange = "GLOBEX";
//		    contract.m_expiry = "201506";
//		    contract.m_secType = "OPT";
//		    contract.m_strike = 2115.;
//		    contract.m_right = "C";
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