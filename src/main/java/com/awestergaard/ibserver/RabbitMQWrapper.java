package com.awestergaard.ibserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.ib.client.ComboLeg;
import com.ib.client.CommissionReport;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.EWrapper;
import com.ib.client.Execution;
import com.ib.client.Order;
import com.ib.client.OrderState;
import com.ib.client.TagValue;
import com.ib.client.UnderComp;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

public class RabbitMQWrapper implements EWrapper {

	private Connection connection;
	private Channel channel;
	private MessagePack msgpack;
	
    public RabbitMQWrapper() throws java.io.IOException {
    	ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
		
		msgpack = new MessagePack();
        msgpack.register(ComboLeg.class);
        msgpack.register(UnderComp.class);
        msgpack.register(Contract.class);
        msgpack.register(TagValue.class);
		msgpack.register(ContractDetails.class);
    }
    
    public void startServingRequests() throws ShutdownSignalException, ConsumerCancelledException, InterruptedException, IOException {
    	
    }
    
	public void error(Exception e) {
		System.out.println(e.getMessage());
	}

	public void error(String str) {
		System.out.println(str);
	}

	public void error(int id, int errorCode, String errorMsg) {
		// TODO Auto-generated method stub

	}

	public void connectionClosed() {
		// TODO Auto-generated method stub

	}

	public void tickPrice(
			int tickerId, 
			int field, 
			double price, 
			int canAutoExecute) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Packer packer = msgpack.createPacker(out);
			packer.write(tickerId);
			packer.write(field);
			packer.write(price);
			packer.write(canAutoExecute);
			channel.basicPublish("", "tickPrice", null, out.toByteArray());
			System.out.println(" [x] Sent tickPrice");
		} catch(java.io.IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void tickSize(int tickerId, int field, int size) {
		// TODO Auto-generated method stub

	}

	public void tickOptionComputation(int tickerId, int field,
			double impliedVol, double delta, double optPrice,
			double pvDividend, double gamma, double vega, double theta,
			double undPrice) {
		// TODO Auto-generated method stub

	}

	public void tickGeneric(int tickerId, int tickType, double value) {
		// TODO Auto-generated method stub

	}

	public void tickString(int tickerId, int tickType, String value) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Packer packer = msgpack.createPacker(out);
			packer.write(tickerId);
			packer.write(tickType);
			packer.write(value);
			byte[] bytes = out.toByteArray();
			channel.basicPublish("", "tickString", null, bytes);
			System.out.println(" [x] Sent tickString");
		} catch(java.io.IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void tickEFP(int tickerId, int tickType, double basisPoints,
			String formattedBasisPoints, double impliedFuture, int holdDays,
			String futureExpiry, double dividendImpact, double dividendsToExpiry) {
		// TODO Auto-generated method stub

	}

	public void orderStatus(int orderId, String status, int filled,
			int remaining, double avgFillPrice, int permId, int parentId,
			double lastFillPrice, int clientId, String whyHeld) {
		// TODO Auto-generated method stub

	}

	public void openOrder(int orderId, Contract contract, Order order,
			OrderState orderState) {
		// TODO Auto-generated method stub

	}

	public void openOrderEnd() {
		// TODO Auto-generated method stub

	}

	public void updateAccountValue(String key, String value, String currency,
			String accountName) {
		// TODO Auto-generated method stub

	}

	public void updatePortfolio(Contract contract, int position,
			double marketPrice, double marketValue, double averageCost,
			double unrealizedPNL, double realizedPNL, String accountName) {
		// TODO Auto-generated method stub

	}

	public void updateAccountTime(String timeStamp) {
		// TODO Auto-generated method stub

	}

	public void accountDownloadEnd(String accountName) {
		// TODO Auto-generated method stub

	}

	public void nextValidId(int orderId) {
		// TODO Auto-generated method stub

	}

	public void contractDetails(int reqId, ContractDetails contractDetails) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Packer packer = msgpack.createPacker(out);
			packer.write(contractDetails);
			byte[] bytes = out.toByteArray();
			channel.basicPublish("", "contractDetails", null, bytes);
			System.out.println(" [x] Sent contractDetails");
		} catch(java.io.IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void bondContractDetails(int reqId, ContractDetails contractDetails) {
		// TODO Auto-generated method stub

	}

	public void contractDetailsEnd(int reqId) {
		// TODO Auto-generated method stub

	}

	public void execDetails(int reqId, Contract contract, Execution execution) {
		// TODO Auto-generated method stub

	}
	
	public void execDetailsEnd(int reqId) {
		// TODO Auto-generated method stub

	}

	public void updateMktDepth(int tickerId, int position, int operation,
			int side, double price, int size) {
		// TODO Auto-generated method stub

	}

	public void updateMktDepthL2(int tickerId, int position,
			String marketMaker, int operation, int side, double price, int size) {
		// TODO Auto-generated method stub

	}

	public void updateNewsBulletin(int msgId, int msgType, String message,
			String origExchange) {
		// TODO Auto-generated method stub

	}

	public void managedAccounts(String accountsList) {
		// TODO Auto-generated method stub

	}

	public void receiveFA(int faDataType, String xml) {
		// TODO Auto-generated method stub

	}

	public void historicalData(int reqId, String date, double open,
			double high, double low, double close, int volume, int count,
			double WAP, boolean hasGaps) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Packer packer = msgpack.createPacker(out);
			packer.write(reqId);
			packer.write(date);
			packer.write(open);
			packer.write(high);
			packer.write(low);
			packer.write(close);
			packer.write(volume);
			packer.write(count);
			packer.write(WAP);
			packer.write(hasGaps);
			byte[] bytes = out.toByteArray();
			channel.basicPublish("", "historicalData", null, bytes);
			System.out.println(" [x] Sent historicalData");
		} catch(java.io.IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void scannerParameters(String xml) {
		// TODO Auto-generated method stub

	}

	public void scannerData(int reqId, int rank,
			ContractDetails contractDetails, String distance, String benchmark,
			String projection, String legsStr) {
		// TODO Auto-generated method stub

	}

	public void scannerDataEnd(int reqId) {
		// TODO Auto-generated method stub

	}

	public void realtimeBar(int reqId, long time, double open, double high,
			double low, double close, long volume, double wap, int count) {
		// TODO Auto-generated method stub

	}

	public void currentTime(long time) {
		// TODO Auto-generated method stub

	}

	public void fundamentalData(int reqId, String data) {
		// TODO Auto-generated method stub

	}

	public void deltaNeutralValidation(int reqId, UnderComp underComp) {
		// TODO Auto-generated method stub

	}

	public void tickSnapshotEnd(int reqId) {
		// TODO Auto-generated method stub

	}

	public void marketDataType(int reqId, int marketDataType) {
		// TODO Auto-generated method stub

	}

	public void commissionReport(CommissionReport commissionReport) {
		// TODO Auto-generated method stub

	}

	public void position(String account, Contract contract, int pos,
			double avgCost) {
		// TODO Auto-generated method stub

	}

	public void positionEnd() {
		// TODO Auto-generated method stub

	}

	public void accountSummary(int reqId, String account, String tag,
			String value, String currency) {
		// TODO Auto-generated method stub

	}

	public void accountSummaryEnd(int reqId) {
		// TODO Auto-generated method stub

	}

}
