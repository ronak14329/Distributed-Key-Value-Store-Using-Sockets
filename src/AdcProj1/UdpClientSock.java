package AdcProj1;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import AdcProj1.SocketPrgmPropertiesHandler;

public class UdpClientSock {

	private static Logger LOGGER = Logger.getLogger(UdpClientSock.class);

	public static void main(String[] args) {

		if (args.length < 2) {
			LOGGER.error("Usage: java UdpClientSock <Host Name> <Port Number>");
			System.exit(1);
		}

		String hostName = args[0].toString();

		int portNumber = Integer.valueOf(args[1]).intValue();
		
		try {
			
			InetAddress host = InetAddress.getByName(hostName);
			
			
			PutTrans(host,portNumber);
			GetTrans(host,portNumber);
			DeleteTrans(host,portNumber);

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void PutTrans(InetAddress host, int portNumber) {
		String putReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("UDP_PUT_REQUEST_DATA");
		LOGGER.debug("put data in client: " + putReqData);
		DatagramSocket client = null;
		try {
			List<String> items = Arrays.asList(putReqData.split("\\s*\\|\\s*"));
			for (String tokens : items) {
				client = new DatagramSocket();
				LOGGER.debug("Message String items: " + tokens);
				String clientMsg = "PUT " + tokens;
				DatagramPacket clientMsgPacket = new DatagramPacket(clientMsg.getBytes(),clientMsg.length(),host,portNumber);
				client.send(clientMsgPacket);
				AckFromServer(client);
				client.close();
			}

		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		} finally {
			client.close();
		}

	}
	
	private static void GetTrans(InetAddress host, int portNumber) {
		String reqReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("UDP_GET_REQUEST_DATA");
		LOGGER.debug("get data in client: " + reqReqData);
		DatagramSocket client = null;
		try {
			List<String> items = Arrays.asList(reqReqData.split("\\s*,\\s*"));
			for (String tokens : items) {
				client = new DatagramSocket();
				LOGGER.debug("Message String items: " + tokens);
				String clientMsg = "GET " + tokens;
				DatagramPacket clientMsgPacket = new DatagramPacket(clientMsg.getBytes(),clientMsg.length(),host,portNumber);
				client.send(clientMsgPacket);
				AckFromServer(client);
				client.close();
			}

		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		} finally {
			client.close();
		}

	}
	
	private static void DeleteTrans(InetAddress host, int portNumber) {
		String reqReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("UDP_DEL_REQUEST_DATA");
		LOGGER.debug("get delete data in client: " + reqReqData);
		DatagramSocket client = null;
		try {
			List<String> items = Arrays.asList(reqReqData.split("\\s*,\\s*"));
			for (String tokens : items) {
				client = new DatagramSocket();
				LOGGER.debug("Message String items: " + tokens);
				String clientMsg = "DEL " + tokens;
				DatagramPacket clientMsgPacket = new DatagramPacket(clientMsg.getBytes(),clientMsg.length(),host,portNumber);
				client.send(clientMsgPacket);
				AckFromServer(client);
				client.close();
			}

		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		} finally {
			client.close();
		}

	}

	private static void AckFromServer(DatagramSocket client) {
		try {

			client.setSoTimeout(
					Integer.valueOf(SocketPrgmPropertiesHandler.getInstance().getProperty("CLIENT_SOCKET_TIMEOUT")));
			byte[] ackMsgBuffer = new byte[500];
			DatagramPacket returnMsgPacket = new DatagramPacket(ackMsgBuffer, ackMsgBuffer.length);
			client.receive(returnMsgPacket);
			LOGGER.debug("Acknowledgement message: " + new String(returnMsgPacket.getData()));
		} catch (SocketTimeoutException e) {
			LOGGER.error("Server is not responding. Timeout error has occured.");
		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		} catch (Exception ex) {
			LOGGER.debug("Exception: " + ex);
		}
	}

	
}