package AdcProj1;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class TcpClientSock {

	private static Logger LOGGER = Logger.getLogger(TcpClientSock.class);

	public static void main(String[] args) {
		LOGGER.debug("Client main is called");
		if (args.length < 2) {
			LOGGER.error("Using: java TcpClientSock <Host Name> <Port Number>");
			System.exit(1);
		}

		String hostName = args[0].toString();
		int portNumber = Integer.valueOf(args[1]).intValue();
		LOGGER.debug("in try of client");
		PutTrans(hostName, portNumber);
		
		GetTrans(hostName, portNumber);
		DeleteTrans(hostName, portNumber);
	}

	private static void PutTrans(String hostName, int portNumber) {
		String putReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("TCP_PUT_REQUEST_DATA");
		LOGGER.debug("putting data in client hash map: " + putReqData);
		try {
			List<String> items = Arrays.asList(putReqData.split("\\s*\\|\\s*"));
			LOGGER.debug("items stored in as arrays: " + items);
			DataOutputStream outputStream = null;
			Socket client = null;
			for (String tokens : items) {
				client = new Socket(hostName, portNumber);
				outputStream = new DataOutputStream(client.getOutputStream());
				LOGGER.debug("String items: " + tokens);
				outputStream.writeUTF("PUT " + tokens);
				AckFromServer(client);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void GetTrans(String hostName, int portNumber) {
		String reqReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("TCP_GET_REQUEST_DATA");
		LOGGER.debug("get data or retrieving data in client: " + reqReqData);
		Socket client = null;
		try {
			List<String> items = Arrays.asList(reqReqData.split("\\s*,\\s*"));
			LOGGER.debug("items retrieve in as arrays: " + items);
			DataOutputStream outputStream = null;

			for (String tokens : items) {
				client = new Socket(hostName, portNumber);
				outputStream = new DataOutputStream(client.getOutputStream());
				LOGGER.debug("String items: " + tokens);
				outputStream.writeUTF("GET " + tokens);
				AckFromServer(client);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	private static void DeleteTrans(String hostName, int portNumber) {
		String reqReqData = SocketPrgmPropertiesHandler.getInstance().getProperty("TCP_DEL_REQUEST_DATA");
		LOGGER.debug("deleting data in clients array: " + reqReqData);
		Socket client = null;
		try {
			List<String> items = Arrays.asList(reqReqData.split("\\s*,\\s*"));
			LOGGER.debug("delete items as arrays: " + items);
			DataOutputStream outputStream = null;

			for (String tokens : items) {
				client = new Socket(hostName, portNumber);
				outputStream = new DataOutputStream(client.getOutputStream());
				LOGGER.debug("Delete String items: " + tokens);
				outputStream.writeUTF("DEL " + tokens);
				AckFromServer(client);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static void AckFromServer(Socket client) {
		try {
			DataInputStream inputStream = new DataInputStream(client.getInputStream());
			client.setSoTimeout(
					Integer.valueOf(SocketPrgmPropertiesHandler.getInstance().getProperty("CLIENT SOCKET TIMEOUT")));
			String ackMessage = inputStream.readUTF();
			LOGGER.debug("Acknowledgement message2: " + ackMessage);
		} catch (SocketTimeoutException e) {
			LOGGER.error("Server is not responding. Timeout error has occured.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception ex) {
			LOGGER.debug("Exception2: " + ex);
		}
	}

}

