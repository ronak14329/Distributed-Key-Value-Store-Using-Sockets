package AdcProj1;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class TcpServerSock {

	public static Logger LOGGER = Logger.getLogger(TcpServerSock.class);
public static void main(String[] args) {
		System.out.println(
				"Test prop file: " + SocketPrgmPropertiesHandler.getInstance().getProperty("TCP_PUT_REQUEST_DATA"));
		LOGGER.debug("debugging using log4j");

		if (args.length < 1) {
			LOGGER.error("Using: java TcpServerSock <Port Number>");
			System.exit(1);
		}

		int portNumber = Integer.valueOf(args[0]).intValue();
		Map<String, String> messageStoreMap = new HashMap<String, String>();

		try {
			ServerSocket server = new ServerSocket(portNumber);
			LOGGER.debug("port number: " + portNumber);
			while (true) {
				LOGGER.debug("waiting  ...");
				Socket client = server.accept();
				LOGGER.debug("waiting 2 ...");
				DataInputStream input = new DataInputStream(client.getInputStream());
				String clientMessage = input.readUTF();
				if (clientMessage != "") {
					String requestType = clientMessage.substring(0, clientMessage.indexOf(" "));
					String msgContent = clientMessage.substring(clientMessage.indexOf(" "));
					LOGGER.debug("requestType: " + requestType + " msgContent" + msgContent);
					if (requestType != "" && requestType.equalsIgnoreCase("PUT")) {
						PutRequest(client, msgContent, messageStoreMap);
					}else if (requestType != "" && requestType.equalsIgnoreCase("GET")) {
						GetRequest(client, msgContent, messageStoreMap);
					}else if (requestType != "" && requestType.equalsIgnoreCase("DEL")) {
						DeleteRequest(client, msgContent, messageStoreMap);
					}else{
						LOGGER.error("Unknown request type: "+requestType+ " is received.");
					}
				}
				LOGGER.debug("current Map size is: " + messageStoreMap.size());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void PutRequest(Socket client, String msgContent, Map<String, String> messageStoreMap) {
		LOGGER.debug("PUT request received from " + client.getInetAddress() + " at Port " + client.getPort());
		if (msgContent != "") {

			String key = msgContent.substring(0, msgContent.indexOf(","));
			String message = msgContent.substring(msgContent.indexOf(","));
			if (key != "") {
				LOGGER.debug("The request is to store a message with key: " + key);
				messageStoreMap.put(key, message);
				AckToClient(client, "PUT", key, "");

			} else {
				LOGGER.error("Received a wrong request of length: " + msgContent.length() + " from: "
						+ client.getInetAddress() + " at Port: " + client.getPort());
			}

		} else {
			LOGGER.debug("The searched message content is not present.");
		}
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void GetRequest(Socket client, String msgContent, Map<String, String> messageStoreMap) {
		LOGGER.debug("GET request received from " + client.getInetAddress() + " at Port " + client.getPort());
		if (msgContent != "") {
			String key = msgContent;
			if (key != "") {
				LOGGER.debug(" Requesting to get a message with key: " + key);
				if (messageStoreMap.containsKey(key)) {
					String retrievedMsg = messageStoreMap.get(key);
					AckToClient(client, "GET", key, retrievedMsg);
				} else {
					LOGGER.error("There exist no key-value pair for key: " + key);
				}

			} else {
				LOGGER.error("Received a wrong request of length: " + msgContent.length() + " from: "
						+ client.getInetAddress() + " at Port: " + client.getPort());
			}

		} else {
			LOGGER.debug("The searched message content is not present.");
		}
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static void DeleteRequest(Socket client, String msgContent, Map<String, String> messageStoreMap) {
		LOGGER.debug(" DELETE request received from " + client.getInetAddress() + " at Port " + client.getPort());
		if (msgContent != "") {
			String key = msgContent;
			if (key != "") {
				LOGGER.debug(" Requesting to delete a message with key: " + key);
				if (messageStoreMap.containsKey(key)) {
					messageStoreMap.remove(key);
					AckToClient(client, "DELETE", key, "");
				} else {
					LOGGER.error("There exists no key-value pair for key: " + key);
				}

			} else {
				LOGGER.error("Received a wrong request of length: " + msgContent.length() + " from: "
						+ client.getInetAddress() + " at Port: " + client.getPort());
			}

		} else {
			LOGGER.debug("The searched message content is not present.");
		}
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void AckToClient(Socket client, String requestType, String key, String returnMsg) {
		LOGGER.debug("Sending acknowledgement to client...");
		try {
			DataOutputStream outStream = new DataOutputStream(client.getOutputStream());
			if (returnMsg != "" && requestType.equalsIgnoreCase("GET")) {
				outStream.writeUTF("Retrieved message with key: " + key + " is: " + returnMsg);
			} else {
				outStream.writeUTF(requestType + " with key: " + key + " SUCCESS");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
