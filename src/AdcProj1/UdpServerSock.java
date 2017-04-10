package AdcProj1;


import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class UdpServerSock {

	private static Logger LOGGER = Logger.getLogger(UdpServerSock.class);

	public static void main(String[] args) {

		if (args.length < 1) {
			LOGGER.error("Usage: java UpdServerSock <Port Number>");
			System.exit(1);
		}

		int portNumber = Integer.valueOf(args[0]).intValue();
		Map<String, String> messageStoreMap = new HashMap<String, String>();

		DatagramSocket socket = null;
		try {
			socket = new DatagramSocket(portNumber);

			byte[] msgbuffer = new byte[500];
			while (true) {
				DatagramPacket dataPacket = new DatagramPacket(msgbuffer, msgbuffer.length);
				socket.receive(dataPacket);
				System.out.println("Message from client: " + new String(dataPacket.getData()));
				String clientMessage = new String(dataPacket.getData());
				if (clientMessage != "") {
					String requestType = clientMessage.substring(0, clientMessage.indexOf(" "));
					LOGGER.debug("requestType: " + requestType);
					if (requestType != "" && requestType.equalsIgnoreCase("PUT")) {
						PutRequest(socket, dataPacket, messageStoreMap);
					} else if (requestType != "" && requestType.equalsIgnoreCase("GET")) {
						GetRequest(socket, dataPacket, messageStoreMap);
					} else if (requestType != "" && requestType.equalsIgnoreCase("DEL")) {
						DeleteRequest(socket, dataPacket, messageStoreMap);
					} else {
						LOGGER.error("Unknown request type: " + requestType + " is received.");
					}
				}
				LOGGER.debug("current Map size is: " + messageStoreMap.size());
			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void PutRequest(DatagramSocket socket, DatagramPacket clientPacket,
			Map<String, String> messageStoreMap) {
		LOGGER.debug("Received a PUT request from " + clientPacket.getAddress() + " at Port " + clientPacket.getPort());
		String messageData = new String(clientPacket.getData());
		if (messageData != "") {
			String keyValueData = messageData.substring(messageData.indexOf(" "));
			String key = keyValueData.substring(0, keyValueData.indexOf(","));
			String message = keyValueData.substring(keyValueData.indexOf(",") + 1);
			if (key != "") {
				LOGGER.debug("The request is to store a message with key: " + key + " and Message" + message);
				messageStoreMap.put(key.trim(), message);
				AckToClient(socket, clientPacket, "PUT", key, "");

			} else {
				String failureMsg = "Received a malformed request of length: " + clientPacket.getLength() + " from: "
						+ clientPacket.getAddress() + " at Port: " + clientPacket.getPort();
				LOGGER.error(failureMsg);
				sendFailureAckToClient(socket, clientPacket, failureMsg);
			}

		} else {
			String failureMsg = "The message content is not present.";
			LOGGER.error(failureMsg);
			sendFailureAckToClient(socket, clientPacket, failureMsg);
		}

	}

	private static void GetRequest(DatagramSocket socket, DatagramPacket clientPacket,
			Map<String, String> messageStoreMap) {
		LOGGER.debug("Received a GET request from " + clientPacket.getAddress() + " at Port " + clientPacket.getPort());
		String messageData = new String(clientPacket.getData());
		if (messageData != "") {
			String keyValueData = messageData.substring(messageData.indexOf(" "));
			String key = keyValueData.substring(0, keyValueData.indexOf(","));
			if (key != "") {
				LOGGER.debug("The request is to get a message with key: " + key);
				if (messageStoreMap.containsKey(key.trim())) {
					String retrievedMsg = messageStoreMap.get(key.trim());
					AckToClient(socket, clientPacket, "GET", key, retrievedMsg);
				} else {
					String failureMsg = "There is no key-value pair for key: " + key;
					LOGGER.error(failureMsg);
					sendFailureAckToClient(socket, clientPacket, failureMsg);
				}

			} else {
				String failureMsg = "Received a malformed request of length: " + clientPacket.getLength() + " from: "
						+ clientPacket.getAddress() + " at Port: " + clientPacket.getPort();
				LOGGER.error(failureMsg);
				sendFailureAckToClient(socket, clientPacket, failureMsg);
			}

		} else {
			String failureMsg = "The message content is not present.";
			LOGGER.error(failureMsg);
			sendFailureAckToClient(socket, clientPacket, failureMsg);
		}

	}

	private static void DeleteRequest(DatagramSocket socket, DatagramPacket clientPacket,
			Map<String, String> messageStoreMap) {
		LOGGER.debug(
				"Received a DELETE request from " + clientPacket.getAddress() + " at Port " + clientPacket.getPort());
		String messageData = new String(clientPacket.getData());
		if (messageData != "") {
			String keyValueData = messageData.substring(messageData.indexOf(" "));
			String key = keyValueData.substring(0, keyValueData.indexOf(","));
			if (key != "") {
				LOGGER.debug("The request is to get a message with key: " + key);
				if (messageStoreMap.containsKey(key.trim())) {
					messageStoreMap.remove(key.trim());
					AckToClient(socket, clientPacket, "DEL", key, "");
				} else {
					String failureMsg = "There exist no such key-value pair for key: " + key;
					LOGGER.error(failureMsg);
					sendFailureAckToClient(socket, clientPacket, failureMsg);
				}

			} else {
				String failureMsg = "Received a malformed request of length: " + clientPacket.getLength() + " from: "
						+ clientPacket.getAddress() + " at Port: " + clientPacket.getPort();
				LOGGER.error(failureMsg);
				sendFailureAckToClient(socket, clientPacket, failureMsg);
			}

		} else {
			String failureMsg = "The message content is not present.";
			LOGGER.error(failureMsg);
			sendFailureAckToClient(socket, clientPacket, failureMsg);
		}

	}

	private static void AckToClient(DatagramSocket socket, DatagramPacket request, String requestType, String key,
			String returnMsg) {
		LOGGER.debug("Sending acknowledgement to client...");
		try {
			byte[] ackMessage = new byte[500];
			if (returnMsg != "" && requestType.equalsIgnoreCase("GET")) {
				ackMessage = ("Retrieved message with key: " + key + " is: " + returnMsg).getBytes();
			} else {
				ackMessage = (requestType + " with key: " + key + " SUCCESS").getBytes();
			}
			DatagramPacket ackMsgPacket = new DatagramPacket(ackMessage, ackMessage.length, request.getAddress(),
					request.getPort());
			socket.send(ackMsgPacket);

		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		}

	}

	private static void sendFailureAckToClient(DatagramSocket socket, DatagramPacket request, String returnMsg) {
		LOGGER.debug("Sending acknowledgement to client for failure...");
		try {
			byte[] ackMessage = new byte[500];
			ackMessage = ("Request FAILED due to: " + returnMsg).getBytes();
			DatagramPacket ackMsgPacket = new DatagramPacket(ackMessage, ackMessage.length, request.getAddress(),
					request.getPort());
			socket.send(ackMsgPacket);

		} catch (IOException e) {
			LOGGER.error("An exception has occured: " + e);
		}

	}

}