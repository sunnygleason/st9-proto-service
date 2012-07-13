package com.g414.st9.proto.service.pubsub;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;

import com.g414.st9.proto.service.helper.EncodingHelper;

public class CubePublisher implements Publisher {
    private final String hostname;
    private final int port;

    public CubePublisher() {
        this.hostname = System.getProperty("cube.host", null);
        this.port = Integer.parseInt(System.getProperty("cube.port", "1180"));
    }

    public void publish(final String topic, final Map<String, Object> message) {
        if (this.hostname == null) {
            return;
        }

        try {
            byte[] tosend = EncodingHelper.convertToJson(message).getBytes();
            InetAddress address = InetAddress.getByName(hostname);
            DatagramPacket packet = new DatagramPacket(tosend, tosend.length,
                    address, port);
            DatagramSocket datagramSocket = new DatagramSocket();
            datagramSocket.send(packet);
        } catch (Exception e) {
            System.out.println("ERROR while sending message to Cube: "
                    + message.toString());
            e.printStackTrace();
        }
    }
}
