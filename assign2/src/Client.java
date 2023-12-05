import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.rmi.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.MessageDigest;

public class Client {

    private Client() {

    }

    public static void main(String[] args) {

        String nodeAP = args[0];
        String operation = args[1];
        if (args.length == 2) {
            try {

                Registry registry = LocateRegistry.getRegistry();
                ClusterMembership membership = (ClusterMembership) registry.lookup(nodeAP);

                switch (operation) {
                    case "join":
                        membership.join();
                        break;

                    case "leave":
                        membership.leave();
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (args.length == 3) {
            try {
                switch (args[1]) {
                    // Client <node> put <path>
                    // Client <node> get <key>
                    // Client <node> delete <key>
                    case "put":

                        String nodePut = args[0];
                        String path_to_file = args[2];

                        // key = hex(hash(ficheiro))
                        // value = hex(texto)
                        put(nodePut, path_to_file);

                        break;

                    case "get":
                        String nodeGet = args[0];
                        String keyGet = args[2];
                        get(nodeGet, keyGet);
                        break;

                    case "delete":
                        String nodeDelete = args[0];
                        String keyDelete = args[2];
                        delete(nodeDelete, keyDelete);
                        break;

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void delete(String nodeId, String key) {
        try {
            sendDatagramDeleteMessage(nodeId, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void get(String nodeId, String key) {
        try {
            sendDatagramGetMessage(nodeId, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void put(String nodeId, String path) {

        String content;
        try {
            content = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
            // System.out.println(content);
            String key = getSha256(content);
            // System.out.println(hashed_content);
            String value = convertStringToHex(content);
            // System.out.println("Hex content of file read: " + value);

            sendDatagramPutMessage(nodeId, key, value);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void sendDatagramDeleteMessage(String nodeId, String key) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress
            (0));
            String original_message = "DELETE-" + key + "-" + true;
            // GET-key
            byte[] msgBytes = original_message.getBytes();
            InetAddress mcastaddr = InetAddress.getByName("127.0.0.1" + nodeId);
            int port = 8080;
            InetSocketAddress dest = new InetSocketAddress(mcastaddr, port);
            DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
            sender.send(hi);
            System.out.println("Delete message sent to node " + nodeId);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static void sendDatagramGetMessage(String nodeId, String key) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
            String original_message = "GET-" + key + "-" + nodeId;
            // GET-key
            byte[] msgBytes = original_message.getBytes();
            InetAddress mcastaddr = InetAddress.getByName("127.0.0.1" + nodeId);
            int port = 8080;
            InetSocketAddress dest = new InetSocketAddress(mcastaddr, port);
            DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
            sender.send(hi);
            System.out.println("Get message sent to node " + nodeId);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public static void sendDatagramPutMessage(String nodeId, String key, String value) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
            String original_message = "PUT-" + nodeId + "-" + key + "-" + value +"-" +"true";
            // PUT-id-key-value
            byte[] msgBytes = original_message.getBytes();
            InetAddress mcastaddr = InetAddress.getByName("127.0.0.1" + nodeId);
            int port = 8080;
            InetSocketAddress dest = new InetSocketAddress(mcastaddr, port);
            DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
            sender.send(hi);
            System.out.println("Put message sent to node " + nodeId + ": " + original_message);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private static String convertStringToHex(String str) {
        // verificada
        byte[] getBytesFromString = str.getBytes(StandardCharsets.UTF_8);
        BigInteger bigInteger = new BigInteger(1, getBytesFromString);

        String convertedResult = String.format("%x", bigInteger);

        return (convertedResult);
    }

    public static String getSha256(String s) {
        // verificada
        // returns hex sha to string
        String failedResult = "-1";

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hashInBytes = md.digest(s.getBytes(StandardCharsets.UTF_8));
            // bytes to hex

            StringBuilder sb = new StringBuilder();
            for (byte b : hashInBytes) {
                sb.append(String.format("%02x", b));
            }
            return (sb.toString());

            // convert byte array to string

        } catch (Exception e) {
            e.printStackTrace();
        }

        return failedResult; // if try fails
    }

    public static BigInteger getSha256fromKey(Integer key) {
        // verificada
        // returns numeric int with ashed key
        BigInteger failedResult = BigInteger.valueOf(-1);

        try {

            String hash_hex = getSha256(key.toString()); // get sha256 of id
            BigInteger hash_number = new BigInteger(hash_hex, 16);
            return hash_number;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return failedResult; // if try fails
    }

}