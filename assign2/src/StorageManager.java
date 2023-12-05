import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

public class StorageManager implements Runnable{

    private Node node;
    private DatagramSocket socket;

    public StorageManager(Node node){
        this.node=node;
        try{
            this.socket = new DatagramSocket(null);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * 
     */
    public void stop(){
        try{
            this.socket.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
        
    }

    public void run(){
        try{

            socket.setReuseAddress(true); // set reuse address before binding
            socket.bind(new InetSocketAddress("127.0.0.1" + node.getId(), 8080)); // bind

            while(true){
                byte[] msgBytes = new byte[60000]; // up to 1024 bytes
                DatagramPacket packet = new DatagramPacket(msgBytes, msgBytes.length);
                socket.receive(packet);

                String content = node.getRidOfAnnoyingChar(packet);

                node.getExecutor().execute(new StorageMessageManager(node, content));
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
    