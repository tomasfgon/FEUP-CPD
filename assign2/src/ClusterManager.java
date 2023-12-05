import java.net.*;
import java.nio.Buffer;
import java.util.*;
import java.io.*;

public class ClusterManager implements Runnable{
    
    private int mCastPort;
    private InetAddress mCastAddress;
    private MulticastSocket mCastSocket;
    private Node node;
    //private NetworkInterface netif;

    public ClusterManager(Node node,int port, String address){
        try{
            this.node = node;
            this.mCastPort=port;
            this.mCastAddress=InetAddress.getByName(address);
            //this.netif = NetworkInterface.getByName("le0");
            //this.messagesReceived = 0;
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * Faz uso do cluster manager para enviar uma mensagem para a socket multicast
     * @param message
     */
    public void sendMessage(byte[] message){
        try{
            DatagramPacket messagePacket = new DatagramPacket(message, message.length, mCastAddress, mCastPort);
            this.mCastSocket.send(messagePacket);
            System.out.println("message sent");

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * chamado quando existe um pedido de leave
     * sai do multicast group e fecha a socket
     */
    public void stop(){
        try{
            this.mCastSocket.leaveGroup(this.mCastAddress);
            System.out.println("Node left the cluster.");
            //this.mCastSocket.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
        
    }

    /**
     * Está consitentemente a receber mensagens multicast e quando recebe
     * cria um novo thread da classe MessageManager que processa as mensagens recebidas
     */
    public void run(){

        try{
            this.mCastSocket = new MulticastSocket(this.mCastPort);
            this.mCastSocket.joinGroup(this.mCastAddress);

            while(true){
                byte[] buffer = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                mCastSocket.receive(packet);

                String content = node.getRidOfAnnoyingChar(packet);

                node.getExecutor().execute(new MessageManager(this.node, content));
            }

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

}
