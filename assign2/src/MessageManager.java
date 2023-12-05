import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

public class MessageManager implements Runnable{

    private String receivedMessage;
    private Node node;

    public MessageManager(Node node, String message){
        this.node = node;
        this.receivedMessage = message;
    }

    /**
     * recebe uma mensagem filtra e processa a mesma
     */
    public void run() {
        
        String[] msg = receivedMessage.split("-");

        switch(msg[1]){
            case "JOIN":
                if(Integer.parseInt(msg[2])!=node.getId()){
                    try{
                        receiveJoinMessage(msg[2],msg[3], Integer.parseInt(msg[4]), InetAddress.getByName(msg[5]));
                    }catch(Exception ex){
                        ex.printStackTrace();
                    }
                }
                break;

            case "LEAVE":
                for(String s : msg){
                    System.out.println("message content:" + s);
                }
                if(Integer.parseInt(msg[2])!=node.getId()){
                    receiveLeaveMessage(msg[2], msg[3]);
                }
                    break;
        }
        
    }

 /*----------------------------------------------- Join Message ---------------------------------------------------------*/
    
    /**
     * faz update ao membership log adicionando o novo membro que fez o pedido de join
     * e seguidamente tenta estabelecer uma conexão com o novo node
     * @param node_id
     * @param membershipCounter
     * @param port
     * @param tcpAddress
     */
    public void receiveJoinMessage(String node_id, String membershipCounter, int port, InetAddress tcpAddress){
        /*recebe a mensagem, retira IP e MembershipCounter,
        faz update ao membership log, e envia o membership log de volta ao node que fez join*/
        System.out.println("Received join message from node " + node_id);
        try{
            node.updateMembershipLog(node_id+"-"+membershipCounter);
            node.updateMembershipList();
            Thread.sleep(1000);
            String messageString = node.getMembershipLogString();
            Thread.sleep((long) ((Math.random()* (3000 - 1000)) + 1000));
            tcpJoinConnection(messageString, tcpAddress, port);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * tenta estabelecer conexão tcp com o novo nome para lhe enviar o membership log atualizado
     * @param message
     * @param tcpAddress
     * @param port
     */
    public void tcpJoinConnection(String message, InetAddress tcpAddress, int port){
        try{
            Socket socket = new Socket(tcpAddress, port);
            PrintWriter output = new PrintWriter(socket.getOutputStream());
            output.print(message);
            output.flush();
            output.close();
            socket.close();
        }catch(Exception ex){
            System.out.println("Node is no longer accepting connections..");
            //ex.printStackTrace();
        }
    }
    
/*------------------------------------------------ Leave Message --------------------------------------------------------*/

    /**
     * 
     * @param node_id
     * @param membership_counter
     */
    public void receiveLeaveMessage(String node_id, String membership_counter){
        System.out.println("Received leave message from node " + node_id + "|counter:" + membership_counter);
        node.updateMembershipLog(node_id+"-"+membership_counter); 
        node.updateMembershipList();
    }
}
