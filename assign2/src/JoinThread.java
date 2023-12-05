import java.io.*;
import java.net.*;

public class JoinThread implements Runnable{
    
    private int port; 
    private Node node;

    public JoinThread(Node node, int port) {
        this.node = node;
        this.port = port;
    }

    @Override
    public void run(){
        //cria sockets tcp que aceitam conexoes para a membership log message
        createConnection();

    }

    /**
     * Cria uma socket que aceita ligações tcp para receber o membership log de nodes 
     * que receberam o pedido de join
     */
    private void createConnection(){
        while(node.getMembershipMessagesReceived()<3){
        try(ServerSocket serverSocket = new ServerSocket(this.port)){
                serverSocket.setSoTimeout(5000);
                Socket socket = serverSocket.accept();

                InputStream input = socket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    
                String membershipLogMessage = "";
                String line;

                while((line = reader.readLine()) != null){
                    membershipLogMessage+=line + " ";
                }
                node.updateMembershipLog(membershipLogMessage);
                System.out.println("Received log update");
                node.increaseMembershipMessages();

                //String[] messageArray = membershipLogMessage.split(System.getProperty("line.separator"));
                reader.close();
                input.close();
                socket.close();
            
            serverSocket.close();
        }catch(SocketTimeoutException sTOex){
            if(node.getMembershipMessagesReceived() < 2){
                node.increaseMembershipMessages();
            }else{
                node.increaseMembershipMessages();
                node.setNoRespnses(true);
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
}
