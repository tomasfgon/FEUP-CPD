    import java.net.*;
    import java.nio.charset.StandardCharsets;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;
    import java.nio.file.StandardCopyOption;
    import java.io.*;
    import java.math.BigDecimal;
    import java.math.BigInteger;
    import java.util.*;
    import java.util.concurrent.ExecutorService;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ThreadPoolExecutor;

    import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

    //import java.rmi.*;
    import java.rmi.registry.Registry;
    import java.rmi.registry.LocateRegistry;
    import java.rmi.RemoteException;
    import java.rmi.server.UnicastRemoteObject;
    import java.security.MessageDigest;
    import java.security.NoSuchAlgorithmException;

    public class Node implements ClusterMembership {

        private String IPMulticastAddress;
        private int mCastPort;
        private int nodeId;
        private int storagePort;
        private int membershipCounter;
        private int membershipMessagesReceived;
        private MulticastSocket mSocket;
        private ExecutorService exec;
        private ClusterManager clusterManager;
        private StorageManager storageManager;
        private ArrayList<Integer> membershipList;
        private int auxnodeId;
        private boolean noResponses;

    /*---------------------------------------------- Construtores ---------------------------------------------------------------*/

        /**
         * constructor for Node
         * @param mcastAddress
         * @param mcastPort
         * @param nodeId
         * @param storagePort
         * @throws IOException
         */
        public Node(String mcastAddress, int mcastPort, int node_Id, int storage_Port) throws IOException {
            IPMulticastAddress = mcastAddress;
            mCastPort = mcastPort;
            nodeId = node_Id;
            storagePort = storage_Port;
            membershipMessagesReceived=0;
            membershipList = new ArrayList<Integer>();
            //inicializa membershipCounter
            membershipCounter=0;
            readMembershipCounter();
            //inicia o executor com uma thread pool com o nr de threads igual ao numero de cores do processador da maquina
            exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            //inicializa os managers
            clusterManager = new ClusterManager(this, mCastPort, IPMulticastAddress);
            this.auxnodeId = nodeId;
            this.noResponses=false;
        }

        /*------------------------------------------ Getters e Setters de Atributos -----------------------------------------------------*/

        /**
         * devolve o id do node em questão
         * @return ID DE UM NODE
         */
        public int getId() {
            return nodeId;
        }

        /**
         * retorna o nr de conexões recebidas após enviar a mensagem de join 
         * (max 3)
         * @return nr de conexões bem sucedidas após enviar a mensagem de join 
         */
        public int getMembershipMessagesReceived() {
            return membershipMessagesReceived;
        }

        /**
         * retorna o executor
         * @return a instancia do executor
         */
        public ExecutorService getExecutor() {
            return exec;
        }

        /**
         * retorna o cluster manager
         * @return a instancia do cluster Manager
         */
        public ClusterManager getClusterManager() {
            return clusterManager;
        }

        public ArrayList<Integer> getMembershipList(){
            return this.membershipList;
        }

        /**
         * incrementa o atributo membershipCounter após uma operação de leave ou join
         */
        public void updateMembershipCounter(){
            membershipCounter++;
        }

        /**
         * incrementa o nr de conexões bem sucedidas, após enviar a mensagem de join, em 1 valor
         */
        public void increaseMembershipMessages() {
            membershipMessagesReceived += 1;
        }

        public boolean getNoresponses(){
            return this.noResponses;
        }

        public void setNoRespnses(boolean b) {
            this.noResponses=b;
        }

        /*----------------------------------------------- Main -----------------------------------------------------------------*/

        public static void main(String[] args) {

            String mcastAddress = args[0];
            int mcastPort = Integer.parseInt(args[1]);
            int nodeId = Integer.parseInt(args[2]);
            int storagePort = Integer.parseInt(args[3]);

            try {

                Node obj = new Node(mcastAddress, mcastPort, nodeId, storagePort);

                ClusterMembership stub = (ClusterMembership) UnicastRemoteObject.exportObject(obj, 0);

                String nodeAP = args[2];

                // Bind the remote object's stub in the registry
                Registry registry = LocateRegistry.getRegistry();
                registry.rebind(nodeAP, stub);
                
                System.err.println("Node Ready");

            } catch (Exception e) {
                System.err.println("Server exception: " + e.toString());
                e.printStackTrace();
            }

        }

    /*---------------------------------------------------- Join Metodos Membership -------------------------------------------*/

        /**
         * quando recebe um pedido de join (RMI)
         */
        public void join(){
            //readMembershipCounter();
            //So pode fazer join se o membership counter for par o que significa que ainda não faz parte do cluster
            if(membershipCounter%2!=1 && membershipCounter!=0){
                System.out.println("Este node já faz parte do cluster.");
                return; 
            }

            //faz update ao próprio membership counter adicionando 1 
            if(membershipCounter!=0){
                updateMembershipCounter();
            }
            //Começa a correr o clusterManager responsável por aceitar conexões multicast do cluster do IP indicado
            exec.execute(clusterManager);

            
            
            int joinTcpPort = 100 + nodeId;

            //cria threads que aceitam, em tcp, mensagens de membership
            exec.execute(new JoinThread(this,joinTcpPort));
            //envia a mensagem de join por multicast
            joinMessage(joinTcpPort);

            //só pode avançar quando pelo menos 3 mensagens de membership forem recebidas
            System.out.println("Waiting for membership messages");
            while(membershipMessagesReceived<3){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //se não receber nenhuma resposta inicia o seu proprio log
            if(getNoresponses()==true){
                System.out.println("No message received");
                updateMembershipLog(nodeId + "-" + membershipCounter);
            }

            //adiciona os nodes que estão no log à membershipList
            updateMembershipList();     

            System.out.println("Node joinded the cluster successfully.");

            //começa a correr o storageManager para lidar com pedidos de storage(put get delete)
            storageManager = new StorageManager(this);
            exec.execute(storageManager);

            //introducing new node n ;  if (key in node n+1 and < node n)=> (go from n+1 to n)  

        if(membershipList.size()>1){
            Float currentNodeAngle = getAngle(getSha256fromKey(nodeId).toString()); //angulo do novo node
            ArrayList<Float> anglesList = getAllAngles(membershipList); //lista com todos os angulos
            Collections.sort(anglesList); //ordena a lista
            Map<Float, Integer> map = getAngleIdMap(membershipList);//id atraves do angulo

            //vai buscar o node acima
            int index = anglesList.indexOf(currentNodeAngle); 
            Float upper;
            if (index == anglesList.size() - 1) {
                upper = anglesList.get(0);
            } else {
                upper = anglesList.get(index + 1);
            }

            int idUpper = map.get(upper);//hash do id

            System.out.println("Getting files from node " + map.get(upper));
            
            sendMoveMessage(String.valueOf(idUpper), currentNodeAngle);
        }
        }

        public String sendMoveMessage(String upperNode, Float currentNodeAngle){
            try {
                DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
                String original_message = "MOVE-" + nodeId +"-" + upperNode + "-" + currentNodeAngle;
                
                byte[] msgBytes = original_message.getBytes();
                InetAddress addr = InetAddress.getByName("127.0.0.1" + upperNode);
                int port = 8080;
                InetSocketAddress dest = new InetSocketAddress(addr, port);
                DatagramPacket packet = new DatagramPacket(msgBytes, msgBytes.length, dest);
                sender.send(packet);


                System.out.println("Move message sent to node " + upperNode);
            } catch (Exception exception) {
                exception.printStackTrace();
            }
            return "";
        }

        private static void copyFile(File src, File dest) throws IOException {
            InputStream is = null;
            OutputStream os = null;
            try {
                is = new FileInputStream(src);
                os = new FileOutputStream(dest);

                // buffer size 1K
                byte[] buf = new byte[1024];

                int bytesRead;
                while ((bytesRead = is.read(buf)) > 0) {
                    os.write(buf, 0, bytesRead);
                }
            } finally {
                is.close();
                os.close();
            }
        }

        public Map<Float, Integer> getAngleIdMap(ArrayList<Integer> membershipList) {
            // verificado
            // map com angulos e ids dos nos da lista
            Map<Float, Integer> map = new HashMap<Float, Integer>();

            for (int i = 0; i < membershipList.size(); i++) {

                String id = membershipList.get(i) + "";

                try {
                    map.put(getAngle(getSha256fromKey(Integer.parseInt(id)).toString()), Integer.parseInt(id));

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            return map;
        }

        public ArrayList<Float> getAllAngles(ArrayList<Integer> membershipList) {
            // verificada
            // lista ordenada de todos os angulos de nós
            ArrayList<Float> list = new ArrayList<Float>();

            for (int i = 0; i < membershipList.size(); i++) {

                String id = membershipList.get(i) + "";

                try {
                    BigInteger hash_number = getSha256fromKey(Integer.valueOf(id));
                    list.add(getAngle(hash_number.toString()));
                    Collections.sort(list);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            return list;
        }

        /**
         * 
         * @param hashedId
         * @return
         */
        public float getAngle(String hashedId) {
            // verificada
            // must receive hashedId in decimal form
            BigInteger value = new BigInteger(hashedId);

            // maximum angle is 2^256 -> 360º; hashedId -> x
            BigDecimal val256 = new BigDecimal(Math.pow(2, 256));
            BigDecimal value2 = new BigDecimal(value);
            BigDecimal aux = (value2.multiply(new BigDecimal(360)));
            BigDecimal aux2 = aux.divide(val256);

            return aux2.floatValue();
        }

        /**
         * 
         * @param membershipList
         * @return
         */
        public ArrayList<BigInteger> getHashedIdList(ArrayList<Integer> membershipList) {
            // lista de hash dos nós em formato decimal
            // verificada
            ArrayList<BigInteger> list = new ArrayList<BigInteger>();

            for (int i = 0; i < membershipList.size(); i++) {

                String id = membershipList.get(i) + "";

                try {
                    BigInteger hash_number = getSha256fromKey(Integer.valueOf(id));
                    list.add(hash_number);
                    Collections.sort(list);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            return list;
        }

        /**
         * envia mensagem de join para o servico multicast
         */
        public void joinMessage(int joinTcpPort) {
            try {
                // cria a mensagem para enviar para o grupo "TIPO(NODE ou
                // CLIENT)-OPERACAO-NODE_ID-MEMBERSHIP_COUNTER"
                String joinMessage = "NODE-JOIN-" + this.nodeId + "-" + this.membershipCounter + "-" + joinTcpPort + "-" + InetAddress.getLocalHost().getHostAddress();
                byte[] buffer = joinMessage.getBytes();
                // cria um thread para enviar a mensagem
                MessageThread sendMessage = new MessageThread(this, buffer, "Membership");
                // executa o thread de envio utilizando o ClusterManager
                exec.execute(sendMessage);
            } catch (Exception e) {
                System.out.println("Join MultiCast Group error...");
                e.printStackTrace();
            }
        }

    /*------------------------------------------------- Leave Membership -----------------------------------------------------*/

        /**
         * 
         */
        public void leave() {
            //So pode fazer leave se o membership counter for impar o que significa que já faz parte do cluster
            if(membershipCounter%2!=0){
                System.out.println("Node isn't in the cluster");
                return; 
            }

            updateMembershipCounter();
            writeMembershipCounter();

            leaveMessage();

            updateMembershipLog(this.nodeId + "-" + this.membershipCounter);

            if(membershipList.size()>1){
                Float currentNodeAngle = getAngle(getSha256fromKey(nodeId).toString()); //angulo do novo node
                ArrayList<Float> anglesList = getAllAngles(membershipList); //lista com todos os angulos
                Collections.sort(anglesList); //ordena a lista
                Map<Float, Integer> map = getAngleIdMap(membershipList);//id atraves do angulo

                //vai buscar o node acima
                int index = anglesList.indexOf(currentNodeAngle); 
                Float upper;
                if (index == anglesList.size()-1) {
                    upper = anglesList.get(0);
                } else {
                    upper = anglesList.get(index+1);
                }
                int idLower = map.get(upper);//hash do id

                System.out.println("Transfering files to node " + map.get(upper));
                BigInteger hashedID = getSha256fromKey(nodeId);

                File dir = new File("./" + hashedID + "/originals");
                        File[] directoryListing = dir.listFiles();
                        if (directoryListing != null) {
                            for (File child : directoryListing) {
                                BigInteger hashedKeyDecimal = new BigInteger(child.getName().replace(".txt", ""), 16);
                                
                                    try {
                                        DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
                                        String content = new String(Files.readAllBytes(Paths.get("./" + hashedID + "/originals/"+ child.getName())), StandardCharsets.UTF_8);
                                        String value = convertStringToHex(content);
                                        String original_message = "MOVEFILE" + "-" + child.getName().replace(".txt", "") + "-" + value;
                                        // PUT-id-key-value
                                        byte[] msgBytes = original_message.getBytes();
                                        InetAddress addr = InetAddress.getByName("127.0.0.1" + idLower);
                                        int port = 8080;
                                        InetSocketAddress dest = new InetSocketAddress(addr, port);
                                        DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
                                        sender.send(hi);
                                        System.out.println("Movefile message sent to node " + idLower + "| key: " + child.getName());
                                        child.delete();
                                        sender.close();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                
                            }
                        }
            }       
            updateMembershipList();
            clusterManager.stop();
            System.out.println("Node left the cluster");
            
        }
        
        public String convertStringToHex(String str) {
            // verificada
            byte[] getBytesFromString = str.getBytes(StandardCharsets.UTF_8);
            BigInteger bigInteger = new BigInteger(1, getBytesFromString);

            String convertedResult = String.format("%x", bigInteger);

            return (convertedResult);
        }

        public void leaveMessage() {
            try {
                
                String leaveMessage = "NODE-LEAVE-" + this.nodeId + "-" + this.membershipCounter;
                System.out.println("Before sending message counter: " + this.membershipCounter);
                byte[] buffer = leaveMessage.getBytes();
                // cria um thread para enviar a mensagem
                MessageThread sendMessage = new MessageThread(this, buffer, "Membership");
                // executa o thread de envio utilizando o ClusterManager
                exec.execute(sendMessage);
            } catch (Exception e) {
                System.out.println("Leave MultiCast Group error...");
                e.printStackTrace();
            }
        }

    /*--------------------------------------------- Membership Acesso a ficheiros / IO ---------------------------------------*/
        /**
         * retorna o membership log numa string
         */
        public String getMembershipLogString() {
            String log="",line;
            try{
                String hashedDecimalKey= getSha256fromKey(this.nodeId).toString();

                File membersLog = new File(hashedDecimalKey +"/membershipLog.txt");
                BufferedReader reader = new BufferedReader(new FileReader(membersLog));

                while((line = reader.readLine()) != null){
                    log+= line + System.getProperty("line.separator");
                }
                
                reader.close();
            }catch(IOException ex){
                ex.printStackTrace();
            }
            return log;
        }

        /**
         * faz update ao membership log a partir de uma string
         * @param membershipLogMessage
         */
        public void updateMembershipLog(String updatedLog){
            try{
                String hashedDecimalKey= getSha256fromKey(nodeId).toString();
                
                
                File directory = new File("./"+hashedDecimalKey);
                if (! directory.exists()){
                    directory.mkdir();
                }
                File membersLog = new File(hashedDecimalKey +"/membershipLog.txt");
                if(!membersLog.exists()){
                    membersLog.createNewFile();
                }
                
                //Anotar as diferenças entre os logs e atualizar o deste node
                String[] updatedLogArray = updatedLog.split(" ");
                File temp = new File(hashedDecimalKey +"/temp.txt");
                BufferedReader reader = new BufferedReader(new FileReader(membersLog));
                BufferedWriter writer = new BufferedWriter(new FileWriter(temp));
                String line;
                ArrayList<String> logList = new ArrayList<>();

                while((line = reader.readLine()) != null){
                    logList.add(line.trim());
                }

                reader.close();

                for(int i=updatedLogArray.length-1; i>= 0; i--){
                    if(!(logList.contains(updatedLogArray[i]))){
                        logList.add(0,updatedLogArray[i]);
                    }
                }

                for(String s : logList){
                    writer.write(s);
                    writer.newLine();
                }
                writer.close();

                membersLog.delete();
                temp.renameTo(membersLog);
            }catch(Exception e){
                System.out.println("Membership Log file error...");
                e.printStackTrace();
            }
        }

        /**
         * 
         */
        public void updateMembershipList() {
            try{

                String hashedDecimalKey= getSha256fromKey(nodeId).toString();
                
                File directory = new File("./"+hashedDecimalKey);
                if (! directory.exists()){
                    directory.mkdir();
                }
                File membersLog = new File(hashedDecimalKey +"/membershipLog.txt");
                if(!membersLog.exists()){
                    membersLog.createNewFile();
                }

                BufferedReader reader = new BufferedReader(new FileReader(membersLog));
                String line;
                int counter=0;
                ArrayList<Integer> visited = new ArrayList<>();
                while((line = reader.readLine()) != null && counter<32){
                    counter++;
                    String[] currentLine = line.trim().split("-");
                    if(!membershipList.contains(Integer.parseInt(currentLine[0])) && Integer.parseInt(currentLine[1])%2==0 && !visited.contains(Integer.parseInt(currentLine[0]))){
                        membershipList.add(Integer.parseInt(currentLine[0]));
                        visited.add(Integer.parseInt(currentLine[0]));
                    }
                    if(membershipList.contains(Integer.parseInt(currentLine[0])) && Integer.parseInt(currentLine[1])%2!=0 && !visited.contains(Integer.parseInt(currentLine[0]))){
                        membershipList.remove(Integer.valueOf(Integer.parseInt(currentLine[0])));
                        visited.add(Integer.parseInt(currentLine[0]));
                    }
                }
                reader.close();
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }

        /**
         * 
         */
        private void writeMembershipCounter() {
            try{
                String hashedDecimalKey= getSha256fromKey(nodeId).toString();
                
                File directory = new File("./"+hashedDecimalKey);
                if (! directory.exists()){
                    directory.mkdir();
                }
                File memberCounter = new File(hashedDecimalKey +"/membershipCounter.txt");
                if(!memberCounter.exists()){
                    memberCounter.createNewFile();
                }
                
                File temp = new File(hashedDecimalKey +"/temp.txt");
                BufferedWriter bw = new BufferedWriter(new FileWriter(temp));

                bw.write(String.valueOf(membershipCounter));
                bw.close();

                memberCounter.delete();
                temp.renameTo(memberCounter);

            }catch(Exception ex){
                ex.printStackTrace();
            }
        }

        /**
         * 
         */
        public void readMembershipCounter(){
            try{
                String hashedDecimalKey= getSha256fromKey(nodeId).toString();

                File directory = new File("./"+ hashedDecimalKey);
                if (! directory.exists()){
                    directory.mkdir();
                }
                File counter = new File(hashedDecimalKey + "/membershipCounter.txt");
                if(!counter.exists()){
                    membershipCounter=0;
  
                }else{

                    Scanner reader = new Scanner(counter);
                    int number = Integer.parseInt(reader.next());
                    reader.close();
                    membershipCounter= number;
                }
            } catch (Exception e) {
                System.out.println("Membership Counter file error...");
                e.printStackTrace();
            }
        }

    /*------------------------------------------ Storage ---------------------------------------------------------------------*/

        /**
         * 
         * @param key
         * @return
         */
        public BigInteger getSha256fromKey(Integer key) {
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

        /**
         * 
         * @param s
         * @return
         */
        public String getSha256(String s) {
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

        /**
         * 
         * @param packet
         * @return
         */
        public String getRidOfAnnoyingChar(DatagramPacket packet) {
            String result = new String(packet.getData());
            char[] annoyingchar = new char[1];
            char[] charresult = result.toCharArray();
            result = "";
            for (int i = 0; i < charresult.length; i++) {
                if (charresult[i] == annoyingchar[0]) {
                    break;
                }
                result += charresult[i];
            }
            return result;
        }
    }