import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;

public class StorageMessageManager implements Runnable{
    
    private String message;
    private Node node;

    public StorageMessageManager(Node node, String message){
        this.message=message;
        this.node=node;
    }

    @Override
    public void run() {
        try {

            String[] parts = this.message.split("-");
            String instruction = parts[0];

            System.out.println(instruction + " Message received: ");

            if (instruction.equals("PUT")) {
                String id = parts[1];
                String key = parts[2];
                String value = parts[3];
                String flag = parts[4];
                System.out.println("Key: " + key);
                System.out.println("Value: " + getOriginalText(value));
                System.out.println();

                BigInteger decimalKey = new BigInteger(key, 16);
                float angle = getAngle(decimalKey.toString());

                ArrayList<Integer> membershipListCopy = new ArrayList<>(node.getMembershipList());
                // replications = 1 (cópia inicial) + 3 (cópias backup) -- menos se não houver nodes suficientes
                Integer replications = membershipListCopy.size() > 3 ? 4  : membershipListCopy.size();

                for (int i = 0; i < replications; i++) {
                    Integer responsibleId = getResponsibleNode(angle, membershipListCopy);
                    Boolean replica = i == 0 ? false : true;

                    System.out.println("Replica " + i);

                    if (node.getId() == responsibleId) {
                        System.out.println("The current node is responsible for putting the file");
                        saveFile(value, id, key, replica);
                    }
                    if (node.getId() != responsibleId && flag.equals("true")) {
                        System.out.println("The node responsible for putting this file is: " + responsibleId);
                        reSendDatagramPutMessage(responsibleId.toString(), key, value, "false");
                    }

                    membershipListCopy.remove(responsibleId);
                }
                

            } else
            if (instruction.equals("GET")) {
                String key = parts[1];
                InetAddress senderAddress = InetAddress.getByName("127.0.0.1" + parts[2]);
                BigInteger hash_number_decimal = new BigInteger(key, 16);

                Integer responsibleId = getResponsibleNode(getAngle(hash_number_decimal.toString()), node.getMembershipList());

                BigInteger numericId = node.getSha256fromKey(responsibleId);

                String finalLine="RESPONSEGET-";
                if (node.getId() == responsibleId) {
                    System.out.println("The current node is responsible for getting the file");
                    String path = numericId + "/originals/" + key + ".txt";
                    BufferedReader br = new BufferedReader(new FileReader(path));
                    System.out.println("Text file: ");
                    String line;
                    while ((line = br.readLine()) != null) {
                        finalLine+=line;
                        System.out.println(line);
                    }
                    br.close();

                    DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
                    byte[] msgBytes = finalLine.getBytes();
                    int port = 8080;
                    InetSocketAddress dest = new InetSocketAddress(senderAddress, port);
                    DatagramPacket message = new DatagramPacket(msgBytes, msgBytes.length, dest);
                    sender.send(message);
                    sender.close();
                    //System.out.println(finalLine);

                }else
                if (node.getId() != responsibleId) {
                    BigInteger hashedCurrentNodeId = node.getSha256fromKey(node.getId());
                    File replica = new File(hashedCurrentNodeId + "/replica/" + key + ".txt");
                    if(replica.exists()){
                        System.out.println("The current node has a  replica of the file..");
                        BufferedReader br = new BufferedReader(new FileReader(replica));
                        System.out.println("Text file: ");
                        String line;
                        while ((line = br.readLine()) != null) {
                            finalLine+=line;
                            System.out.println(line);
                        }
                        br.close();
                    }else{
                        System.out.println("The node responsible for getting this file is: " + responsibleId);
                        reSendDatagramGetMessage(responsibleId.toString(), key, parts[2]);
                    }
                }

            }else
            if (instruction.equals("DELETE")) {

                String key = parts[1];
                boolean flag = Boolean.parseBoolean(parts[2]);
                if(flag){
                    for(Integer member : node.getMembershipList()){
                        if(!member.equals(node.getId())){
                            reSendDatagramDeleteMessage(String.valueOf(member), key);
                        }
                    }
                }
                System.out.println("Key: " + key);

                BigInteger hash_number_decimal = new BigInteger(key, 16);
                Integer responsibleId = getResponsibleNode(getAngle(hash_number_decimal.toString()), node.getMembershipList());
                System.out.println("The responsible is: " + responsibleId);

                BigInteger numericId = node.getSha256fromKey(responsibleId);

                if (node.getId() == responsibleId) {
                    String path = numericId + "/originals/" + key + ".txt";
                    File myObj = new File(path);
                    if (myObj.delete()) {
                        System.out.println("Deleted the file: " + myObj.getName());
                    } else {
                        System.out.println("Failed to delete the file.");
                    }
                }
                if (node.getId() != responsibleId) {
                    BigInteger hashedCurrentNodeId = node.getSha256fromKey(node.getId());
                    File replica = new File(hashedCurrentNodeId + "/replica/" + key + ".txt");
                    if(replica.exists()){
                        //String path = hashedCurrentNodeId + "/replica/" + key + ".txt";
                        if (replica.delete()) {
                            System.out.println("Deleted the file replica: " + replica.getName());
                        } else {
                            System.out.println("Failed to delete the file replica.");
                        }
                    }else{
                        System.out.println("No replicas found..");
                    }
                }
            } else 
            if(instruction.equals("RESPONSEGET")){
                String value = new String(parts[1]);
                System.out.println("File content: " + value);
            }
            if(instruction.equals("MOVE")){
                String senderNodeID = parts[1];
                Float senderNodeAngle = Float.parseFloat(parts[3]);
                
                //InetAddress senderAddress = InetAddress.getByName("127.0.0.1" + parts[2]);
                BigInteger hashedID = node.getSha256fromKey(node.getId());
                BigInteger hashedSenderId = node.getSha256fromKey(Integer.parseInt(senderNodeID));

                File dir = new File("./" + hashedID + "/originals");
                File[] directoryListing = dir.listFiles();
                if (directoryListing != null) {
                    for (File child : directoryListing) {
                        BigInteger hashedKeyDecimal = new BigInteger(child.getName().replace(".txt", ""), 16);
                        if(getAngle(hashedKeyDecimal.toString())<senderNodeAngle){
                            try {
                                DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
                                String content = new String(Files.readAllBytes(Paths.get("./" + hashedID + "/originals/"+ child.getName())), StandardCharsets.UTF_8);
                                String value = node.convertStringToHex(content);
                                String original_message = "MOVEFILE" + "-" + child.getName().replace(".txt", "") + "-" + value;
                                // PUT-id-key-value
                                byte[] msgBytes = original_message.getBytes();
                                InetAddress addr = InetAddress.getByName("127.0.0.1" + senderNodeID);
                                int port = 8080;
                                InetSocketAddress dest = new InetSocketAddress(addr, port);
                                DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
                                sender.send(hi);
                                System.out.println("Movefile message sent to node " + senderNodeID);
                                child.delete();
                                sender.close();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } else {
                    System.out.println("No files in directory");
                }

            }else
            if(instruction.equals("MOVEFILE")){
                String key = parts[1];
                String value= parts[2];
                System.out.println("Key:" + parts[1]);
                saveFile(value, String.valueOf(node.getId()), key, false);
            }


        }catch (Exception exception) {
            exception.printStackTrace();
        }
        
    }

    public void reSendDatagramPutMessage(String nodeId, String key, String value, String flag) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
            String original_message = "PUT-" + nodeId + "-" + key + "-" + value + "-" + flag;
            // PUT-id-key-value
            byte[] msgBytes = original_message.getBytes();
            InetAddress mcastaddr = InetAddress.getByName("127.0.0.1" + nodeId);
            int port = 8080;
            InetSocketAddress dest = new InetSocketAddress(mcastaddr, port);
            DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
            sender.send(hi);
            System.out.println("Put message sent to node " + nodeId );
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public void reSendDatagramGetMessage(String nodeId, String key, String originalNodeId) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
            String original_message = "GET-" + key + "-" + originalNodeId;
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

    public void reSendDatagramDeleteMessage(String nodeId, String key) {
        try {
            DatagramSocket sender = new DatagramSocket(new InetSocketAddress(0));
            String original_message = "DELETE-" + key + "-" + false;
            // GET-key
            byte[] msgBytes = original_message.getBytes();
            InetAddress mcastaddr = InetAddress.getByName("127.0.0.1" + nodeId);
            int port = 8080;
            InetSocketAddress dest = new InetSocketAddress(mcastaddr, port);
            DatagramPacket hi = new DatagramPacket(msgBytes, msgBytes.length, dest);
            sender.send(hi);
            System.out.println("Delete message sent to node " + nodeId );
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    public Map<BigInteger, Integer> getHashIdMap(ArrayList<Integer> membershipList) {
        // verificado
        Map<BigInteger, Integer> map = new HashMap<BigInteger, Integer>();
        // map.put(BigInteger hashed_id,Integer original id);
        // map.get(hashed_id )-> original_id

        for (int i = 0; i < membershipList.size(); i++) {

            String id = membershipList.get(i) + "";

            try {
                map.put(node.getSha256fromKey(Integer.parseInt(id)), Integer.parseInt(id));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return map;
    }

    public Map<Float, Integer> getAngleIdMap(ArrayList<Integer> membershipList) {
        // verificado
        // map com angulos e ids dos nos da lista
        Map<Float, Integer> map = new HashMap<Float, Integer>();
        // map.put(BigInteger hashed_id,Integer original id);
        // map.get(hashed_id )-> original_id

        for (int i = 0; i < membershipList.size(); i++) {

            String id = membershipList.get(i) + "";

            try {
                map.put(getAngle(node.getSha256fromKey(Integer.parseInt(id)).toString()), Integer.parseInt(id));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return map;
    }

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

    public ArrayList<BigInteger> getHashedIdList(ArrayList<Integer> membershipList) {
        // lista de hash dos nós em formato decimal
        // verificada
        ArrayList<BigInteger> list = new ArrayList<BigInteger>();

        for (int i = 0; i < membershipList.size(); i++) {

            String id = membershipList.get(i) + "";

            try {
                BigInteger hash_number = node.getSha256fromKey(Integer.valueOf(id));
                list.add(hash_number);
                Collections.sort(list);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return list;
    }

    public ArrayList<Float> getAllAngles(ArrayList<Integer> membershipList) {
        // verificada
        // lista ordenada de todos os angulos de nós
        ArrayList<Float> list = new ArrayList<Float>();

        for (int i = 0; i < membershipList.size(); i++) {

            String id = membershipList.get(i) + "";

            try {
                BigInteger hash_number = node.getSha256fromKey(Integer.valueOf(id));
                list.add(getAngle(hash_number.toString()));
                Collections.sort(list);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return list;
    }

    public String getOriginalText(String value) {
        //byte[] bytes = HexFormat.of().parseHex(value);
        //return new String(bytes, StandardCharsets.UTF_8);

        int l = value.length();
        byte[] data = new byte[l / 2];
        for (int i = 0; i < l; i += 2) {
            data[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4)+ Character.digit(value.charAt(i + 1), 16));
        }
        return new String(data);
        
    }

    public void saveFile(String value, String id, String key, Boolean replica) {

        try {
            String originalText = (getOriginalText(value));
            System.out.println("File content: ");
            System.out.println(originalText + "/n");
            System.out.println();

            String path = replica ? node.getSha256fromKey(Integer.valueOf(id)) + "/replica/" + key + ".txt" : node.getSha256fromKey(Integer.valueOf(id)) + "/originals/" + key + ".txt";
            //String path = getSha256fromKey(Integer.valueOf(id)) + "/" + key + ".txt";
            File f = new File(path);
            f.getParentFile().mkdirs();
            Files.write(Paths.get(path), originalText.getBytes());
        } catch (IOException e) {

            e.printStackTrace();
        }

        System.out.println("This file was saved");

    }

    public Integer getResponsibleNode(float keyAngle, ArrayList<Integer> memberList) {
        // verificada
        // gets node with angle imediately above the parameter

        Integer id = 0;
        Map<Float, Integer> map = getAngleIdMap(memberList);
        ArrayList<Float> angles = getAllAngles(memberList);
        angles.add(keyAngle);
        Collections.sort(angles);
        // puts passed value in list of all angles of all nodes and sorts

        int index = angles.indexOf(keyAngle);
        Float chosenAngle;
        if (index == angles.size() - 1) {
            chosenAngle = angles.get(0);
        } else {
            chosenAngle = angles.get(index + 1);
        }
        // picks the angle immediately above. if none, picks first

        id = map.get(chosenAngle);
        return id;
    }

}




