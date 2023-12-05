

public class MessageThread implements Runnable{
    private byte[] message;
    private String service;
    private Node node;

    public MessageThread(Node node, byte[] message, String service) {
        this.node = node;
        this.message = message;
        this.service = service;
    }

    public void run() {
        switch (service) {
            case "Membership":
                node.getClusterManager().sendMessage(this.message);
                System.out.println(new String(this.message));
                break;
            /*case "Storage":
                Node.getStorageManager().sendMessage(this.message);
                break;*/
        }
    }    
}
