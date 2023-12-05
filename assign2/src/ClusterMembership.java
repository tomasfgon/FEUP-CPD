import java.io.*;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClusterMembership extends Remote{
    void join() throws RemoteException;
    void leave() throws RemoteException;
}