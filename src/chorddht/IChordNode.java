package chorddht;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * An interface used to allow remote access to ChordNode objects over RMI.
 *
 * @author Dominic
 */
public interface IChordNode extends Remote {

    public IChordNode findSuccessor(int key) throws RemoteException;

    public byte[] get(String key) throws RemoteException;

    public IChordNode put(String key, byte[] value) throws RemoteException;
    
    public IChordNode move(int key, Store store) throws RemoteException;

    public byte[] getStoreBytes(String key, int hashKey) throws RemoteException;

    public int getKey() throws RemoteException;

    public IChordNode closestPrecedingNode(int key) throws RemoteException;

    public void notifyNode(IChordNode potentialPredecessor) throws RemoteException;

    public Finger getPredecessor() throws RemoteException;

    public boolean ping() throws RemoteException;

    public boolean isAlive(Finger node) throws RemoteException;

    public void setPredecessor(Finger predecessor) throws RemoteException;

    public void setSuccessor(Finger successor) throws RemoteException;
    
    public Finger[] getSuccessorList() throws RemoteException;
}
