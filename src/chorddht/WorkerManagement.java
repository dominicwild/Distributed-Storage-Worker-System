package chorddht;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The remote interface used by the worker to communicate with the management
 * server.
 *
 * @author Dominic
 */
public interface WorkerManagement extends Remote {

    public void notifyReady(Task task) throws RemoteException;

    public boolean ping() throws RemoteException;
    
    public Task take() throws RemoteException, InterruptedException;
}
