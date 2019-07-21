package tasksubmissionsystem;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The remote interface used to send requests to the management server.
 *
 * @author Dominic
 */
public interface RESTInterface extends Remote {

    public void putTask(String fileName, byte[] bytes, String type) throws RemoteException;

    public String fileList() throws RemoteException;

    public byte[] getResults(String fileName, String taskType) throws RemoteException;
}
