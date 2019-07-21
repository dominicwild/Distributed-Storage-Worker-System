package chorddht;

import static chorddht.Utility.log;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

/**
 * A management class that handles the interface between the worker threads in
 * Chord nodes and the management server.
 *
 * @author Dominic
 */
public class WorkerManager implements WorkerManagement {

    private HashMap<Task, Boolean> files; //Reference to a management servers files directory.
    private BlockingQueue taskQueue;      //The list of tasks to process.

    /**
     * Creates an instance of the WorkerManager using the passed files to
     * manage.
     *
     * @param files The HashMap containing the files at the management server to
     * interface with.
     * @param taskQueue The task queue from the management server.
     */
    public WorkerManager(HashMap<Task, Boolean> files, BlockingQueue taskQueue) {
        this.files = files;
        this.taskQueue = taskQueue;
    }

    /**
     * Changes status of the HashMap in the management server to indicate
     * processing on some task is done.
     *
     * @param task The task to notify to the management server is done
     * processing.
     */
    @Override
    public void notifyReady(Task task) throws RemoteException {
        if (task.getType() != TaskType.UNDEFINED) {
            this.files.put(task, true);
        }
        log("Processing finished on task " + task.getFileName());
    }

    /**
     * A simple ping to check if this process is still alive.
     *
     * @return true if the process is alive. If not, an exception will be
     * thrown.
     */
    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    @Override
    public Task take() throws RemoteException, InterruptedException {
        return (Task) this.taskQueue.take();
    }
}
