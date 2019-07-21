package chorddht;

import java.rmi.RemoteException;

/**
 * A Chord node that processes tasks, instead of just simply storing data.
 *
 * @author Dominic
 */
public class ChordWorkerNode extends ChordNode implements Runnable {
    
    private Worker worker;                      //The worker object that processes the requests.
    private Thread workerThread;                //The thread object the worker runs on.

    /**
     * A Chord node that has the capacity to process requests it gets in its
     * store.
     *
     * @param myKeyString The name of the node, that a key will be generated
     */
    public ChordWorkerNode(String myKeyString) {
        super(myKeyString);
        this.worker = new Worker(this);
        this.workerThread = new Thread(this.worker);
        this.workerThread.start();
    }
    
}
