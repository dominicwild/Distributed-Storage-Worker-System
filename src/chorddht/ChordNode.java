package chorddht;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A node to operate in a DHT peer to peer networking running the Chord
 * protocol.
 *
 * @author Dominic
 */
class ChordNode implements Runnable, IChordNode {

    static final int KEY_BITS = 8;              //Amount of bits to use for generation of the hash key. Also determines size of routing table.
    static final int STABLE_DELAY = 2000;       //Delay to wait until a new joined node begins taking put and get requests.
    static final int SUCESSOR_LIST_SIZE = 2;    //The number of successors the successor list will contain.
    private String name;                        //The name of the node that a key is generated in the hashing function.

    private volatile Finger predecessor;        //This node with next least value in the ring, in relation to this node.

    private int routeTableLength;               //Length of the routing table this node uses.
    private Finger[] routingTable;              //Table containing nodes to be used for routing requests through the ring.
    private int nextFix;                        //Determines the next routing table entry to check the correctness of.
    private int nextSuccessCheck;               //Determines the next successor list entry to check the correctness of.

    private TreeMap<Integer, Store> dataStore = new TreeMap<>();    //Stores all DHT byte array data, in an ordered fashion.
    private int key;                                                //The hashed key of this node.
    private Finger[] successorList;                                 //Holds list of r next successors.

    volatile boolean stable = true;                                 //Represents if the node is in a stable state to accept requests for get and put.

    /**
     * Creates a Chord node given a string to form a key from.
     *
     * @param myKeyString The name of the node, that a key will be generated
     * from.
     */
    public ChordNode(String myKeyString) {
        this.name = myKeyString;
        this.key = Utility.hash(myKeyString);
        log("Key assigned is: " + key);
        this.nextFix = -1;
        this.nextSuccessCheck = SUCESSOR_LIST_SIZE;
        this.successorList = new Finger[SUCESSOR_LIST_SIZE];

        //Initialise finger table. Fill all initial values with this node, to avoid dealing with nulls.
        routingTable = new Finger[KEY_BITS];
        for (int i = 0; i < KEY_BITS; i++) {
            routingTable[i] = new Finger(this);
        }
        this.routeTableLength = KEY_BITS;

        this.routingTable[0] = new Finger(this);

        //Start up the periodic maintenance thread
        new Thread(this, myKeyString).start();
    }

    /**
     * Adds the key, value pair to the corresponding responsible node that has
     * the duty of storing data for that particular key.
     *
     * @param key The key of the data to store.
     * @param value The value, or bytes of the data to store.
     */
    @Override
    public IChordNode put(String key, byte[] value) throws RemoteException {
        while (this.stable == false); //Wait until stabilized to conduct store requests.
        int hashKey = Utility.hash(key); //First see if key is in our range that we're responsible for
        if (this.isInHalfOpenRangeR(hashKey, this.predecessor.getKey(), this.key)) { //If so, add to our store.
            synchronized (this.dataStore) {
                if(Objects.isNull(this.dataStore.get(hashKey))){ //If null, make store and add initial entry.
                    this.dataStore.put(hashKey, new Store(hashKey,key, value));
                } else { //If not null, add to existing store.
                    this.dataStore.get(hashKey).put(key, value);
                }
            }
            log("Placed in data with key: " + hashKey);
            return this;
        } else { //If not find the node which is responsible and put it there.
            IChordNode responsibleNode = this.findSuccessor(hashKey);
            return responsibleNode.put(key, value);
        }
    }

    @Override
    public IChordNode move(int key, Store store) throws RemoteException {
        while (this.stable == false); //Wait until stabilized to conduct store requests.
        if (this.isInHalfOpenRangeR(key, this.predecessor.getKey(), this.key)) { //If so, add to our store.
            synchronized (this.dataStore) {
                this.dataStore.put(key, store);
            }
            log("Placed in data with key: " + key);
            return this;
        } else { //If not find the node which is responsible and put it there.
            IChordNode responsibleNode = this.findSuccessor(key);
            return responsibleNode.move(key, store);
        }
    }
    
    /**
     * Runs a query to find the resource the given key references and returns
     * that resource.
     *
     * @param key The key of the resource to return.
     * @return The resource the key references.
     */
    public byte[] get(String key) throws RemoteException {
        int keyHash = Utility.hash(key);
        byte[] store = this.getStoreBytes(key, keyHash);
        if (store != null) { //If we had the resource, return it.
            return store;
        } else { //If not, find the node that does and return it.
            IChordNode ownerNode = this.findSuccessor(keyHash);
            return ownerNode.getStoreBytes(key, keyHash);
        }
    }

    /**
     * References local store to get a specific requested resource with the
     * given key. If the resource cannot be found on this node, return null.
     *
     * @param key The key of the resource we have.
     * @return The store that the key references. If it is not present, then
     * null is returned.
     */
    @Override
    public byte[] getStoreBytes(String key, int hashKey) throws RemoteException {
        if (this.dataStore.containsKey(hashKey)) {
            return this.dataStore.get(hashKey).getValue(key);
        } else {
            return null;
        }
    }

    /**
     * Makes this node join a Chord ring of nodes. The node joins the passed
     * nodes ring.
     *
     * @param nodeToJoin The node whose ring is desired to be joined.
     */
    void join(IChordNode nodeToJoin) throws RemoteException {
        this.predecessor = null;    //Predecessor now unknown when entering a new ring. A node will inform us of this value.
        Finger successor = new Finger(nodeToJoin.findSuccessor(this.key));
        for (int i = 0; i < this.routingTable.length; i++) { //Assign successor to all routing table entries to avoid null
            this.routingTable[i] = successor;
        }
        for (int i = 0; i < this.successorList.length; i++) {
            this.successorList[i] = successor;
        }
        this.stableDelay();
    }

    /**
     * A delay that takes place to ensure this node is stable.
     */
    private void stableDelay() {
        this.stable = false;
        try {
            Thread.sleep(STABLE_DELAY);
        } catch (InterruptedException ex) {
            Logger.getLogger(ChordNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.stable = true;
    }

    /**
     * Finds the successor node to the given key.
     *
     * @param key The key to find the successor node of.
     * @return The node which is responsible for storing the passed key.
     */
    @Override
    public IChordNode findSuccessor(int key) throws RemoteException {
        Finger successor = this.getImmediateSuccessor();
        if (successor != null) {
            //If key is in range exclusive left of us, then key is managed by our successor.
            if (this.isInHalfOpenRangeR(key, this.key, successor.getKey())) {
                return successor.getNode();
            } else { //Otherwise forward onto closest preceding node we know to conduct a further search
                IChordNode closestPrecedessor = successor.getNode().closestPrecedingNode(key);
                return closestPrecedessor.findSuccessor(key);
            }
        } else {
            log("No successor currently assigned to conduct findSuccessor()");
            return null;
        }
    }

    /**
     * Checks if a given node is alive via its Finger entry.
     *
     * @param node The node to ping and check if is still active.
     * @return A boolean representing if the node is still active and reachable
     * on the network.
     */
    @Override
    public boolean isAlive(Finger node) throws RemoteException {
        boolean ping = false;
        try { //A ping attempt will end in an exception if its unreachable.
            ping = node.getNode().ping();
        } catch (Exception e) {
            log("Failed to ping node with key " + node.getKey() + ".");
        }
        return ping;
    }

    /**
     * Finds the closest node preceeding the given key value. The opposite of
     * finding a successor to a key.
     *
     * @param key The key to find the closest preceeding node of.
     * @return The node that precedes the key value.
     */
    @Override
    public IChordNode closestPrecedingNode(int key) throws RemoteException {
        for (int i = this.routeTableLength - 1; i >= 0; i--) {
            try { //Look through routing table in order of the most far away nodes first, to achieve fastest search time.
                if (isAlive(this.routingTable[i]) && this.isInClosedRange(this.routingTable[i].getKey(), this.key, key)) {
                    return this.routingTable[i].getNode();
                }
            } catch (Exception e) {
                log("Corrupt or empty entry in routing table position: " + i);
            }
        }
        return this;
    }

    /**
     * Checks if key is in range [a,b] of a space that wraps. All inclusive.
     *
     * @param key Value to check in range of.
     * @param a The lower bound.
     * @param b The upper bound.
     * @return A Boolean if key is within the range or not.
     */
    boolean isInOpenRange(int key, int a, int b) {
        if (b > a) {
            return key >= a && key <= b;
        } else {
            return key >= a || key <= b;
        }
    }

    /**
     * Checks if key is in range (a,b) of a space that wraps. All exclusive.
     *
     * @param key Value to check in range of.
     * @param a The lower bound.
     * @param b The upper bound.
     * @return A Boolean if key is within the range or not.
     */
    boolean isInClosedRange(int key, int a, int b) {
        if (b > a) {
            return key > a && key < b;
        } else {
            return key > a || key < b;
        }
    }

    /**
     * Checks if key is in range [a,b) of a space that wraps. Exclusive right.
     *
     * @param key Value to check in range of.
     * @param a The lower bound.
     * @param b The upper bound.
     * @return A Boolean if key is within the range or not.
     */
    boolean isInHalfOpenRangeL(int key, int a, int b) {
        if (b > a) {
            return key >= a && key < b;
        } else {
            return key >= a || key < b;
        }
    }

    /**
     * Checks if key is in range (a,b] of a space that wraps. Exclusive left.
     *
     * @param key Value to check in range of.
     * @param a The lower bound.
     * @param b The upper bound.
     * @return A Boolean if key is within the range or not.
     */
    boolean isInHalfOpenRangeR(int key, int a, int b) {
        if (b > a) {
            return key > a && key <= b;
        } else {
            return key > a || key <= b;
        }
    }

    /**
     * Checks if a particular node is our predecessor. Used by nodes to ensure
     * consistency in the network.
     *
     * @param potentialPredecessor The node to verify is our predecessor or not.
     * Usually the sending node.
     */
    public void notifyNode(IChordNode potentialPredecessor) throws RemoteException {
        //If we have no predecessor, this is our best guess for a predecessor.
        //If our potential predecessor node is between what we believe is our predecessor and us, then this node is a more accurate predecessor.
        if (this.predecessor == null || this.isInClosedRange(potentialPredecessor.getKey(), this.predecessor.getKey(), this.key)) {
            this.predecessor = new Finger(potentialPredecessor);
        }
    }

    /**
     * Periodically called to verify that this nodes successor, thinks that this
     * node is its predecessor. Checking for consistency.
     */
    void stabilise() throws RemoteException {
        Finger successor = this.getImmediateSuccessor();
        Finger successorsPredecessor = successor.getNode().getPredecessor();
        if (successorsPredecessor != null) {
            int nodeKey = successorsPredecessor.getKey(); //Successors predecessor's key
            if (this.isInClosedRange(nodeKey, this.key, successor.getKey())) {
                this.routingTable[0] = successorsPredecessor; //If this nodes key is in the exclusive range, then its not our key (our successor has a new predecessor), so its our new successor.
            }
        }
        successor.getNode().notifyNode(this); //Make our successor check, that we're its predecessor. For itself to perform a sanity check.
    }

    /**
     * Maintains accuracy and correctness of the routing table. It is run
     * periodically by the maintenance thread.
     */
    void fixFingers() throws RemoteException {
        this.nextFix++; //Next entry to fix.
        if (nextFix >= this.routeTableLength) {
            this.nextFix = 0;
        }
        int key = this.key + (int) Math.pow(2, this.nextFix);
        IChordNode entry = this.findSuccessor(key);
        this.routingTable[this.nextFix] = new Finger(entry);
    }

    /**
     * Checks if predecessor is still responding and has not crashed.
     */
    void checkPredecessor() throws RemoteException {
        if (this.predecessor != null && !isAlive(this.predecessor)) {
            this.predecessor = null;
        }
    }

    /**
     * Pings this node.
     *
     * @return A boolean with the value of true.
     */
    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    /**
     * Check if data needs to moved to our current predecessor, if so, move it.
     * Procedure run periodically by the maintenance thread.
     */
    void checkDataMoveDown() throws RemoteException {
        if (this.predecessor != null && this.predecessor.getKey() != this.key) { //If there is a predecessor to move to and it's not the same node as the one running this function.
            SortedMap<Integer, Store> toMove;
            Iterator<Entry<Integer, Store>> iter; //Used for removing values safetly from underlying collection.
            synchronized (this.dataStore) {
                if (this.predecessor.getKey() > this.key) { //Handles wrap around range.
                    toMove = this.dataStore.subMap(key, false, this.predecessor.getKey(), true); //Range from exclusive key, to inclusive predecessor key.
                } else {
                    toMove = this.dataStore.tailMap(key, false); //Greater than exclusive to key.
                    SortedMap<Integer, Store> lessThan = this.dataStore.headMap(this.predecessor.getKey(), true); //Less than inclusive to predecessor key.
                    iter = lessThan.entrySet().iterator();
                    moveData(iter, this.predecessor); //Move less than data before exit due to SortedMap contiguous key range restrictions.
                }
                iter = toMove.entrySet().iterator();
                moveData(iter, this.predecessor);
                //Remove from our store, as data has been moved.
            }
        }
    }

    /**
     * Moves data in the given iterator to the passed node.
     *
     * @param iter The iterator containing the data to move.
     * @param moveToNode The node to move the data to.
     */
    private void moveData(Iterator<Entry<Integer, Store>> iter, Finger moveToNode) throws RemoteException {
        while (iter.hasNext()) { //Steps through the data and moves it.
            Entry moveEntry = iter.next();
            Store moveStore = (Store) moveEntry.getValue();
            moveToNode.getNode().move(moveStore.getKey(), moveStore); //Put key and value into necessary node
            log("Moved data with ID " + moveEntry.getKey() + " to " + moveToNode.getKey());
            iter.remove();
        }
    }

    /**
     * Run periodically to maintain correctness of entries in successor list.
     */
    private void maintainSuccessorList() throws RemoteException {
        this.nextSuccessCheck++;    //The entry to check on this excution of the function.
        if (this.nextSuccessCheck >= this.successorList.length) {
            this.nextSuccessCheck = 0;
            int key = this.routingTable[this.nextSuccessCheck].getKey(); //Immediate successor's key for this node
            this.successorList[this.nextSuccessCheck] = new Finger(this.findSuccessor(key + 1)); //Finds the successor, of our successor
        } else {
            int key = this.successorList[this.nextSuccessCheck - 1].getKey(); //Key of successor to check
            IChordNode nextSuccessor = this.findSuccessor(key + 1);
            this.successorList[this.nextSuccessCheck] = new Finger(nextSuccessor);
        }
    }

    /**
     * Checks if the current assigned successor is active and reachable. If not
     * replaces it with the next entry on the successor list. Run periodically
     * in the maintenance thread.
     */
    private void checkSuccessorStatus() throws RemoteException {
        if (!this.isAlive(this.routingTable[0])) {
            this.routingTable[0] = this.successorList[0];
            for (int i = 0; i < this.successorList.length - 1; i++) {
                this.successorList[i] = this.successorList[i + 1];
            }
        }
    }

    /**
     * Gets the immediate successor of this node. This function attempts a best
     * effort to not return a null successor, otherwise core functions of the
     * node can't operate correctly.
     *
     * @return The successor of this node.
     */
    private Finger getImmediateSuccessor() throws RemoteException {
        if (!this.isAlive(this.routingTable[0])) {
            for (int i = 0; i < this.successorList.length; i++) {
                if (this.isAlive(this.successorList[i])) { //Start checking all entries in successor list until one that is alive is found.
                    return this.successorList[i];
                }
            }
        }
        return this.routingTable[0];
    }

    /**
     * The maintenance thread.
     */
    public void run() {
        while (true) {
            if (this.routingTable[0] != null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted");
                }

                try {
                    stabilise();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    fixFingers();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    checkPredecessor();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    maintainSuccessorList();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    checkSuccessorStatus();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    checkDataMoveDown();
                } catch (Exception e) {
                    log("Error moving data down.");
                }
            }
        }
    }

    /**
     * Gets the key for this node.
     *
     * @return The key for this node.
     */
    @Override
    public int getKey() {
        return key;
    }

    /**
     * Gets the predecessor Finger object for this node.
     *
     * @return The Finger of this nodes predecessor.
     */
    @Override
    public Finger getPredecessor() throws RemoteException {
        return predecessor;
    }

    /**
     * Creates a generic log message for actions taken on the node.
     *
     * @param msg The message to print in the terminal.
     */
    public void log(String msg) {
        System.out.println(Thread.currentThread().getName() + "[" + this.key + "]: " + msg);
    }

    /**
     * Triggers a node to safely leave the Chord ring it is within.
     */
    public void leave() {
        try {
            stable = false; //Stops all incoming put requests
            Finger successor = this.getImmediateSuccessor();
            //Change successor and predecessor values such that they think this node does not exist.
            successor.getNode().setPredecessor(predecessor);
            this.predecessor.getNode().setSuccessor(successor);
            log("Left Chord Ring. Preceeding to move data.");

            Iterator data = this.dataStore.entrySet().iterator();
            moveData(data, successor);
            log("Successfully left Chord Ring and moved data.");
        } catch (Exception e) {
            log("Failed to leave the Chord Ring and/or move data.");
        }
    }

    /**
     * Changes the successor of this node to given the successor.
     *
     * @param successor The successor to change this node successor to.
     */
    @Override
    public void setSuccessor(Finger successor) throws RemoteException {
        this.routingTable[0] = successor;
    }

    /**
     * Changes the predecessor of this node to given the predecessor.
     *
     * @param predecessor The predecessor to change this node predecessor to.
     */
    @Override
    public void setPredecessor(Finger predecessor) throws RemoteException {
        this.predecessor = predecessor;
    }

    /**
     * Gets the name of this node.
     *
     * @return The name this node was assigned.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the list of successors that this node is storing.
     *
     * @return The list of successors this node contains.
     */
    public Finger[] getSuccessorList() throws RemoteException {
        return successorList;
    }

}
