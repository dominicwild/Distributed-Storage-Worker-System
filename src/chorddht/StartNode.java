package chorddht;

import static chorddht.Utility.log;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  A main method that creates a Chord node and manages commands to it.
 * @author Dominic
 */
public class StartNode {

    static final String RMI_SERVER_NAME = "rmi://localhost:1099/";  
    private ChordNode node;                 //Chord node to boot up.
    private boolean inRing = false;         //Determines if Chord node is in a ring or not.

    public static void main(String[] args) throws AlreadyBoundException {
        boolean worker = true;
        try {
            if(args.length > 0){
                worker = false;
                System.out.println("Storage Node");
            }
            StartNode startNode = new StartNode(worker);
            startNode.commandLoop();
        } catch (InterruptedException ex) {
            Logger.getLogger(StartNode.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    private StartNode(boolean worker) throws InterruptedException, AlreadyBoundException {
        createNode(worker);
    }
    
    /**
     * Main method that creates the node and manages command to it.
     */
    private void createNode(boolean worker) {
        Scanner input = new Scanner(System.in);
        System.out.println("Insert the name of node to create: ");
        String nodeName = input.nextLine();

        if (worker) {
            this.node = new ChordWorkerNode(nodeName); //Worker Node
        } else {
            this.node = new ChordNode(nodeName);
        }
        try { //Register node on RMI registry
            IChordNode chordStub = (IChordNode) UnicastRemoteObject.exportObject(node, 0);
            LocateRegistry.getRegistry().rebind(nodeName, chordStub);
        } catch (RemoteException ex) {
            log("Could not register node within the RMI server.");
        }

        System.out.println("Connect the node (Y/N)?: ");
        String inputString = input.nextLine().toLowerCase();

        if (inputString.equals("y")) {
            System.out.println("Insert the name of the node to join: ");
            inputString = input.nextLine();
            this.joinNode(inputString);
        } else {
            this.inRing = true;
            System.out.println("Idling");
        }
    }

    /**
     * Command loop that handles commands to the node.
     */
    private void commandLoop() {
        Scanner input = new Scanner(System.in);
        String inputString;
        boolean exit = false;
        String[] command;

        while (exit == false) {
            inputString = input.nextLine();
            command = inputString.split(" ");
            if (command.length == 0) {
                continue;
            }
            try {
                switch (command[0].toLowerCase()) {
                    case "join":
                        this.joinNode(inputString.substring(5));
                        break;
                    case "leave":
                        if (command.length > 1) {
                            throw new IllegalArgumentException(); //If more than one argument in command
                        }
                        this.leave();
                        break;
                    case "get":
                        this.get(inputString.substring(4));
                        break;
                    case "put":
                        this.put(inputString.substring(4));
                        break;
                }
            } catch (IllegalArgumentException e) {
                log("Passed to many arguments to command.");
            } catch (Exception ex) {
                log("Invalid command entered.");
                ex.printStackTrace();
            }
        }
        log("Server closing.");
        System.exit(0);
    }

    /**
     * Finds data a specified key and prints it.
     * @param key The key to find the data of in the Chord ring.
     */
    public void get(String key) {
        try {
            byte[] bytes = this.node.get(key);
            log("Data found at key " + key + " was: '" + new String(bytes) + "'");
        } catch (RemoteException ex) {
            log("Could not get resource with key " + key + ".");
        }
    }

    /**
     * Puts sample data into the Chord ring with the given key.
     * @param key The key of the sample data to enter.
     */
    public void put(String key) {
        try {
            this.node.put(key, new String(key + "[DATA]").getBytes());
            log("Successfully put data with key " + key + ".");
        } catch (RemoteException ex) {
            log("Could not put resource with key " + key + ".");
        }
    }

    /**
     * Makes the node leave the Chord ring and safely distributes any remaining data it held onto.
     */
    public void leave() {
        if (!inRing) {
            log("Node is not in a Chord ring to leave.");
        } else {
            this.node.leave();
            this.inRing = false;
            try {
                LocateRegistry.getRegistry().unbind(this.node.getName());
                log("Left the Chord Ring and transferred all data & closing server.");
                System.exit(0);
            } catch (NotBoundException | RemoteException e) {
                log("Unable to unbind node from RMI.");
            }
        }
    }

    /**
     * Joins a Chord Ring via the specified node.
     * @param nameOfNode The node in a Ring to join.
     */
    public void joinNode(String nameOfNode) {
        try {
            IChordNode joinNode = (IChordNode) Naming.lookup(RMI_SERVER_NAME + nameOfNode);
            this.node.join(joinNode);
            this.inRing = true;
            log("Joined node: " + nameOfNode + ".");
        } catch (NotBoundException ex) {
            log(nameOfNode + " does not have an associated RMI binding on server at " + RMI_SERVER_NAME + nameOfNode);
        } catch (MalformedURLException ex) {
            log("Attempted to join node with malformed URL " + RMI_SERVER_NAME + nameOfNode);
        } catch (RemoteException ex) {
            log("Remote exception occurred joining node " + nameOfNode + ".");
            ex.printStackTrace();
        }
    }
}
