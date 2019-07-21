package chorddht;

import static chorddht.StartNode.RMI_SERVER_NAME;
import static chorddht.Utility.log;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import tasksubmissionsystem.RESTInterface;
import tasksubmissionsystem.RESTServer;

/**
 * An object responsible for interacting with the front end REST requests and forwarding them to the back-end DHT server. 
 * @author Dominic
 */
public class ManagementServer implements RESTInterface, Runnable{
    
    private HashMap<Task,Boolean> files;            //Holds a object describing a task to be carried out and a corressponding Boolean indicating if processing is finished.
    private IChordNode node;                        //The node used to interact with the DHT back end.
    private Finger[] successorList;                 //The successor list of the node used as a gateway into DHT. Used incase the node itself fails.
    private BlockingQueue taskQueue;                //The list of tasks to process.
    boolean fileStoreChange = false;                //Indicates if there was a change in the file store.
    //Various constants used for formatting and generation of responses for the REST requests.
    public static String PREFIX_DOMAIN = "http://localhost:8080/myapp/rest/";
    public static String HTML_FOLDER = "G:\\Program Files\\Apache Software Foundation\\Tomcat 8.5\\webapps\\myapp\\";
    private static String HTML_RED_COLOR_CODE = "#FEA28E";
    private static String HTML_GREEN_COLOR_CODE = "#B1FF70";
    private static String HTML_GREY_COLOR_CODE = "#E3E4EF";
    private static String OBJECT_LIST_FILE_NAME = "list.tmp";   //Name of backup file.
    private static int TASK_CAPACITY = 255;                     //Max amount of requests that this node can hold.
    private static int MISSING_TIME_LIMIT = 60 * 1000;          //In milliseconds. Time for task to determined not in the DHT ring anymore.
    private static int LOOP_INTERVAL = 1000;                    //In milliseconds. Delay between loops in maintenance thread.


    /**
     * Makes a new management server object.
     */
    public ManagementServer() {
        this.files = new HashMap<>();
        this.taskQueue = new ArrayBlockingQueue<>(TASK_CAPACITY);
        this.loadFileList();
    }
    
    public static void main(String[] args){
        ManagementServer server = new ManagementServer();
        
        try { //Register server on RMI registry
            RESTInterface serverStub = (RESTInterface)server;
            serverStub = (RESTInterface) UnicastRemoteObject.exportObject(serverStub, 0);
            LocateRegistry.getRegistry().rebind("RESTManagement", serverStub);      //Interface for REST clients.
            WorkerManagement workerStub = new WorkerManager(server.getFiles(),server.getTaskQueue());
            workerStub = (WorkerManagement) UnicastRemoteObject.exportObject(workerStub, 0);
            LocateRegistry.getRegistry().rebind("WorkerManagement", workerStub);    //Interface for worker clients.
        } catch (RemoteException ex) {
            System.out.println("Could not register server within the RMI server.");
            ex.printStackTrace();
        }
        String command;
        String inputString;
        String parameters;
        int pos;
        Scanner input = new Scanner(System.in);
        server.connectToNodePrompt();
        new Thread(server).start(); //Start maintenance thread.
        while(true){ //Simple UI loop
            inputString = input.nextLine();
            pos = inputString.indexOf(' ');
            if(pos == -1){
                pos = inputString.length();
            }
            command = inputString.substring(0,pos);
            parameters = inputString.substring(pos).trim();
            switch(command){
                case"connect":
                    server.connectToNode(parameters);
                    break;
                case "test":
                    server.test();
                    break;
                case "empty":
                    server.emptyList();
                    break;
                case "list":
                    server.showList();
                    break;
            }
        }
    }

    /**
     * Saves file list object to disk.
     */
    private void backUpFileList(){
        try (ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(OBJECT_LIST_FILE_NAME))){
            output.writeObject(this.files);
            output.flush();
        } catch (FileNotFoundException ex) {
            //Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Loads file list object from disk.
     */
    private void loadFileList() {
        File file = new File(OBJECT_LIST_FILE_NAME);
        if (file.exists()) {
            try (ObjectInputStream input = new ObjectInputStream(new FileInputStream(OBJECT_LIST_FILE_NAME))) {
                this.files = (HashMap<Task, Boolean>) input.readObject();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException | ClassNotFoundException ex) {
                Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * Performs a simple test Task input for the DHT back end system to process. Used for debugging.
     */
    public void test(){
        try {
            RESTInterface toTest = (RESTInterface)Naming.lookup("rmi://localhost:1099/RESTManagement");
            toTest.putTask("aName", "SomeString".getBytes(), TaskType.WORD_METRICS.toString());
            toTest.putTask("aName", "SomeString".getBytes(), TaskType.ENCRYPT.toString());
            toTest.putTask("aName", "SomeString".getBytes(), TaskType.HASH.toString());
        } catch (NotBoundException | MalformedURLException | RemoteException ex) {
            Logger.getLogger(RESTServer.class.getName()).log(Level.SEVERE, "No management server found to process request.", ex);
            ex.printStackTrace();
        }
    }
    
    /**
     * Triggers the set of prompts to connect a node and then connects to the specified node.
     */
    public void connectToNode(String nodeName){
        try {
            this.node = (IChordNode) Naming.lookup(RMI_SERVER_NAME + nodeName);
            this.successorList = this.node.getSuccessorList();
            log("Connected to node " + nodeName);
        } catch (NotBoundException | MalformedURLException | RemoteException ex) {
            log("Failed to connect to server. Server may not exist or RMI registry not running.");
            //Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, "Could not apply worker node to management server.", ex);
        }
    }
    
    /**
     * Triggers the set of prompts to connect a node and then connects to the specified node.
     */
    public void connectToNodePrompt(){
        System.out.println("Insert the name of the node to connect to: ");
        Scanner input = new Scanner(System.in);
        String nodeName = input.nextLine();
        connectToNode(nodeName);
    }

    /**
     * Used by the REST interface to put tasks within the DHT back end to process.
     * @param fileName The name of the resource uploaded to the DHT.
     * @param bytes The bytes of the resource.
     * @param typeString The type of requests to conduct on the resource.
     */
    @Override
    public void putTask(String fileName, byte[] bytes, String typeString) throws RemoteException {
        TaskType taskType = TaskType.stringToType(typeString);
        Task task = new Task(fileName, taskType);
        if (this.submitTask(task.requestName(), bytes)) { //If putting task in DHT is successful
            this.files.put(task, false); //Puts into local file directory log
            this.taskQueue.add(task);
            log("Put task " + fileName + " with type " + typeString);
            this.fileStoreChange = true;
        }
    }
    
    public boolean submitTask(String key, byte[] value){
        try{
            this.node.put(key, value);
            return true;
        } catch (RemoteException ex) {
            log("Unable to put task.");
            return false;
        }
    }

    /**
     * Returns list of all files in system, along with their statuses, if
     * processing is done etc. Returns HTML formatted table.
     *
     * @return A string formatted in HTML of the current files and their
     * statuses in the system.
     */
    @Override
    public String fileList() throws RemoteException {
        StringBuilder html = new StringBuilder();
        //Write basic tags for HTML document and tags for generation of the table.
        html.append("<html>").append(System.lineSeparator()).append(this.readFile("tableCSS.css") + System.lineSeparator());
        html.append("<table>" + System.lineSeparator());
        html.append("<tr bgcolor='" + HTML_GREY_COLOR_CODE + "'>" + System.lineSeparator());
        html.append("<td>File Name" + "</td>" + System.lineSeparator());
        html.append("<td>Task Type" + "</td>" + System.lineSeparator());
        html.append("<td>Done Processing" + "</td>" + System.lineSeparator());
        html.append("<td>Link to Download" + "</td>" + System.lineSeparator());
        html.append("</tr>" + System.lineSeparator());

        String colorCode;   //Colour of the table row being generated.
        String resultLink;  //Link to the resulting file, if available.
        for (Task t : this.files.keySet()) {
            if (this.files.get(t) && t.getMissingTime() == 0) {
                colorCode = HTML_GREEN_COLOR_CODE;
                resultLink = PREFIX_DOMAIN + "files/Results/" + t.getType() + "/" + t.getFileName();
                resultLink = "<a href='" + resultLink + "'>" + resultLink + "</a>";
            } else {
                if (t.getMissingTime() > 0) {
                    colorCode = HTML_GREY_COLOR_CODE;
                    resultLink = "Currently Missing";
                } else {
                    colorCode = HTML_RED_COLOR_CODE;
                    resultLink = "N/A";
                }
            }
            html.append("<tr bgcolor='" + colorCode + "'>" + System.lineSeparator());
            html.append("<td>" + t.getFileName() + "</td>" + System.lineSeparator());
            html.append("<td>" + t.getType() + "</td>" + System.lineSeparator());
            html.append("<td>" + this.files.get(t) + "</td>" + System.lineSeparator());
            html.append("<td>" + resultLink + "</td>" + System.lineSeparator());
            html.append("</tr>" + System.lineSeparator());
        }
        html.append("</table>").append(System.lineSeparator()).append("</html>");
        return html.toString();
    }
    
    /**
     * Used to read small files for the management servers production of responses for the REST interface.
     * @param fileLocation The location of the file to read into a string.
     * @return A string with the contents of an entire file.
     */
    public String readFile(String fileLocation){
        try {
            StringBuilder fileContents = new StringBuilder();
            Scanner scanner = new Scanner(new File(fileLocation));
            while(scanner.hasNext()){
                fileContents.append(scanner.nextLine());
            }
            return fileContents.toString();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, "File does not exist.", ex);
        }
        return "File not read.";
    }

    /**
     * Gets the HashMap storing all the Tasks in the system.
     * @return The HashMap storing all the Tasks in the system.
     */
    public HashMap<Task, Boolean> getFiles() {
        return files;
    }

    /**
     * Gets the results of a specified Task, with the passed file name and task type.
     * @param fileName The name of the resource to find results for.
     * @param taskType The type of task that was conducted on the resource.
     * @return The bytes that resulted from the task being carried out on the resource.
     */
    @Override
    public byte[] getResults(String fileName, String taskType) throws RemoteException {
        return this.node.get(fileName + "Results" + taskType);
    }
    
    /**
     * Clears the list of files in this ManagementServer.
     */
    public void emptyList(){
        this.files.clear();
        this.fileStoreChange = true;
    }

    /**
     * Displays a terminal representation of all files in this server.
     */
    public void showList() {
        System.out.println("File Name\t|\tTask\t|\tDone or not\t");
        for (Entry<Task, Boolean> e : this.files.entrySet()) {
            System.out.println(e.getKey().getFileName() + "\t|"
                    + e.getKey().getType() + "\t|\t"
                    + e.getValue() + "\t");
        }
    }

    /**
     * Runs the maintenance thread for this management server.
     */
    @Override
    public void run() {
        boolean nodeAlive = false; //Determines if node to access DHT is alive or not.
        int elapsed = 0;
        while (true) {
            try {
                Thread.sleep(LOOP_INTERVAL);
                elapsed += LOOP_INTERVAL;
            } catch (InterruptedException ex) {
                Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, null, ex);
            }

            try {
                nodeAlive = this.node.ping();
            } catch (Exception ex) {
                log("Current node used for DHT has failed to respond to ping.");
                nodeAlive = false;
            }

            if (nodeAlive) {
                this.maintainFiles();
                if(this.fileStoreChange || elapsed % (LOOP_INTERVAL * 30) == 0){ //Ensures not saving pointlessly every cycle.
                    this.backUpFileList();
                    this.fileStoreChange = false;
                }
                this.updateSuccessorList();
            } else { //If node is dead, attempt to fetch new one from successor list.
                try {
                    this.getNextNode();
                } catch (Exception e) {
                    log("Next node list unavailable. Must attempt reconnect to a server.");
                }
            }
        }
    }
    
    /**
     * Gets an updated successor list from the node used to query the DHT system. Call periodically in maintenance thread.
     */
    private void updateSuccessorList(){
        try {
            this.successorList = this.node.getSuccessorList(); //Update successor list
        } catch (RemoteException ex) {
            Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, "Unable to update successor list.", ex);
        }
    }
    
    /**
     * Gets the next DHT node in case the one we're currently using fails.
     */
    private void getNextNode(){
        for(Finger f : this.successorList){
            try { //Look for next alive node to use to interact with DHT
                if(f.getNode().ping()){
                    this.node = (IChordNode) f.getNode();
                    return;
                }
            } catch (RemoteException ex) {
                log("Could not find next server, may need to reconnect.");
                //Logger.getLogger(ManagementServer.class.getName()).log(Level.SEVERE, "Node down on successor list.", ex);
            }
        }
    }

    /**
     * Check if files in the file list are still accessible on the DHT. If they're not, they're removed from the list.
     */
    private void maintainFiles() {
        synchronized(this.files){
            Iterator iter = this.files.entrySet().iterator();
            while(iter.hasNext()){
                Entry<Task,Boolean> entry = (Entry)iter.next();
                this.checkFile(entry.getKey(), entry.getValue(), iter);
            }
        }
    }

    /**
     * Removes a given task if it returns null from the DHT back-end after a
     * specified time has elapsed.
     *
     * @param key The Task object relating to the resource to check.
     * @param finishedProcessing Boolean to if the resource is done being
     * processed.
     * @param iter The iterator used to remove the object, after the check.
     */
    private void checkFile(Task key, boolean finishedProcessing, Iterator iter) {
        try {
            if (!key.getType().equals(TaskType.UNDEFINED)) {
                if (finishedProcessing) {
                    this.node.get(key.resultName());
                } else {
                    this.node.get(key.requestName());
                    if (!this.taskQueue.contains(key)) { //If task not queue & not done processing, add it in. Maybe a node failed processing it?
                        this.taskQueue.add(key);
                    }
                }
            }
            key.resetMissingTime();
        } catch (Exception ex) { //Remove if results are missing.
            log("Missing entry in DHT.");
            key.incrementMissingTime(LOOP_INTERVAL);
            log("Missing " + key.getFileName() + " with task " + key.getType() + " for " + key.getMissingTime() + "ms");
            if (key.getMissingTime() > MISSING_TIME_LIMIT) {
                iter.remove();
                log("Removed " + key.getFileName() + " with task " + key.getType());
                this.fileStoreChange = true;
            }
        }
    }

    public BlockingQueue getTaskQueue() {
        return taskQueue;
    }

    
    
}
