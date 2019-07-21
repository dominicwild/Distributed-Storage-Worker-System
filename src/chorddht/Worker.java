package chorddht;

import static chorddht.StartNode.RMI_SERVER_NAME;
import static chorddht.Utility.log;
import java.util.Base64;
import java.io.ByteArrayOutputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.parsers.ParserConfigurationException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A worker that polls a passed Queue and conducts processing.
 *
 * @author Dominic
 */
public class Worker implements Runnable {
    
    private ChordNode node;                     //A node this worker can use to get and store files.
    private WorkerManagement managementServer;  //A remote reference to the management server.

    /**
     * Creates a generic worker to handle Task requests from the given queue.
     *
     * @param node The ChordNode to which this worker runs on.
     */
    public Worker(ChordNode node) {
        this.node = node;
        this.managementServer = this.getManagementServer();
    }

    /**
     * The main processing loop.
     */
    @Override
    public void run() {
        Task task;
        while (true) {
            try {
                task = (Task) this.getManagementServer().take(); //Blocks until task is put in queue.
                switch (task.getType()) {
                    case WORD_METRICS:
                        wordMetricsTask(task);
                        break;
                    case ENCRYPT:
                        encrypt(task);
                        break;
                    case HASH:
                        hash(task);
                        break;
                    case UNDEFINED:
                        break;
                }
                this.getManagementServer().notifyReady(task);
                log("Processed task: " + task.getFileName());
            } catch (InterruptedException ex) {
                Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (RemoteException ex) {
                log("Remote exception occurred while attempting to process tasks. Management server may be down.");
                //Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, "Could not complete processing of a task due to remote exception.", ex);
                try {
                    Thread.sleep(1000); //Sleep to avoid constantly repolling.
                } catch (InterruptedException ex1) {
                    Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex1);
                }
            } 

        }
    }

    /**
     * Gets the management server from RMI, with some fault tolerance
     * mechanisms.
     *
     * @return The remote interface object of the management server.
     */
    private WorkerManagement getManagementServer() {
        boolean alive = false;

        if (this.managementServer != null) { //Checks if first initializing the variable
            alive = this.isServerAlive();
        }
        if (alive) {
            return this.managementServer;
        } else {
            try {
                this.managementServer = (WorkerManagement) Naming.lookup(RMI_SERVER_NAME + "WorkerManagement");
                if (this.isServerAlive()) {
                    log("New alive reference found.");
                } else {
                    log("No alive reference found on RMI.");
                }
                return this.managementServer;
            } catch (NotBoundException | MalformedURLException | RemoteException ex) {
                log("Error in fetching management server from RMI server.");
                return this.managementServer;
            }
        }
    }

    /**
     * Checks if management server is active and reachable.
     *
     * @return A boolean representing if the management server is still running.
     */
    private boolean isServerAlive() {
        boolean alive = false;
        try {
            alive = this.managementServer.ping();
        } catch (RemoteException ex) {
            log("Management server isn't responding. Attempting to fetch new reference.");
        }
        return alive;
    }

    /**
     * Conducts the "Word Metric" TaskType job.
     *
     * @param task The task with associated information to conduct the
     * processing.
     */
    private void wordMetricsTask(Task task) throws RemoteException {
        byte[] fileBytes = this.getTaskBytes(task);
        //Get metrics
        String[] words = getWords(new String(fileBytes));
        int averageWordLength = this.averageStringLength(words);
        String mostCommonWord;
        if (words.length == 0) {
            mostCommonWord = new String(fileBytes);
        } else {
            mostCommonWord = mostCommonString(words);
        }
        //Begin building XML response.
        Document xml;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try { //Make tags
            xml = (Document) DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element wordNumTag = xml.createElement("NumberOfWords");
            Element avgWordLengthTag = xml.createElement("AverageWordLength");
            Element commonWordTag = xml.createElement("MostCommonWord");
            Element root = xml.createElement("WordMetrics");
            //Add data to tags
            wordNumTag.appendChild(xml.createTextNode(Integer.toString(words.length)));
            avgWordLengthTag.appendChild(xml.createTextNode(Integer.toString(averageWordLength)));
            commonWordTag.appendChild(xml.createTextNode(mostCommonWord));
            //Insert tags into XML document.
            root.appendChild(wordNumTag);
            root.appendChild(avgWordLengthTag);
            root.appendChild(commonWordTag);
            xml.appendChild(root);
            //Put into a byte stream for storage.
            TransformerFactory.newInstance().newTransformer().transform(new DOMSource(xml), new StreamResult(output));
        } catch (ParserConfigurationException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, "Parser not correctly configured.", ex);
        } catch (TransformerConfigurationException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, "Transformer configuration failed.", ex);
        } catch (TransformerException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, "Transformer threw an exception.", ex);
        } //Store the result.
        this.node.put(task.resultName(), output.toByteArray());
    }

    /**
     * Finds the average string length, in terms of characters, in an array of
     * strings.
     *
     * @param strings The array of strings to find the average length of.
     * @return The average length of the strings.
     */
    private int averageStringLength(String[] strings) {
        int total = 0;
        for (String s : strings) {
            total += s.length();
        }
        if (strings.length == 0) {
            return 0;
        } else {
            return total / strings.length;
        }
    }

    /**
     * Finds the most common string in a set of strings.
     *
     * @param strings The array of strings to find the most common of.
     * @return The most common string in the array of strings.
     */
    private String mostCommonString(String[] strings) {
        HashMap<String, Integer> list = new HashMap<>();
        int count = 0;
        int largest = 0;
        String mostCommon = "";
        for (String s : strings) {
            if (list.containsKey(s)) {
                count = list.get(s);
                count++;
                if (count > largest) {
                    largest = count;
                    mostCommon = s;
                }
                list.put(s, count);
            } else {
                list.put(s, 1);
                if(largest == 0){
                    largest++;
                    mostCommon = s;
                }
            }
        }
        return mostCommon;
    }

    /**
     * Gets the unprocessed bytes of the associated task from DHT.
     * @param t The task to get the bytes of.
     * @return The bytes of the unprocessed task.
     */
    public byte[] getTaskBytes(Task t) throws RemoteException{
        return this.node.get(t.requestName());
    }

    /**
     * Gets an array of words from the given passed string.
     * @param string The string to extract a list of words from.
     * @return The list of words as an array of strings.
     */
    public String[] getWords(String string) {
        StringTokenizer tokenizer = new StringTokenizer(string, " ");
        String[] words = new String[tokenizer.countTokens()];
        for (int i = 0; i < words.length; i++) {
            words[i] = tokenizer.nextToken();
        }
        return words;
    }

    /**
     * Encrypts data within a Task using the DES algorithm.
     *
     * @param task The task to run the encryption job on.
     */
    private void encrypt(Task task) throws RemoteException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            //Get and encrypt the bits
            byte[] toEncrypt = this.getTaskBytes(task);
            SecretKey key = KeyGenerator.getInstance("DES").generateKey();
            Cipher des = Cipher.getInstance("DES");
            des.init(Cipher.ENCRYPT_MODE, key);
            byte[] encryptedBytes = des.doFinal(toEncrypt);
            //Build the XML document.
            Document xml = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element root = xml.createElement("Encrypt");
            Element keyTag = xml.createElement("Key");
            Element encryptedDataTag = xml.createElement("EncryptedData");
            Element algorithmTag = xml.createElement("Algorithm");
            //Base 64 encoded strings to store the characters
            byte[] key64 = Base64.getEncoder().encode(key.getEncoded());
            byte[] data64 = Base64.getEncoder().encode(encryptedBytes);
            //Add data onto tags.
            keyTag.appendChild(xml.createTextNode(new String(key64)));
            encryptedDataTag.appendChild(xml.createTextNode(new String(data64)));
            algorithmTag.appendChild(xml.createTextNode(des.getAlgorithm()));
            //Put tags into document.
            root.appendChild(keyTag);
            root.appendChild(encryptedDataTag);
            root.appendChild(algorithmTag);

            xml.appendChild(root);
            //Create the XML document into the byte stream.
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(xml), new StreamResult(output));
        } catch (NoSuchAlgorithmException ex) {
            log("No such algorithm detected.");
        } catch (NoSuchPaddingException ex) {
            log("No such cipher detected.");
        } catch (InvalidKeyException ex) {
            log("Invalid initialization of cipher.");
        } catch (RemoteException ex) {
            log("Failed to get bytes from DHT.");
        } catch (IllegalBlockSizeException | BadPaddingException ex) {
            log("Failed to encrypt bytes.");
        } catch (ParserConfigurationException ex) {
            log("Failed to create XML document object.");
        } catch (TransformerException ex) {
            log("Failed to make XML document into the ByteOutputStream.");
        }
        this.node.put(task.resultName(), output.toByteArray());
    }

    /**
     * Conducts the hashing job on a given task.
     *
     * @param task The task to conducting the hashing job on.
     */
    private void hash(Task task) throws RemoteException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            //Hash the bytes
            byte[] toHash = this.getTaskBytes(task);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            //Base 64 encode for storage
            byte[] hashedBytes = md5.digest(toHash);
            hashedBytes = Base64.getEncoder().encode(hashedBytes);
            //Make the XML document.
            Document xml = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element root = xml.createElement("Hash");
            Element algorithmTag = xml.createElement("Algorithm");
            Element hashDataTag = xml.createElement("HashedData");
            Element providerTag = xml.createElement("Provider");
            Element lengthTag = xml.createElement("Length");
            //Add data to tags.
            algorithmTag.appendChild(xml.createTextNode(md5.getAlgorithm()));
            hashDataTag.appendChild(xml.createTextNode(new String(hashedBytes)));
            providerTag.appendChild(xml.createTextNode(md5.getProvider().toString()));
            lengthTag.appendChild(xml.createTextNode(Integer.toString(md5.getDigestLength())));
            //Add tags to document.
            root.appendChild(algorithmTag);
            root.appendChild(hashDataTag);
            root.appendChild(providerTag);
            root.appendChild(lengthTag);

            xml.appendChild(root);
            //Create the document into the byte stream.
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(xml), new StreamResult(output));
        } catch (NoSuchAlgorithmException ex) {
            log("Invalid algorithm provided for message digest object.");
        } catch (RemoteException ex) {
            log("Exception thrown during retrieval of bytes to hash.");
        } catch (ParserConfigurationException ex) {
            log("Failed to create XML document object.");
        } catch (TransformerException ex) {
            log("Failed to make XML document into the ByteOutputStream.");
        }
        this.node.put(task.resultName(), output.toByteArray());
    }

}
