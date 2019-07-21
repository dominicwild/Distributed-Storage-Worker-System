package chorddht;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class that represents a task for the workers in the Chord Ring to carry out.
 * @author Dominic
 */
public class Task implements Serializable {

    private String fileName;        //The name of the file in the DHT system.
    private TaskType type;          //The type of processing to carry out.
    private int missingTime;        //The time this task has been unable to be retrieved, in milliseconds.

    /**
     * Creates a task to be carried on the DHT system.
     * @param fileName The name of the file that has the data to process.
     * @param type The type of task to carry out.
     */
    public Task(String fileName, TaskType type) {
        this.fileName = fileName;
        this.type = type;
    }
    
    /**
     * Gets the type of processing that is to be done in this task.
     * @return The type of processing that is to be done in this task.
     */
    public TaskType getType() {
        return type;
    }

    /**
     * Gets the name of the file this tasks associates with.
     * @return The name of the file this tasks associates with.
     */
    public String getFileName() {
        return fileName;
    }
    
    public String requestName(){
        return this.getFileName() + "Request" + this.getType();
    }

    /**
     * Creates the key name of the resulting byte array to store on the DHT.
     *
     * @param task The task that has been processed.
     * @return The key to store the data on the DHT with.
     */
    public String resultName() {
        return this.getFileName() + "Results" + this.getType();
    }
    
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.fileName);
        hash = 97 * hash + Objects.hashCode(this.type);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Task other = (Task) obj;
        if (!Objects.equals(this.fileName, other.fileName)) {
            return false;
        }
        return this.type == other.type;
    }

    public int getMissingTime() {
        return missingTime;
    }
    
    public void resetMissingTime(){
        this.missingTime = 0;
    }
    
    public void incrementMissingTime(int time){
        this.missingTime += time;
    }

    

    
    
}
