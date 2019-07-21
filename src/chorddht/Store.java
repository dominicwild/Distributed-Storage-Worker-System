package chorddht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Class used to store data within the DHT system.
 * @author Dominic
 */
class Store implements Serializable{
    private int key;     //Key for the data to store, for searching purposes.
    private HashMap<String,byte[]> values = new HashMap<>();

    /**
     * Creates a new store with a key associated to some data.
     * @param hashKey The key of the data.
     */
    Store(int hashKey) {
        this.key = hashKey;
    }
    
    /**
     * Creates a new store with a key associated to some data.
     * @param hashKey The key of the data.
     */
    Store(int hashKey, String key ,byte[] bytes) {
        this.key = hashKey;
        this.values.put(key, bytes);
    }
    
    /**
     * Gets the key of this store.
     * @return The key of this store.
     */
    public int getKey() {
        return key;
    }

//    /**
//     * Gets the bytes stored in this store.
//     * @return The bytes stored in this store.
//     */
//    public byte[] getValue() {
//        return value;
//    }

    /**
     * Gets the bytes stored in this store.
     * @return The bytes stored in this store.
     */
    public byte[] getValue(String key) {
        return this.values.get(key);
    }
    
    /**
     * Gets the bytes stored in this store.
     * @return The bytes stored in this store.
     */
    public byte[] put(String key, byte[] value) {
        return this.values.put(key,value);
    }
    
    
}
