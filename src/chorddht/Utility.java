package chorddht;

import static chorddht.ChordNode.KEY_BITS;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A class that contains generic utility methods.
 * @author Dominic
 */
public class Utility {

    /**
     * Converts a given string to a key that can be used with the distributed
     * hash table.
     *
     * @param s The string to return the hash of.
     * @return The has of the string.
     */
    public static int hash(String s) {
        int hash = 0;
        for (int i = 0; i < s.length(); i++) {
            hash = hash * 31 + (int) s.charAt(i);
        }
        if (hash < 0) {
            hash = hash * -1;
        }
        return hash % ((int) Math.pow(2, KEY_BITS));
    }
    
    /**
     * Outputs a generic log message in the terminal with the date and time.
     * @param logLine The line to output in the log.
     */
    public static void log(String logLine) {
        SimpleDateFormat formatter = new SimpleDateFormat();
        System.out.println("[" + formatter.format(new Date()) + "] " + logLine);
    }
    
}
