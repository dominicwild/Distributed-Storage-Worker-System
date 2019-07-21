package chorddht;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An object that encapsulate a remote reference to a Chord node in a ring. It
 * contains a remote reference to said node and its key.
 *
 * @author Dominic
 */
public class Finger implements Serializable {

    private int key;            //The key of the node stored in this Finger.
    private IChordNode node;    //A remote reference to the node.

    /**
     * Creates a finger for the given remote node reference.
     *
     * @param node The node to create a Finger of.
     */
    public Finger(IChordNode node) {
        this.node = node;
        try {
            this.key = node.getKey();
        } catch (RemoteException ex) {
            Logger.getLogger(Finger.class.getName()).log(Level.SEVERE, "Cannot make Finger.", ex);
        }
    }

    /**
     * Gets the key of this particular node.
     *
     * @return The key of the node stored by this Finger.
     */
    public int getKey() {
        return key;
    }

    /**
     * Gets the remote reference to the node stored by this Finger.
     *
     * @return The remote reference to the node stored by this Finger.
     */
    public IChordNode getNode() {
        return node;
    }

}
