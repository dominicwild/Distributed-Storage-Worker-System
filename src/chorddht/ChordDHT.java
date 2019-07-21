package chorddht;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Vector;

/**
 *
 * @author Dominic
 */
public class ChordDHT {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException, RemoteException {

//        Registry registry = LocateRegistry.createRegistry(1010);
//        System.out.println("RMI server is running.");
//        while(true);
        
//        ChordNode n1 = new ChordNode("1NodeOne");
//        ChordNode n2 = new ChordNode("2NodeTwo");
//        ChordNode n3 = new ChordNode("3NodeThree");
//        ChordNode n4 = new ChordNode("4NodeFour");
//        ChordNode n5 = new ChordNode("5NodeFive");
//        ChordNode n6 = new ChordNode("6NodeSix");
//        ChordNode n7 = new ChordNode("7NodeSeven");
//        ChordNode n8 = new ChordNode("8NodeEight");
//        ChordNode n9 = new ChordNode("9NodeNine");
//        ChordNode n10 = new ChordNode("10NodeTen");
//        
//        
//        n1.join(n2);
//        n3.join(n1);
//        
//        stabilizeWait();
        
//        ChordNode[] nodes = {n1,n2,n3};
//        
//        for(int i=0;i<256;i++){
//            nodes[i % 3].put(Integer.toString(i),Integer.toBinaryString(i).getBytes());
//        }
//        
//        System.out.println(new String(n1.get("44")));
//        
//        n4.join(n1);
//        n5.join(n3);
//
//        n6.join(n4);
//        n7.join(n2);
//        n8.join(n4);
//        n9.join(n7);
//        n10.join(n6);
//
//        System.out.println(new String(n1.get("163")));
//        for(int i=0;i<256;i++){
//            Thread.sleep(1000);
//            System.out.println(new String(n1.get(Integer.toString(i))));
//        }
//        String command;
//        String inputString;
//        String parameters;
//        Scanner input = new Scanner(System.in);
//
//        inputString = input.nextLine();
//        int pos = inputString.indexOf(' ');
//        command = inputString.substring(0, pos);
//        parameters = inputString.substring(pos).trim();
        
        HashMap<Integer,ArrayList<String>> map = new HashMap<>();

        for(int i=0;i<255;i++){
            for(int j=0;j<255;j++){
                int hash = Utility.hash("" + (char)i + (char)j);
                if(map.containsKey(hash)){
                    map.get(hash).add("" + (char)i + (char)j);
                } else {
                    map.put(hash, new ArrayList<>());
                    map.get(hash).add("" + (char)i + (char)j);
                }
            }
        }
        
        for(ArrayList list : map.values()){
            System.out.println(list);
        }
    
    }

    public static void stabilizeWait(){
        System.out.println("Waiting for topology to stabilise...");

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            System.out.println("Interrupted");
        }
    }
    
    public static void stockTest() throws RemoteException{
        ChordNode n1 = new ChordNode("yorkshire");
        ChordNode n2 = new ChordNode("lancashire");
        ChordNode n3 = new ChordNode("cheshire");

        System.out.println("Joining nodes to network...");
        n1.join(n2);
        n3.join(n1);
        
        
        stabilizeWait();
        

        System.out.println("Inserting keys...");

        String key1 = "alex";
        byte[] data1 = new byte[128];
        String key2 = "sam";
        byte[] data2 = new byte[64];
        String key3 = "jamie";
        byte[] data3 = new byte[256];
        
        n1.put(key1, "Alex Data".getBytes());
        n2.put(key2, "Sam Data".getBytes());
        n3.put(key3, "Jamie Data".getBytes());

        System.out.println("All done.");
        byte[] derr = n1.get("alex");
        String data = new String(n1.get("alex"));
        
        System.out.println(data);
        System.out.println(new String(n1.get("sam")));
        System.out.println(new String(n1.get("jamie")));
    }
    
    public static int binarySearch(Vector<Integer> list, int target){
        int boundLeft = 0;
        int boundRight = list.size()-1;
        int midPoint;
        
        while(boundLeft < boundRight){
            midPoint = (int)(boundLeft + boundRight)/2;
            if(target > list.get(midPoint)){
                boundLeft = midPoint + 1;
            } else if (target < list.get(midPoint)){
                boundRight = midPoint - 1;
            } else {
                return midPoint;
            }
        }
        
        return boundLeft;
    }
    
    
    public static void insert(Vector<Integer> list, int toInsert){
        int closeIndex = binarySearch(list,toInsert);
        int value;
        if(list.size() > 0 && list.get(closeIndex) != null){
            value = list.get(closeIndex);
        }
        if(closeIndex == 0 || list.get(closeIndex) >= toInsert){
            list.insertElementAt(toInsert, closeIndex);
        } else {
            list.insertElementAt(toInsert, closeIndex+1);
        }
    }

}
