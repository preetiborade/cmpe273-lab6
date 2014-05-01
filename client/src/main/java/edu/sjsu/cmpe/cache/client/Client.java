package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;

public class Client {
	static char values[] = {'a','b','c','d','e','f','g','h','i','j'};
	private final static HashFunction hashFunc= Hashing.md5();
	private  final static SortedMap<Integer, String> circle = new TreeMap<Integer, String>();

	private static String server1="http://localhost:3000";
	private static String server2="http://localhost:3001";
	private static String server3="http://localhost:3002";

	
    /*static CacheServiceInterface server1 = new DistributedCacheService("http://localhost:3000");
    static CacheServiceInterface server2 = new DistributedCacheService("http://localhost:3001");
    static CacheServiceInterface server3 = new DistributedCacheService("http://localhost:3002");*/
	
	public static void add(String node, int i) {
	//	for (int i = 0; i < numberOfReplicas; i++) {
			HashCode hashCode=hashFunc.hashLong(i);
			circle.put(hashCode.asInt(),node);
	//	}
    }

	 public void remove(String node) {
	//	 for (int i = 0; i < numberOfReplicas; i++) {
		  	circle.remove(Hashing.md5().hashCode());
	//	 }
	  }

	public static String get(Object key) {
	    if (circle.isEmpty()) {
	      return null;
	    }
	    int hash = hashFunc.hashLong((Integer)key).asInt();
	    if (!circle.containsKey(hash)) {
	      SortedMap<Integer, String> tailMap = circle.tailMap(hash);
	      hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
	    }
	    return circle.get(hash);
	  } 

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        List<String> servers = new ArrayList<String>();
        servers.add(server1);
        servers.add(server2);
        servers.add(server3);
        
        //Listing servers available  
        for (int i=0;i<servers.size();i++)
        {
        	System.out.println("Servers :: "+ servers.get(i));
        	add(servers.get(i),i);
        }
        
        
        for (int i=0;i<10;i++)
        {
        	int bucket = Hashing.consistentHash(Hashing.md5().hashLong(i),circle.size());
        	String serverSelected = get(bucket);
        	System.out.println("Server selected :: "+serverSelected);

        	CacheServiceInterface cache = new DistributedCacheService(serverSelected);
        	cache.put(i+1, String.valueOf(values[i]));
        	cache.get(i+1);
        	System.out.println( "Key :: " +(i+1)+ " Value :: "+cache.get(i+1));
        	System.out.println("Cache Client.. ");

        // one of the back end servers is removed from the (middle of the) pool
        	servers.remove(serverSelected);
        }

        //servers.remove(cur_server);
    }
}
        
     
