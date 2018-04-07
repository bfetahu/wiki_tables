package fastSim.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import fastSim.util.io.TextReader;



public class ClusterManager {

	public static final String FILE_SEP = System.getProperties().getProperty("file.separator");
	
	private Map<Integer, Integer> mapping;
	
	public static String getClusterDir() {
		String path = Config.clusterDir;
	    if (!path.endsWith(FILE_SEP))
	    	path += FILE_SEP;
	    	
	    path += Config.numClusters + FILE_SEP;
	    return path;
	}
	
	public static void mkSubDir() {
        File f = new File(getClusterDir());
        if (!f.exists())
        	f.mkdirs();
    }
	
	public static String getClusterFile(int clusterId) {
		return getClusterDir() + clusterId;
	}
	
	public static String getClusterEdge(int clusterId) {
		return getClusterDir() + clusterId + "edges";
	}
	public static String getClusterMappingFile() {
		return getClusterDir() + "mapping";
	}
	public static String getClusterNode(int clusterId){
		return getClusterDir() + clusterId +"nodes";
	}
	public ClusterManager() throws Exception {
		TextReader in = new TextReader(getClusterMappingFile());
		mapping = new HashMap<Integer, Integer>();
		
		String line;
		while ( (line = in.readln()) != null) {
			String[] split = line.split("\t");
			int nodeId = Integer.parseInt(split[0]);
			int clusterId = Integer.parseInt(split[1]);
			mapping.put(nodeId, clusterId);
		}
		in.close();
	}
	
	public int getClusterId(int nodeId) {
		return mapping.get(nodeId);
	}
}
