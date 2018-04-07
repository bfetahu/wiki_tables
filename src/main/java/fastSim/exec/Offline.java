package fastSim.exec;

import fastSim.core.Indexer;
import fastSim.data.Graph;
import fastSim.util.Config;
import fastSim.util.IndexManager;

public class Offline {
	
	public static void main(String[] args) throws Exception {
	    Config.hubType = args[0];
		Config.numHubs = Integer.parseInt(args[1]);
	//	boolean forceUpdate = true;
		Config.depth = Integer.parseInt(args[2]);
		Config.stopRea =Double.parseDouble(args[3]);
	    Graph g = new Graph();
	    g.loadFromFile(Config.nodeFile, Config.edgeFile, true);
	    
	    IndexManager.mkSubDirDeep();
	    
	    Indexer.index(g.getHubs(), g, true);
	}
}
