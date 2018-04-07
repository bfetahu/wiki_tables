package fastSim.core.hubselection;

import java.util.ArrayList;
import java.util.Random;

import fastSim.data.Node;
import fastSim.util.KeyValuePair;



public class RandomHub extends HubSelection {

    public RandomHub(String nodeFile, String edgeFile) throws Exception {
		super(nodeFile, edgeFile);
	}



	@Override
	protected void fillNodes() {
		Random rnd = new Random(4326210911L);
		nodes = new ArrayList<>();
		
		for (Node n : graph.getNodes())
			nodes.add(new KeyValuePair(n.id, rnd.nextDouble()));
	}
	
	

}
