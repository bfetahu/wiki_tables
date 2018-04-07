package fastSim.core.hubselection;

import fastSim.data.Node;
import fastSim.util.KeyValuePair;

import java.util.ArrayList;


public class InDegHub extends HubSelection {

    protected double pagerankPow;
    protected double inDegPow;

    public InDegHub(String nodeFile, String edgeFile) throws Exception {
        super(nodeFile, edgeFile);

    }

    @Override


    protected void fillNodes() {
        nodes = new ArrayList<>();

        for (Node n : graph.getNodes())
            nodes.add(new KeyValuePair(n.id, n.in.size()));
    }


}
