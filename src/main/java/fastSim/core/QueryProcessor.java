package fastSim.core;

import com.google.common.util.concurrent.AtomicDouble;
import fastSim.data.Graph;
import fastSim.data.Node;
import fastSim.data.PrimeSim;
import fastSim.util.Config;

import java.util.HashMap;
import java.util.Map;

public class QueryProcessor {

    private Graph graph;

    public QueryProcessor(Graph graph) {
        this.graph = graph;
    }

    public PrimeSim graphExp(Node q, String graphType) throws Exception {
        graph.resetPrimeG();
        int expansion = Config.eta;

        PrimeSim sim = new PrimeSim(); // start node

        if (q.isHub) { // change q.isHub to false, so that it will not use precomputation; 4/17 @fw
            sim.loadFromDisk(q.id, graphType);
        } else {
            if (graphType == "out")
                sim = graph.computeOutPrimeSim(q);
            else if (graphType == "in")
                sim = graph.computeInPrimeSim(q);
            else {
                System.out.println("Type of prime graph should be either out or in.");
                System.exit(0);
            }
        }
        if (expansion == 0 || sim.numHubs() == 0)
            return sim; // for primeInG, always expand for eta iterations

        PrimeSim expSim = sim.duplicate();

        if (graphType == "in")
            expSim.addMeetingNodes(sim.getMeetingNodes());
        Map<Integer, Map<Integer, Double>> borderHubsScore = new HashMap<>(); // hub->(length,value)

        // extracting borderHubs information
        for (int length : sim.getMap().keySet()) {
            //added 8-27
            if (length == 0) continue; //don't expand the query node if itself is a hub
            for (int nid : sim.getMap().get(length).keySet()) {
                Node node = graph.getNode(nid);
                //added 8-27
                //if(node==q) continue; q should also be expanded, it can affect the reachability of other nodes, just s(q,q) wouldn't be affected
                if (node.isHub) {
                    // store the reachability to hub
                    if (borderHubsScore.get(nid) == null) {
                        borderHubsScore.put(nid, new HashMap<Integer, Double>());
                    }
                    if (borderHubsScore.get(nid).get(length) == null) {
                        borderHubsScore.get(nid).put(length,
                                sim.getMap().get(length).get(nid));
                    } else {
                        System.out.println("shouldn't go to here.");
                        double old_value = borderHubsScore.get(nid).get(length);
                        borderHubsScore.get(nid).put(length, old_value + sim.getMap().get(length).get(nid));
                    }
                }
            }
        }


        // recursively adding outG of hubs
        while (expansion > 0) {
            expansion = expansion - 1;
            Map<Integer, Map<Integer, Double>> borderHubsNew = null;

            if (expansion > 0)
                borderHubsNew = new HashMap<>();

            if (borderHubsScore.size() == 0)
                return expSim;

            for (int hid : borderHubsScore.keySet()) {
                //add expanding threshold for hubs: Config.delta
                double hubScore = 0;
                for (int len : borderHubsScore.get(hid).keySet())
                    hubScore += borderHubsScore.get(hid).get(len);
                if (hubScore < Config.delta)
                    continue;
                //end. 2/18/2015fw
                PrimeSim nextSim = new PrimeSim();
                //commented the next line to prevent using hub scores 04/17
                nextSim.loadFromDisk(hid, graphType);

                if (graphType == "in")
                    expSim.addMeetingNodes(nextSim.getMeetingNodes());

                expSim.addFrom(nextSim, borderHubsScore.get(hid));// expand

                if (expansion > 0) {
                    //store border hubs in nextSim

                    for (int i = 0; i < nextSim.numHubs(); i++) {
                        int newHub = nextSim.getHubId(i);
                        for (int l = 1; l < nextSim.getMap().size(); l++) {
                            if (nextSim.getMap().get(l).containsKey(newHub)) {
                                double addScore = nextSim.getMap().get(l).get(newHub);

                                //set borderHubsNew
                                if (borderHubsNew.get(newHub) == null)
                                    borderHubsNew.put(newHub, new HashMap<Integer, Double>());
                                for (int oldLen : borderHubsScore.get(hid).keySet()) {
                                    double oldScore = borderHubsScore.get(hid).get(oldLen);
                                    double existScore;
                                    if (borderHubsNew.get(newHub).get(l + oldLen) == null)
                                        existScore = 0.0;
                                    else
                                        existScore = borderHubsNew.get(newHub).get(l + oldLen);
                                    borderHubsNew.get(newHub).put(l + oldLen, existScore + oldScore * addScore);
                                }
                            }
                        }
                    }
                }
            }
            borderHubsScore = borderHubsNew;
        }

        return expSim;
    }

    public double query(Node q1, Node q2) throws Exception {
        if (q1.id == q2.id)
            return 1;
        double result = 0;

        PrimeSim inSim1 = graphExp(q1, "in");
        PrimeSim inSim2 = graphExp(q2, "in");

        if (inSim1.getMeetingNodes().size() == 0 || inSim2.getMeetingNodes().size() == 0) {
            result = 0;
            return result;
        }
        PrimeSim smallPS = inSim1.numLength() < inSim2.numLength() ? inSim1 : inSim2;
        PrimeSim bigPS = inSim1.numLength() < inSim2.numLength() ? inSim2 : inSim1;

        AtomicDouble atm = new AtomicDouble();

        smallPS.getLengths().parallelStream().forEach(length -> {
            if (bigPS.getMap().get(length) == null)
                return;
            Map<Integer, Double> small_node_score_map = smallPS.getMap().get(length).size() < bigPS.getMap().get(length).size() ? smallPS.getMap().get(length) : bigPS.getMap().get(length);
            Map<Integer, Double> big_node_score_map = smallPS.getMap().get(length).size() < bigPS.getMap().get(length).size() ? bigPS.getMap().get(length) : smallPS.getMap().get(length);
            for (int nid : small_node_score_map.keySet()) {
                if (big_node_score_map.get(nid) == null)
                    continue;
                double rea = small_node_score_map.get(nid) * big_node_score_map.get(nid); //multiply alpha here
                atm.getAndAdd(rea);
            }
        });

        return atm.get();
    }

}
