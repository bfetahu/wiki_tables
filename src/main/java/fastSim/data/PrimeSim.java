package fastSim.data;

import fastSim.util.Config;
import fastSim.util.IndexManager;
import fastSim.util.MapCapacity;
import fastSim.util.io.DataReader;
import fastSim.util.io.DataWriter;

import java.io.Serializable;
import java.util.*;


public class PrimeSim implements Serializable {
    private static final long serialVersionUID = -7028575305146090045L;
    private List<Integer> hubs;
    protected Map<Integer, Map<Integer, Double>> map;
    protected boolean outG;
    protected List<Integer> meetingNodes;

    public PrimeSim() {
        map = new HashMap<>();
        hubs = new ArrayList<>();
        meetingNodes = new ArrayList<>();

    }

    public PrimeSim(int numNodes) {
        //need to change MapCapacity when double->Map?
        map = new HashMap<>(MapCapacity.compute(numNodes));
        hubs = new ArrayList<>();
    }

    public Set<Integer> getLengths() {
        return map.keySet();
    }

    public int numHubs() {
        return hubs.size();
    }

    public int numLength() {
        return map.size();
    }

    public Map<Integer, Map<Integer, Double>> getMap() {
        return map;
    }

    public int getHubId(int index) {
        return hubs.get(index);
    }

    public List<Integer> getMeetingNodes() {
        return meetingNodes;
    }

    public void addNewNode(Node h, String simType) {

        h.isVisited = true;
        if (h.isHub)
            hubs.add(h.id);
        if (simType == "in" && h.out.size() > 1)  //store meeting nodes for ingraphs //meetingnodes refer to >1 nodes (descendants)
            meetingNodes.add(h.id);


    }

    public void set(int l, Node n, double value) {
        Map<Integer, Double> nodesVal;
        if (map.get(l) != null) {
            nodesVal = map.get(l);
            nodesVal.put(n.id, value);
            map.put(l, nodesVal);
        } else {
            nodesVal = new HashMap<>();
            nodesVal.put(n.id, value);
            map.put(l, nodesVal);
        }

    }


    public void set(int l, Map<Integer, Double> nodeValuePairs) {
        Map<Integer, Double> nodesVal = map.get(l);

        if (nodesVal == null) {
            map.put(l, nodeValuePairs);
        } else {
            System.out.println("####PrimeSim line108: should not go to here.");
            nodesVal.putAll(nodeValuePairs);
            map.put(l, nodesVal);
        }
    }


    public long computeStorageInBytes() {
        long nodeIdSize = (1 + hubs.size()) * 4;
        long mapSize = (1 + map.size()) * 4 + map.size() * 8;
        return nodeIdSize + mapSize;
    }


    public String getCountInfo() {
        //int graphSize = map.size();
        int hubSize = hubs.size();
        int meetingNodesSize = meetingNodes.size();

        return "hub size: " + hubSize + " meetingNodesSize: " + meetingNodesSize;
    }

    public void trim(double clip) {

        Map<Integer, Map<Integer, Double>> newMap = new HashMap<Integer, Map<Integer, Double>>();
        List<Integer> newHublist = new ArrayList<Integer>();
        List<Integer> newXlist = new ArrayList<Integer>();

        for (int l : map.keySet()) {
            Map<Integer, Double> pairMap = map.get(l);
            Map<Integer, Double> newPairs = new HashMap<Integer, Double>();
            for (int nid : pairMap.keySet()) {
                double score = pairMap.get(nid);
                if (score > clip) {
                    newPairs.put(nid, score);
                    if (hubs.contains(nid) && !newHublist.contains(nid))
                        newHublist.add(nid);
                    if (meetingNodes.contains(nid) && !newXlist.contains(nid))
                        newXlist.add(nid);
                }

            }

            newMap.put(l, newPairs);
        }

        this.map = newMap;
        this.hubs = newHublist;
        this.meetingNodes = newXlist;

    }

    public void saveToDisk(int id, String type, boolean doTrim) throws Exception {
        String path = "";
        if (type == "out")
            //path = "./outSim/" + Integer.toString(id);
            path = IndexManager.getIndexDeepDir() + "out/" + Integer.toString(id);
        else if (type == "in")
            //path = "./inSim/" + Integer.toString(id);
            path = IndexManager.getIndexDeepDir() + "in/" + Integer.toString(id);
        else {
            System.out.println("Type of prime graph should be either out or in.");
            System.exit(0);
        }

        //	System.out.println(path+"/"+id);

        DataWriter out = new DataWriter(path);

        if (doTrim)
            trim(Config.clip);

        out.writeInteger(hubs.size());
        for (int i : hubs) {
            out.writeInteger(i);
        }

        out.writeInteger(meetingNodes.size());
        for (int i : meetingNodes) {
            out.writeInteger(i);
        }
        out.writeInteger(map.size());
        for (int i = 0; i < map.size(); i++) {
            int pairNum = map.get(i).size();
            Map<Integer, Double> pairMap = map.get(i);
            out.writeInteger(pairNum);
            for (int j : pairMap.keySet()) {
                out.writeInteger(j);
                out.writeDouble(pairMap.get(j));
            }
        }

        out.close();
    }

    public void loadFromDisk(int id, String type) throws Exception {
        String path = "";
        if (type == "out")
            path = IndexManager.getIndexDeepDir() + "out/" + Integer.toString(id);
        else if (type == "in")
            path = IndexManager.getIndexDeepDir() + "in/" + Integer.toString(id);
        else {
            System.out.println("Type of prime graph should be either out or in.");
            System.exit(0);
        }


        //==============

        DataReader in = new DataReader(path);

        int n = in.readInteger();
        this.hubs = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++)
            this.hubs.add(in.readInteger());

        int numM = in.readInteger();
        this.meetingNodes = new ArrayList<Integer>(numM);
        for (int i = 0; i < numM; i++)
            this.meetingNodes.add(in.readInteger());

        int numL = in.readInteger();
        for (int i = 0; i < numL; i++) {
            int numPair = in.readInteger();
            Map<Integer, Double> pairMap = new HashMap<Integer, Double>();
            for (int j = 0; j < numPair; j++) {
                int nodeId = in.readInteger();
                double nodeScore = in.readDouble();
                pairMap.put(nodeId, nodeScore);
            }
            this.map.put(i, pairMap);
        }


        in.close();


    }

    public PrimeSim duplicate() {
        // TODO Auto-generated method stub
        PrimeSim sim = new PrimeSim();
        sim.map.putAll(this.map);
        return sim;

    }

    public void addFrom(PrimeSim nextOut, Map<Integer, Double> oneHubValue) {
        // TODO Auto-generated method stub
        for (int lenToHub : oneHubValue.keySet()) {
            double hubScoreoflen = oneHubValue.get(lenToHub);
            for (int lenFromHub : nextOut.getMap().keySet()) {
                if (lenFromHub == 0) {
                    // the new score of hub (over length==0) is just the score on prime graph
                    continue;
                }
                int newLen = lenToHub + lenFromHub;
                if (!this.getMap().containsKey(newLen))
                    this.getMap().put(newLen, new HashMap<Integer, Double>());
                for (int toNode : nextOut.getMap().get(lenFromHub).keySet()) {
                    double oldValue = this.getMap().get(newLen).keySet()
                            .contains(toNode) ? this.getMap().get(newLen).get(toNode) : 0.0;
                    double newValue = hubScoreoflen * nextOut.getMap().get(lenFromHub).get(toNode);
                    this.getMap().get(newLen).put(toNode, oldValue + newValue);
                }
            }
        }

    }

    public void addMeetingNodes(List<Integer> nodes) {

        for (int nid : nodes) {
            if (!this.meetingNodes.contains(nid))
                this.meetingNodes.add(nid);
        }
    }

}
