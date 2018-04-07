package fastSim.data;

import fastSim.util.Config;
import fastSim.util.IndexManager;
import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Graph implements Serializable {

    protected Map<Integer, Node> nodes;
    protected Set<Node> hubs;
    private Random rnd;
    private final double ITER_STOP = Config.stopRea; //0.001;
    private final double DEPTH = Config.depth;
    private final int maxNode = Config.maxNode;

    public Graph() {
        nodes = new HashMap<>();
        rnd = new Random(9876804367L);
    }

    public void clear() {
        nodes.clear();
    }


    private void loadGraphFromFile(String nodeFile, String edgeFile) throws Exception {
        clear();

        BufferedReader inN = FileUtils.getFileReader(nodeFile);
        BufferedReader inE = FileUtils.getFileReader(edgeFile);
        String line;

        System.out.print("Loading graph");
        int count = 0;
        while ((line = inN.readLine()) != null) {
            String[] data = line.split("\t");
            if (data.length != 2) {
                continue;
            }
            int id = Integer.parseInt(data[1]);
            this.addNode(new Node(id, count));
            count++;
            if (count % 1000000 == 0)
                System.out.print(".");
        }

        while ((line = inE.readLine()) != null) {
            String[] split = line.split("\t");
            int from = Integer.parseInt(split[0]);
            int to = Integer.parseInt(split[1]);
            this.addEdge(from, to);
        }
        System.out.println();

        inN.close();
        inE.close();

        init();
    }

    public void loadFromFile(String nodeFile, String edgeFile, boolean identifyHubs) throws Exception {
        loadGraphFromFile(nodeFile, edgeFile);

        if (identifyHubs) {
            String hubNodeFile = IndexManager.getHubNodeFile();
            loadHubs(hubNodeFile);
        }

    }

    public void loadHubs(String hubNodeFile) throws Exception {
        BufferedReader in = FileUtils.getFileReader(hubNodeFile);
        String line;

        hubs = new HashSet<>();

        while ((line = in.readLine()) != null) {
            if (hubs.size() == Config.numHubs)
                break;
            int id = Integer.parseInt(line);
            Node n = getNode(id);
            if (n == null)
                n = new Node(id);
            n.isHub = true;
            hubs.add(n);
        }
        in.close();
    }

    public Set<Node> getHubs() {
        return hubs;
    }

    public void addNode(Node n) {
        nodes.put(n.id, n);
    }

    public void addEdge(int from, int to) {
        Node nFrom = getNode(from);
        Node nTo = getNode(to);
        nFrom.out.add(nTo);
        nTo.in.add(nFrom);
    }

    public void resetPrimeG() {
        nodes.values().parallelStream().forEach(node -> node.isVisited = false);
    }

    public PrimeSim computeOutPrimeSim(Node q) throws IOException {
        PrimeSim outSim = new PrimeSim();
        List<Node> expNodes = new ArrayList<Node>();
        Map<Integer, Double> valInLastLevel = new HashMap<Integer, Double>();

        expNodes.add(q);

        q.isVisited = true; //don't save query node as hub or meeting node
        outSim.addNewNode(q, "out");
        outSim.set(0, q, 1.0);
        valInLastLevel.put(q.id, 1.0);

        int length = 1;
        while (length <= DEPTH) {

            if (expNodes == null)
                break;
            List<Node> newExpNodes = new ArrayList<Node>();
            Map<Integer, Double> valInThisLevel = new HashMap<Integer, Double>();
            for (Node cur : expNodes) {
                //System.out.println("current node: " + cur.id);
                for (Node n : cur.out) { // an edge cur-->n, where cur is meeting node, so for n, should store the Reversed reachability: R(n->cur)=1*1/In(n)
                    //System.out.println("out:" + n.id);
                    if (!n.isVisited)
                        outSim.addNewNode(n, "out");
                    double rea = valInLastLevel.get(cur.id) / n.in.size() * Math.sqrt(Config.alpha); //*(Config.alpha)2.26: ensure the prime subgraphs are not too large
                    //know the reachability when expanding hubs
                    //	double rea = valInLastLevel.get(cur.id)/n.in.size();//modified 8.27, don't multiply alpha at this time, otherwise will double multiply alpha^length as another tour also has this alpha, so leave it for online merging.
                    if (rea > ITER_STOP) {
                        if (valInThisLevel.get(n.id) == null) {
                            //System.out.println("value:" + rea);
                            valInThisLevel.put(n.id, rea);
                            if (n.out.size() > 0 && !n.isHub)
                                newExpNodes.add(n);

                        } else
                            valInThisLevel.put(n.id, valInThisLevel.get(n.id) + rea);
                    }
                }
            }

            outSim.set(length, valInThisLevel);
            expNodes = newExpNodes;
            valInLastLevel = valInThisLevel;

            length++;
        }
        List<Integer> toRemoveLength = new ArrayList<Integer>();
        for (int l : outSim.getMap().keySet()) {
            if (outSim.getMap().get(l).size() == 0) {
                toRemoveLength.add(l);
            }
        }
        for (int i : toRemoveLength)
            outSim.getMap().remove(i);

        return outSim;
    }


    public PrimeSim computeInPrimeSim(Node q) throws IOException {
        PrimeSim inSim = new PrimeSim();

        List<Node> expNodes = new ArrayList<Node>();
        //List<Node> newExpNodes = new ArrayList <Node>();
        Map<Integer, Double> valInLastLevel = new HashMap<Integer, Double>();
        //Map<Integer,Double> valInThisLevel = new HashMap<Integer,Double>();

        expNodes.add(q); // Nodes to be expanded, initially only q
        //add 2/23/2015
        inSim.addNewNode(q, "in");
        //q.isVisited = true;
        inSim.set(0, q, 1.0);
        valInLastLevel.put(q.id, 1.0);

        int length = 1;
        while (length <= DEPTH) {
            if (expNodes == null)
                break;
            List<Node> newExpNodes = new ArrayList<>();
            Map<Integer, Double> valInThisLevel = new HashMap<Integer, Double>();
            for (Node cur : expNodes) {
                for (Node n : cur.in) { //a edge n-->cur, if R(cur)==a, then R(n)==1/In(cur)*a, because of the reversed walk from cur to n.
                    if (!n.isVisited)
                        inSim.addNewNode(n, "in"); // mark n as visited and add n to meetingNodes in PrimeSim
                    double rea = valInLastLevel.get(cur.id) / cur.in.size() * Math.sqrt(Config.alpha); //
                    if (rea > ITER_STOP) {
                        if (valInThisLevel.get(n.id) == null) {
                            valInThisLevel.put(n.id, rea);
                            if (n.in.size() > 0 && !n.isHub)
                                newExpNodes.add(n);
                        } else
                            valInThisLevel.put(n.id, valInThisLevel.get(n.id) + rea);
                    }

                }
            }

            inSim.set(length, valInThisLevel);

            expNodes = newExpNodes;
            valInLastLevel = valInThisLevel;
            length++;
        }
        List<Integer> toRemoveLen = new ArrayList<Integer>();
        for (int l : inSim.getMap().keySet()) {
            if (inSim.getMap().get(l).size() == 0) {
                toRemoveLen.add(l);
            }
        }
        for (int i : toRemoveLen)
            inSim.getMap().remove(i);

        return inSim;
    }


    public int numNodes() {
        return nodes.size();
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public Collection<Node> getNodes() {
        return nodes.values();
    }

    public boolean containsNode(int id) {
        return nodes.containsKey(id);
    }


    public void init() {
        for (Node n : nodes.values())
            n.initOutEdgeWeight();
    }
}
