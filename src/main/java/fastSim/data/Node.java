package fastSim.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * node structure.
 *
 * @author zfw
 */
public class Node implements Serializable {

    public int id;
    public boolean isHub = false;
    public boolean isVisited = false;
    public int index = -1;

    public List<Node> out;
    public List<Node> in;

    //public List<Node> inInSub;

    public List<Integer> outId;
    public List<Integer> inId;

    public double outEdgeWeight = 0;
    public double outEdgeWeightInSub = 0;
    public double vOld = 0;
    public double vNew = 0;

    public Node(int id) {
        this.id = id;
        out = new ArrayList<Node>();
        in = new ArrayList<Node>();
    }

    public Node(int id, int index) {
        this.id = id;
        this.index = index;
        out = new ArrayList<Node>();
        in = new ArrayList<Node>();
    }

    @Override
    public int hashCode() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        Node n = (Node) o;
        return n.id == this.id;
    }


    public void initOutEdgeWeight() {
        if (out.size() > 0)
            outEdgeWeight = 1.0 / out.size();
        else
            outEdgeWeight = 0;
    }

    public void initOutEdgeWeightUsingNeighborId() {
        if (outId.size() > 0)
            outEdgeWeight = 1.0 / outId.size();
        else
            outEdgeWeight = 0;
    }


}
