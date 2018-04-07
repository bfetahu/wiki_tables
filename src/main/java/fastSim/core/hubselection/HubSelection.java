package fastSim.core.hubselection;

import fastSim.data.Graph;
import fastSim.util.Config;
import fastSim.util.IndexManager;
import fastSim.util.KeyValuePair;
import fastSim.util.io.TextWriter;

import java.util.Collections;
import java.util.List;

public abstract class HubSelection {

    protected List<KeyValuePair> nodes;
    protected Graph graph;

    protected abstract void fillNodes();

    public HubSelection(String nodeFile, String edgeFile) throws Exception {
        graph = new Graph();
        graph.loadFromFile(nodeFile, edgeFile, false);
    }

    public void sortNodes() {
        Collections.sort(nodes, (arg0, arg1) -> -Double.compare(arg0.value, arg1.value));
    }

    public void save() throws Exception {
        fillNodes();

        IndexManager.mkSubDirShallow();

        sortNodes();

        if (nodes.size() > Config.hubTop)
            nodes = nodes.subList(0, Config.hubTop);

        TextWriter out = new TextWriter(IndexManager.getHubNodeFile());
        for (KeyValuePair n : nodes)
            out.writeln(n.key);
        out.close();
    }
}
