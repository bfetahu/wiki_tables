
package fastSim.exec;

import fastSim.core.QueryProcessor;
import fastSim.data.Graph;
import fastSim.data.Node;
import fastSim.util.Config;


public class OnlineTest {
    public static void main(String args[]) throws Exception {
        //init parameters
        Config.hubType = args[0];
        Config.numHubs = Integer.parseInt(args[1]);
        Config.eta = Integer.parseInt(args[2]);
        int qid1 = Integer.parseInt(args[3]);
        int qid2 = Integer.parseInt(args[4]);
        Graph graph = new Graph();
        graph.loadFromFile(Config.nodeFile, Config.edgeFile, true);

        System.out.println("Starting query processing...");
        QueryProcessor qp = new QueryProcessor(graph);
        Node q1 = graph.getNode(qid1);
        Node q2 = graph.getNode(qid2);


        //List<KeyValuePair> rankedResult = null;
        long start = System.currentTimeMillis();

        System.out.println(qp.query(q1, q2));

        long elapsed = (System.currentTimeMillis() - start);


        System.out.println("Time: " + elapsed);


    }
}
