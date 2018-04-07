package fastSim.exec;

import fastSim.core.QueryProcessor;
import fastSim.data.Graph;
import fastSim.data.Node;
import fastSim.util.Config;
import fastSim.util.io.TextReader;
import io.FileUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Online {
    public static void main(String args[]) throws Exception {
        //init parameters
        Config.hubType = args[0];
        Config.numHubs = Integer.parseInt(args[1]);
        Config.depth = Integer.parseInt(args[2]);
        Config.delta = Double.parseDouble(args[3]);
        Config.stopRea = Double.parseDouble(args[4]);
        Config.eta = Integer.parseInt(args[5]);

        int num_threads = Integer.parseInt(args[6]);

        Map<Integer, Graph> graphs = loadMultipleGraphs(num_threads);
        Graph graph = graphs.get(0);

        //load queries
        System.out.println("Loading queries...");
        List<Node[]> qNodes = new ArrayList<>();
        TextReader in = new TextReader(Config.queryFile);
        String line;

        while ((line = in.readln()) != null) {
            String[] parts = line.split("\t");

            int a = Integer.parseInt(parts[0]);
            int b = Integer.parseInt(parts[1]);
            qNodes.add(new Node[]{graph.getNode(a), graph.getNode(b)});
        }
        in.close();

        Map<Integer, List<Node[]>> parallelQNodes = chunkNodes(qNodes, num_threads);

        System.out.println("Starting query processing...");
        String out_file = Config.outputDir + "fs_" + Config.hubType + "_H" + Config.numHubs + "_Depth" + Config.depth + "_theta" + Config.stopRea + "_delta" + Config.delta + "_OfflineClip" + Config.clip + "_eta" + Config.eta;

        Map<Integer, Set<Integer>> finished_pairs = loadFinishedPairs();
        AtomicInteger atm = new AtomicInteger();
        StringBuffer sb = new StringBuffer();
        graphs.keySet().parallelStream().forEach(k -> {
            List<Node[]> nodes = parallelQNodes.get(k);
            Graph g = graphs.get(k);
            QueryProcessor qp = new QueryProcessor(g);

            for (Node[] qPair : nodes) {
                //if it has been computed before, skip it.
                if (finished_pairs.containsKey(qPair[0].id) && finished_pairs.get(qPair[0].id).contains(qPair[1].id)) {
                    continue;
                }
                try {
                    int tmp = atm.incrementAndGet();
                    if (tmp % 100 == 0)
                        System.out.print("+");

                    long start = System.currentTimeMillis();
                    double result = qp.query(qPair[0], qPair[1]);
                    long elapsed = (System.currentTimeMillis() - start);

                    sb.append(qPair[0].id).append("\t").append(qPair[1].id).append("\t").append(result).append("\n");

                    if (sb.length() > 10000) {
                        FileUtils.saveText(sb.toString(), out_file, true);
                        sb.delete(0, sb.length());
                    }
                    System.out.printf("Finished computing SimRank for (%d, %d)=" + result + " in %d ms\n", qPair[0].id, qPair[1].id, elapsed);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        FileUtils.saveText(sb.toString(), out_file, true);

        System.out.println();
    }

    /**
     * Clone the graph for parallel computation.
     *
     * @param g
     * @param num_threads
     * @return
     */
    public static Map<Integer, Graph> loadMultipleGraphs(int num_threads) throws Exception {
        Map<Integer, Graph> map = new HashMap<>();
        for (int i = 0; i < num_threads; i++) {
            Graph graph = new Graph();
            graph.loadFromFile(Config.nodeFile, Config.edgeFile, true);
            map.put(i, graph);
        }
        System.out.printf("There are in total %d chunks of nodes.\n", map.size());
        return map;
    }

    public static Map<Integer, List<Node[]>> chunkNodes(List<Node[]> query_nodes, int num_threads) {
        Map<Integer, List<Node[]>> map = new HashMap<>();

        int chunk_size = query_nodes.size() / num_threads;
        int k = 0;

        List<Node[]> nodes = new ArrayList<>();
        for (int i = 0; i < query_nodes.size(); i++) {
            if (i % chunk_size == 0) {
                map.put(k, new ArrayList<>(nodes));
                k++;
                nodes.clear();
            }
            Node[] node = query_nodes.get(i);
            nodes.add(node);
        }
        int prev_k = k - 1;
        map.get(prev_k).addAll(nodes);
        System.out.printf("There are in total %d chunks of nodes.\n", k);
        return map;
    }

    public static Map<Integer, Set<Integer>> loadFinishedPairs() {
        String[] lines = FileUtils.readText("finished_pairs.tsv").split("\n");
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (String line : lines) {
            String[] tmp = line.trim().split("\t");
            if (tmp.length != 2) {
                continue;
            }

            try {
                int a = Integer.valueOf(tmp[0]);
                int b = Integer.valueOf(tmp[1]);

                if (!map.containsKey(a)) {
                    map.put(a, new HashSet<>());
                }
                map.get(a).add(b);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.printf("There are %d pairs", lines.length);
        return map;
    }
}
