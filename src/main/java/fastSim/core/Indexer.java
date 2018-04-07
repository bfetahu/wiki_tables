package fastSim.core;

import fastSim.data.Graph;
import fastSim.data.Node;
import fastSim.data.PrimeSim;
import fastSim.util.Config;
import fastSim.util.IndexManager;
import fastSim.util.io.TextWriter;

import java.io.File;
import java.util.Set;

public class Indexer {

    public static void index(Set<Node> hubs, Graph graph, boolean forceUpdate)
            throws Exception {

        // 1. compute primeOutSimrank for hubs and record the statistics
        long storage = 0;

        long start = System.currentTimeMillis();

        int count = 0;
        StringBuilder sb = new StringBuilder();

        for (Node h : hubs) {
            if (forceUpdate || !(new File(IndexManager.getPrimeSimFilename(h.id, true))).exists()) {
                count++;
                if (count % 100 == 0)
                    System.out.print("+");

                graph.resetPrimeG();
                PrimeSim outSim = graph.computeOutPrimeSim(h);

                sb.append("HubID: " + h.id + " " + outSim.getCountInfo() + " \n ");
                storage += outSim.computeStorageInBytes();

                //	TO DO: save outSim
                outSim.saveToDisk(h.id, "out", true);
            }
        }

        TextWriter countWriter = new TextWriter(IndexManager.getIndexPPVCountInfoFilename(true));
        countWriter.write(sb.toString());
        countWriter.close();

        long time = System.currentTimeMillis() - start;

        TextWriter out = new TextWriter(IndexManager.getIndexDeepDir()
                + "_Depth" + Config.depth + "_theta" + Config.stopRea + "_outStats.txt");
        out.writeln("Space (mb): " + (storage / 1024.0 / 1024.0));
        out.writeln("Time (hr): " + (time / 1000.0 / 3600.0));
        out.close();


        // 2. compute primeInSimrank for hubs and record the statistics
        int count2 = 0;
        long storage2 = 0;
        long start2 = System.currentTimeMillis();
        StringBuilder sb2 = new StringBuilder();


        for (Node h : hubs) {
            if (forceUpdate || !(new File(IndexManager.getPrimeSimFilename(h.id, false))).exists()) {
                count2++;
                if (count2 % 100 == 0)
                    System.out.print("+");
                graph.resetPrimeG();

                PrimeSim inSim = graph.computeInPrimeSim(h);
                sb2.append("HubID: " + h.id + " " + inSim.getCountInfo() + "  \n");
                storage2 += inSim.computeStorageInBytes();
                //TO DO: save inSim to disk
                inSim.saveToDisk(h.id, "in", true);
            }
        }


        TextWriter countWriter2 = new TextWriter(IndexManager.getIndexPPVCountInfoFilename(false));
        countWriter2.write(sb2.toString());
        countWriter2.close();

        long time2 = System.currentTimeMillis() - start2;

        TextWriter out2 = new TextWriter(IndexManager.getIndexDeepDir()
                + "_Depth" + Config.depth + "_theta" + Config.stopRea + "_inStats.txt");
        out2.writeln("Space (mb): " + (storage2 / 1024.0 / 1024.0));
        out2.writeln("Time (hr): " + (time2 / 1000.0 / 3600.0));
        out2.close();


    }
}
