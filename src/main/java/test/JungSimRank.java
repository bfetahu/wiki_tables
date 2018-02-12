package test;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.Pair;

import java.util.*;

public class JungSimRank {
    public static void main(String[] args) {
        Graph<String, String> g = new DirectedSparseGraph<>();

        String[] nodes = "a,b,c,d".split(",");
        for (String node : nodes) {
            g.addVertex(node);
        }
        String[] edges = ("d\ta\n" + "d\tb\n" + "a\tb\n" + "a\tc\n" + "b\tc\n" + "c\td").split("\n");
        for (String edge : edges) {
            String[] data = edge.split("\t");
            g.addEdge(edge, data[0], data[1]);
        }

        Map<Pair<String>, Double> simRank = computeSimRank(g);
        print(g, simRank);
    }


    /**
     * Compute the SimRank for the vertices of the given graph.
     *
     * @param <V> The vertex type
     * @param g   The graph
     * @return The SimRank, as a map from pairs of vertices to
     * their similarity
     */
    private static Map<Pair<String>, Double> computeSimRank(Graph<String, String> g) {
        final int kMax = 2;
        final double C = 0.8;

        Map<Pair<String>, Double> currentR = computeInitialSimRank(g);
        Map<Pair<String>, Double> nextR = new LinkedHashMap<>();
        for (int k = 0; k < kMax; k++) {
            System.out.println("Iteration " + k);
            for (String a : g.getVertices()) {
                for (String b : g.getVertices()) {
                    double sum = computeSum(g, a, b, currentR);
                    Pair<String> ab = new Pair<>(a, b);
                    int sia = g.inDegree(a);
                    int sib = g.inDegree(b);
                    if (sia == 0 || sib == 0) {
                        nextR.put(ab, 0.0);
                    } else {
                        nextR.put(ab, C / (sia * sib) * sum);
                    }
                }
            }

            Map<Pair<String>, Double> temp = nextR;
            nextR = currentR;
            currentR = temp;
        }
        return currentR;
    }

    /**
     * Compute the sum of all SimRank values of all incoming
     * neighbors of the given vertices
     *
     * @param <V>     The vertex type
     * @param g       The graph
     * @param a       The first vertex
     * @param b       The second vertex
     * @param simRank The current SimRank
     * @return The sum of the SimRank values of the
     * incoming neighbors of the given vertices
     */
    private static double computeSum(Graph<String, String> g, String a, String b, Map<Pair<String>, Double> simRank) {
        Collection<String> ia = g.getPredecessors(a);
        Collection<String> ib = g.getPredecessors(b);
        float sum = 0;
        for (String iia : ia) {
            for (String ijb : ib) {
                Pair<String> key = new Pair<>(iia, ijb);
                double r = simRank.get(key);
                sum += r;
            }
        }
        return sum;
    }

    /**
     * Compute the initial SimRank for the vertices of the given graph.
     * This initial SimRank for two vertices (a,b) is 0.0f when
     * a != b, and 1.0f when a == b
     *
     * @param <V> The vertex type
     * @param g   The graph
     * @return The SimRank, as a map from pairs of vertices to
     * their similarity
     */
    private static Map<Pair<String>, Double> computeInitialSimRank(Graph<String, ?> g) {
        Map<Pair<String>, Double> R0 = new LinkedHashMap<>();
        for (String a : g.getVertices()) {
            for (String b : g.getVertices()) {
                Pair<String> ab = new Pair<>(a, b);
                if (a.equals(b)) {
                    R0.put(ab, 1.0);
                } else {
                    R0.put(ab, 0.0);
                }
            }
        }
        return R0;
    }

    /**
     * Print a table with the SimRank values
     *
     * @param <V>     The vertex type
     * @param g       The graph
     * @param simRank The SimRank
     */
    private static void print(Graph<String, String> g, Map<Pair<String>, Double> simRank) {
        final int columnWidth = 8;
        final String format = "%4.3f";

        List<String> vertices = new ArrayList<>(g.getVertices());
        System.out.printf("%" + columnWidth + "s", "");
        for (int j = 0; j < vertices.size(); j++) {
            String s = vertices.get(j);
            System.out.printf("%" + columnWidth + "s", s);
        }
        System.out.println();

        for (int i = 0; i < vertices.size(); i++) {
            String s = vertices.get(i);
            System.out.printf("%" + columnWidth + "s", s);
            for (int j = 0; j < vertices.size(); j++) {
                String a = vertices.get(i);
                String b = vertices.get(j);
                Pair<String> ab = new Pair<>(a, b);
                double value = simRank.get(ab);
                String vs = String.format(Locale.ENGLISH, format, value);
                System.out.printf("%" + columnWidth + "s", vs);
            }
            System.out.println();
        }
    }

}