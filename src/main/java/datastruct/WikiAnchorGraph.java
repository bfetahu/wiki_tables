package datastruct;

import com.google.common.collect.Sets;
import edu.uci.ics.jung.graph.util.Pair;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by besnik on 2/7/18.
 */
public class WikiAnchorGraph {
    public Set<Pair<Integer>> pairs = Sets.newConcurrentHashSet();

    public TIntObjectHashMap<TIntHashSet> out_links;
    public TIntObjectHashMap<TIntHashSet> in_links;

    public TIntObjectHashMap<TIntDoubleHashMap> sim_rank_scores;

    //keep the entity entries in a hash-index
    public Map<Integer, String> entities;

    public WikiAnchorGraph() {
        out_links = new TIntObjectHashMap<>();
        in_links = new TIntObjectHashMap<>();
        sim_rank_scores = new TIntObjectHashMap<>();
        entities = new HashMap<>();
    }


    /**
     * Load the Wikipedia anchor graph. We build the set of in-links and out-links for each article.
     *
     * @param anchor_graph_data
     * @throws IOException
     */
    public void loadInOutDegreeAnchorGraph(String anchor_graph_data) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(anchor_graph_data);
        String line;

        int total = 0;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            total++;
            String[] data = line.split("\t");

            //get the out-links of an article
            String article = data[0];
            int article_hash = article.hashCode();

            if (!entities.containsKey(article_hash)) {
                entities.put(article_hash, article);
            }

            if (!out_links.containsKey(article_hash)) {
                out_links.put(article_hash, new TIntHashSet());
            }

            TIntHashSet article_out_links = out_links.get(article_hash);
            for (int i = 1; i < data.length; i++) {
                String out_article = data[i];
                int out_article_hash = out_article.hashCode();

                if (!entities.containsKey(out_article_hash)) {
                    entities.put(out_article_hash, out_article);
                }
                article_out_links.add(out_article_hash);


                //add these as in-links from the following article.
                if (!in_links.containsKey(out_article_hash)) {
                    in_links.put(out_article_hash, new TIntHashSet());
                }
                in_links.get(out_article_hash).add(article_hash);
            }


            if (total % 10000 == 0) {
                System.out.println("There have been " + total + " entities parsed.");
            }
        }

        System.out.println("Finished loading the Wikipedia graph.");
    }

    /**
     * Load the Wikipedia anchor graph. We build the set of in-links and out-links for each article.
     *
     * @param anchor_graph_data
     * @throws IOException
     */
    public void loadInDegreeAnchorGraph(String anchor_graph_data, String out_dir) throws IOException {
        boolean has_graph_data = readAnchorObjectData(out_dir);
        if (has_graph_data) {
            return;
        }

        BufferedReader reader = FileUtils.getFileReader(anchor_graph_data);
        String line;

        int total = 0;
        int skip = 0;
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            total++;
            String[] data = line.split("\t");

            //get the out-links of an article
            String article = data[0];
            if (article.contains("File:") || article.contains("Category:") || article.contains(":")) {
                skip++;
                continue;
            }
            int article_hash = article.hashCode();

            if (!entities.containsKey(article_hash)) {
                entities.put(article_hash, article);
            }

            for (int i = 1; i < data.length; i++) {
                String out_article = data[i];
                if (out_article.contains("File:") || out_article.contains("Category:") || out_article.contains(":")) {
                    skip++;
                    continue;
                }
                int out_article_hash = out_article.hashCode();

                if (!entities.containsKey(out_article_hash)) {
                    entities.put(out_article_hash, out_article);
                }
                //add these as in-links from the following article.
                if (!in_links.containsKey(out_article_hash)) {
                    in_links.put(out_article_hash, new TIntHashSet());
                }
                in_links.get(out_article_hash).add(article_hash);
            }

            if (total % 100000 == 0) {
                System.out.println("There have been " + total + " entities parsed.");
            }
        }

        writeAnchorGraph(out_dir);
        System.out.printf("Finished loading the Wikipedia graph with %d entities and skipped %d invalid anchors.\n", entities.size(), skip);
    }

    /**
     * Compute the SimRank between any two articles which have incoming connections in the Wikipedia anchor graph.
     */
    public void computeGraphSimRank(double epsilon, int max_iter) {
        //we compute the SimRank score for all article pairs that have incoming edges, for the rest the score is set to zero.
        int[] articles = in_links.keys();
        for (int k = 0; k < max_iter; k++) {
            TIntObjectHashMap<TIntDoubleHashMap> sim_rank_scores_tmp = new TIntObjectHashMap<>();

            if (pairs != null && !pairs.isEmpty()) {
                System.out.printf("Computing SimRank scores for the %d-th iteration for %d pairs.\n", k, pairs.size());
                AtomicInteger atm = new AtomicInteger();
                pairs.stream().parallel().forEach(pair -> {
                    int count = atm.incrementAndGet();
                    if (count % 10000 == 0) {
                        System.out.println("Finished processing " + count + " entities.");
                    }
                    computeSimRank(pair.getFirst(), pair.getSecond(), epsilon, sim_rank_scores_tmp);
                });
            } else {
                System.out.printf("Computing SimRank scores for the %d-th iteration for %d pairs.\n", k, articles.length * articles.length);
                for (int i = 0; i < articles.length; i++) {
                    int article_a = articles[i];
                    for (int j = 0; j < articles.length; j++) {
                        int article_b = articles[j];
                        computeSimRank(article_a, article_b, epsilon, sim_rank_scores_tmp);
                    }
                }
            }

            sim_rank_scores = sim_rank_scores_tmp;
        }
    }

    /**
     * Initialize the scores.
     */
    public void initialize() {
        int[] articles = in_links.keys();
        for (int i = 0; i < articles.length; i++) {
            int article_a = articles[i];

            //update the scores
            if (!sim_rank_scores.containsKey(article_a)) {
                sim_rank_scores.put(article_a, new TIntDoubleHashMap());
            }
            sim_rank_scores.get(article_a).put(article_a, 1.0);
        }
    }

    /**
     * Compute recursive the SimRank score between any pair of entities.
     *
     * @param article_a
     * @param article_b
     * @return
     */
    public void computeSimRank(int article_a, int article_b, double epsilon, TIntObjectHashMap<TIntDoubleHashMap> sim_rank_scores_new) {
        double score = 0;

        //the in-links for the pair of articles
        TIntHashSet in_links_a = in_links.get(article_a);
        TIntHashSet in_links_b = in_links.get(article_b);

        for (int link_a : in_links_a.toArray()) {
            for (int link_b : in_links_b.toArray()) {
                score += sim_rank_scores.containsKey(link_a) && sim_rank_scores.get(link_a).containsKey(link_b) ? sim_rank_scores.get(link_a).get(link_b) : 0.0;
            }
        }
        score *= epsilon / (in_links_a.size() * in_links_b.size());

        //update the scores
        if (!sim_rank_scores_new.containsKey(article_a)) {
            sim_rank_scores_new.put(article_a, new TIntDoubleHashMap());
        }
        sim_rank_scores_new.get(article_a).put(article_b, score);
    }

    /**
     * Write the computed SimRank scores.
     *
     * @param out_dir
     */
    public void writeSimRankScores(String out_dir) {
        String out_file = out_dir + "/enwiki_simrank.tsv";
        FileUtils.saveText("", out_file);
        StringBuffer sb = new StringBuffer();
        for (int article_a : sim_rank_scores.keys()) {
            String article_a_label = entities.get(article_a);
            for (int article_b : sim_rank_scores.get(article_a).keys()) {
                String article_b_label = entities.get(article_b);
                sb.append(article_a_label).append("\t").append(article_b_label).append("\t").append(sim_rank_scores.get(article_a).get(article_b)).append("\n");

                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
    }

    public static void main(String[] args) throws IOException {
        String anchor_data = "", filter_data = "", out_dir = "";
        double damping_factor = 0.6;
        int iterations = 5;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-anchor_data")) {
                anchor_data = args[++i];
            } else if (args[i].equals("-filter_data")) {
                filter_data = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-damping")) {
                damping_factor = Double.valueOf(args[++i]);
            } else if (args[i].equals("-iterations")) {
                iterations = Integer.valueOf(args[++i]);
            }
        }
        WikiAnchorGraph w = new WikiAnchorGraph();
        System.out.println("Loading Wikipedia in-degree anchor graph.");
        w.loadInDegreeAnchorGraph(anchor_data, out_dir);

        if (!filter_data.isEmpty()) {
            System.out.println("Loading the filters for which we wanna compute the sim-rank scores.");
            w.readEntityFilterFiles(filter_data);
        }
        System.out.println("Initializing the feature weights.");
        w.initialize();
        w.computeGraphSimRank(damping_factor, iterations);
        w.writeSimRankScores(out_dir);
    }

    /**
     * Read the entities for which we are interested in computing the scores.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public void readEntityFilterFiles(String file) throws IOException {
        String line;
        BufferedReader reader = FileUtils.getFileReader(file);

        Set<Integer> trace = Sets.newConcurrentHashSet();
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            int article_a = data[1].hashCode();
            int article_b = data[2].hashCode();
            System.out.println("Processing " + data[1] + "\t" + data[2]);
            gatherAllRelevantPairs(pairs, article_a, article_b, trace);

            System.out.printf("Finished retrieving pairs for %s\t%s, with a total of %d pairs.\n", data[1], data[2], pairs.size());
            trace.clear();
        }
    }


    /**
     * Read the entities for which we are interested in computing the scores.
     *
     * @param entity_pairs
     * @return
     * @throws IOException
     */
    public void readEntityFilterFiles(Map<String, Set<String>> entity_pairs) throws IOException {
        entity_pairs.keySet().parallelStream().forEach(article_a -> {
            Set<Integer> trace = Sets.newConcurrentHashSet();
            for (String article_b : entity_pairs.get(article_a)) {
                System.out.println("Processing " + article_a + "\t" + article_b);
                gatherAllRelevantPairs(pairs, article_a.hashCode(), article_b.hashCode(), trace);

                System.out.printf("Finished retrieving pairs for %s\t%s, with a total of %d pairs.\n", article_a, article_b, pairs.size());
                trace.clear();
            }
        });
    }

    /**
     * Recursively add all the possible pairs for which we need to compute the SimRank scores. In case we have some
     * nodes for which we are interested in computing the SimRank, we need to compute the corresponding SimRank scores
     * for all their in-degree nodes.
     * <p>
     * Since the graph contains cycles we need to make sure that we stop the moment we encounter one of our original
     * starting points in the graph.
     *
     * @param pairs
     * @param article_a
     * @param article_b
     */
    public void gatherAllRelevantPairs(Set<Pair<Integer>> pairs, int article_a, int article_b, Set<Integer> trace) {
        if (trace.contains(article_a) || trace.contains(article_b)) {
            return;
        }
        trace.add(article_a);
        trace.add(article_b);

        TIntHashSet in_links_a = in_links.get(article_a);
        TIntHashSet in_links_b = in_links.get(article_b);

        Pair<Integer> pair = new Pair<>(article_a, article_b);
        //if this pair has been already added do not continue in the recursion step.
        if (pairs.contains(pair)) {
            return;
        }
        pairs.add(pair);

        if (pairs.size() % 100000 == 0) {
            System.out.printf("Currently there are %d pairs for computing SimRank.\n", pairs.size());
        }

        //we get recursively all the in-link nodes as possible pairs for which we need to compute the SimRank scores
        if (in_links_a == null || in_links_a.isEmpty() || in_links_b == null || in_links_b.isEmpty()) {
            return;
        }
        Arrays.stream(in_links_a.toArray()).parallel().forEach(in_link_a -> {
            for (int in_link_b : in_links_b.toArray()) {
                gatherAllRelevantPairs(pairs, in_link_a, in_link_b, trace);
            }
        });
    }

    /**
     * Writes the data object into an output folder.
     *
     * @param out_dir
     */
    public void writeAnchorGraph(String out_dir) {
        FileUtils.checkDir(out_dir + "/graph_object/");

        FileUtils.saveObject(in_links, out_dir + "/graph_object/in-links.obj");
        FileUtils.saveObject(entities, out_dir + "/graph_object/entity-dict.obj");

        System.out.println("Finished writing the objects for the anchor graph.");
    }

    /**
     * Read the anchor graph data objects.
     *
     * @param out_dir
     */
    public boolean readAnchorObjectData(String out_dir) {
        if (FileUtils.fileDirExists(out_dir + "/graph_object/")) {
            in_links = (TIntObjectHashMap<TIntHashSet>) FileUtils.readObject(out_dir + "/graph_object/in-links.obj");
            entities = (Map<Integer, String>) FileUtils.readObject(out_dir + "/graph_object/entity-dict.obj");
            return true;
        }
        return false;
    }
}
