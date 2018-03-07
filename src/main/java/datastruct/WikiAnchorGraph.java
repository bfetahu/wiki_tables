package datastruct;

import com.google.common.collect.Sets;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by besnik on 2/7/18.
 */
public class WikiAnchorGraph {
    public boolean isOutLinks;

    public Map<Integer, Set<Integer>> pairs_map = new ConcurrentHashMap<>();

    //keep also the links in terms of entity indexes
    public TIntObjectHashMap<TIntHashSet> in_links;

    public TIntObjectHashMap<TIntDoubleHashMap> sim_rank_scores;

    //keep the entity entries in a hash-index
    public Map<Integer, String> entities;
    public Map<String, Integer> index_entities;

    public WikiAnchorGraph() {
        in_links = new TIntObjectHashMap<>();
        sim_rank_scores = new TIntObjectHashMap<>();
        entities = new HashMap<>();
        index_entities = new HashMap<>();
    }


    /**
     * Load the valid set of Wikipedia articles.
     *
     * @param wiki_articles
     * @param isIndex       in case the file with the entities contains the entity index as well. If not then we construct the index on the fly.
     * @throws IOException
     */
    public void loadEntityIndex(String wiki_articles, boolean isIndex, String out_dir) throws IOException {
        String line;
        BufferedReader reader = FileUtils.getFileReader(wiki_articles);

        int index = 0;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            if (!isIndex) {
                index += 1;
            } else {
                index = Integer.parseInt(data[1]);
            }
            String entity = data[0];
            entities.put(index, entity);
            index_entities.put(entity, index);
        }

        if (!isIndex) {
            StringBuffer sb = new StringBuffer();
            index_entities.keySet().forEach(e -> sb.append(e).append("\t").append(index_entities.get(e)).append("\n"));

            //get the file name
            String out_file = (new File(wiki_articles)).getName();
            FileUtils.saveText(sb.toString(), out_dir + "/" + out_file + ".index");
        }
    }

    /**
     * Check if the article is a valid one. We ignore Categories, Templates, Talk etc.
     *
     * @param article
     * @return
     */
    public boolean isValidArticle(String article) {
        if (article.trim().isEmpty()) {
            return false;
        }
        boolean starts_with_lowcap = Character.isLowerCase(article.charAt(0));
        if (starts_with_lowcap || article.contains("File:") ||
                article.contains("Category:") || article.contains(":") ||
                article.contains("Template:") ||
                article.contains("Talk:") || article.contains("#") || article.endsWith(".")) {
            return false;
        }

        return true;
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
            if (isOutLinks) {
                loadInLinks(data);
            } else {
                loadOutLinks(data);
            }
            if (total % 100000 == 0) {
                System.out.println("There have been " + total + " entities parsed.");
            }
        }
        writeAnchorGraph(out_dir);
        System.out.printf("Finished loading the Wikipedia graph with %d entities and skipped %d invalid anchors.\n", entities.size(), skip);
    }

    /**
     * From the Wikipedia anchor graph load the in-links for an article.
     *
     * @param data
     */
    private void loadInLinks(String[] data) {
        String article = data[0];
        if (!index_entities.containsKey(article)) {
            return;
        }
        int article_id = index_entities.get(article);
        for (int i = 1; i < data.length; i++) {
            String out_article = data[i];
            if (!index_entities.containsKey(out_article)) {
                continue;
            }

            int out_article_id = index_entities.get(out_article);
            //add these as in-links from the following article.
            if (!in_links.containsKey(out_article_id)) {
                in_links.put(out_article_id, new TIntHashSet());
            }
            in_links.get(out_article_id).add(article_id);
        }
    }

    /**
     * From the Wikipedia anchor graph load the out-links for an article.
     *
     * @param data
     */
    private void loadOutLinks(String[] data) {
        String article = data[0];
        if (!index_entities.containsKey(article)) {
            return;
        }
        int article_id = index_entities.get(article);
        if (!in_links.containsKey(article_id)) {
            in_links.put(article_id, new TIntHashSet());
        }

        for (int i = 1; i < data.length; i++) {
            String out_article = data[i];
            if (!index_entities.containsKey(out_article)) {
                continue;
            }

            int out_article_id = index_entities.get(out_article);
            //add these as in-links from the following article.
            in_links.get(article_id).add(out_article_id);
        }
    }

    /**
     * Compute the SimRank between any two articles which have incoming connections in the Wikipedia anchor graph.
     */
    public void computeGraphSimRank(double epsilon, int max_iter) {
        //we compute the SimRank score for all article pairs that have incoming edges, for the rest the score is set to zero.
        int[] articles = in_links.keys();
        for (int k = 0; k < max_iter; k++) {
            TIntObjectHashMap<TIntDoubleHashMap> sim_rank_scores_tmp = new TIntObjectHashMap<>();

            if (pairs_map != null && !pairs_map.isEmpty()) {
                System.out.printf("Computing SimRank scores for the %d-th iteration for %d pairs.\n", k, pairs_map.size());
                AtomicInteger atm = new AtomicInteger();
                pairs_map.keySet().stream().parallel().forEach(article_a -> {
                    for (int article_b : pairs_map.get(article_a)) {
                        int count = atm.incrementAndGet();
                        if (count % 10000 == 0) {
                            System.out.println("Finished processing " + count + " entities.");
                        }
                        computeSimRank(article_a, article_b, epsilon, sim_rank_scores_tmp);
                    }
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
        AtomicInteger atm = new AtomicInteger();
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            if (data.length != 2) {
                continue;
            }

            int article_a = data[0].hashCode();
            int article_b = data[1].hashCode();
            System.out.println("Processing " + data[0] + "\t" + data[1]);
            gatherAllRelevantPairs(pairs_map, article_a, article_b, trace, atm);

            System.out.printf("Finished retrieving pairs for %s\t%s, with a total of %d pairs.\n", data[0], data[1], atm.get());
            trace.clear();
        }
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
    public void gatherAllRelevantPairs(Map<Integer, Set<Integer>> pairs, int article_a, int article_b, Set<Integer> trace, AtomicInteger atm) {
        if (trace.contains(article_a) || trace.contains(article_b)) {
            return;
        }
        trace.add(article_a);
        trace.add(article_b);

        TIntHashSet in_links_a = in_links.get(article_a);
        TIntHashSet in_links_b = in_links.get(article_b);

        //if this pair has been already added do not continue in the recursion step.
        if (pairs.containsKey(article_a) && pairs.get(article_a).contains(article_b)) {
            return;
        }

        if (!pairs.containsKey(article_a)) {
            pairs.put(article_a, new HashSet<>());
        }
        pairs.get(article_a).add(article_b);

        int tmp = atm.incrementAndGet();
        if (tmp % 100000 == 0) {
            System.out.printf("Currently there are %d pairs for computing SimRank.\n", tmp);
        }

        //we get recursively all the in-link nodes as possible pairs for which we need to compute the SimRank scores
        if (in_links_a == null || in_links_a.isEmpty() || in_links_b == null || in_links_b.isEmpty()) {
            return;
        }
        Arrays.stream(in_links_a.toArray()).parallel().forEach(in_link_a -> {
            for (int in_link_b : in_links_b.toArray()) {
                gatherAllRelevantPairs(pairs, in_link_a, in_link_b, trace, atm);
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

        if (!FileUtils.fileExists(out_dir + "/graph_object/entity-dict.obj", false)) {
            FileUtils.saveObject(entities, out_dir + "/graph_object/entity-dict.obj");
        }

        System.out.println("Finished writing the objects for the anchor graph.");
    }

    /**
     * Read the anchor graph data objects.
     *
     * @param out_dir
     */
    public boolean readAnchorObjectData(String out_dir) {
        if (FileUtils.fileDirExists(out_dir + "/graph_object/") && FileUtils.fileExists(out_dir + "/graph_object/in-links.obj", false)) {
            in_links = (TIntObjectHashMap<TIntHashSet>) FileUtils.readObject(out_dir + "/graph_object/in-links.obj");
            entities = (Map<Integer, String>) FileUtils.readObject(out_dir + "/graph_object/entity-dict.obj");

            System.out.printf("Loaded the Wikipedia anchor graph with the following stats %d.\n", entities.size());
            return true;
        }
        return false;
    }

    /**
     * Compute the Milne-Witten score for a pair of entities. Return 0 in case the entities have no in-links in common.
     *
     * @param N
     * @param article_a
     * @param article_b
     * @return
     */
    private double computeMilneWittenScore(int N, int article_a, int article_b) {
        double score = 0;
        if (!in_links.containsKey(article_a) || !in_links.containsKey(article_b)) {
            return score;
        }
        TIntHashSet inlinks_a = in_links.get(article_a);
        TIntHashSet inlinks_b = in_links.get(article_b);

        int inlinks_a_size = inlinks_a.size();
        int inlinks_b_size = inlinks_b.size();

        TIntHashSet common = new TIntHashSet(inlinks_a);
        common.retainAll(inlinks_b);

        if (!common.isEmpty()) {
            score = (Math.log(Math.max(inlinks_a_size, inlinks_b_size)) - Math.log(common.size())) / (Math.log(N) - (Math.log(Math.min(inlinks_a_size, inlinks_b_size))));
        }
        return score;
    }

    /**
     * Compute the normalized google distance for a pair of entities.
     *
     * @param pairs
     * @param out_dir
     */
    public Map<String, Map<String, Double>> computeMilneWittenScorePairs(Map<String, Set<String>> pairs, String out_dir) {
        Map<String, Map<String, Double>> pair_scores = new HashMap<>();
        int N = entities.size();
        String out_file = out_dir + "/relatedness_mw.tsv";
        StringBuffer sb = new StringBuffer();
        for (String article_a_title : pairs.keySet()) {
            if (!index_entities.containsKey(article_a_title)) {
                System.out.printf("There is no entry[A] for %s.\n", article_a_title);
                continue;
            }
            int article_a = index_entities.get(article_a_title);
            if (!pair_scores.containsKey(article_a_title)) {
                pair_scores.put(article_a_title, new HashMap<>());
            }

            for (String article_b_title : pairs.get(article_a_title)) {
                if (!index_entities.containsKey(article_b_title)) {
                    continue;
                }

                int article_b = index_entities.get(article_b_title);
                double score = computeMilneWittenScore(N, article_a, article_b);

                if (score == 0) {
                    continue;
                }
                pair_scores.get(article_a_title).put(article_b_title, score);
                sb.append(article_a).append("\t").append(article_a_title).append("\t").append(article_b).append("\t").append(article_b_title).append("\t").append(score).append("\n");

                if (sb.length() > 10000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
            System.out.printf("Finished computing the relatedness scores for %s.\n", entities.get(article_a));
        }
        FileUtils.saveText(sb.toString(), out_file, true);
        return pair_scores;
    }

    public static void main(String[] args) throws IOException {
        String out_dir = "", anchor_graph = "", entity_dict = "";
        Set<String> pairs = new HashSet<>();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-entity_dict")) {
                entity_dict = args[++i];
            } else if (args[i].equals("-pairs")) {
                pairs = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-anchor_graph")) {
                anchor_graph = args[++i];
            }
        }

        Set<String> pairs_f = pairs;
        WikiAnchorGraph wg = new WikiAnchorGraph();
        wg.isOutLinks = false;
        System.out.println("Loading Wikipedia in-degree anchor graph.");
        wg.loadEntityIndex(entity_dict, false, out_dir);
        wg.loadInDegreeAnchorGraph(anchor_graph, out_dir);

        String out_file = out_dir + "/dewiki_mw_score.tsv";
        //compute the milne-witten score for all entities
        int N = wg.index_entities.size();
        pairs_f.parallelStream().forEach(article_a -> {
            StringBuffer sb = new StringBuffer();
            if (!wg.index_entities.containsKey(article_a)) {
                return;
            }
            int article_a_idx = wg.index_entities.get(article_a);
            for (String article_b : pairs_f) {
                if (!wg.index_entities.containsKey(article_b)) {
                    continue;
                }
                int article_b_idx = wg.index_entities.get(article_b);

                double mw_score = wg.computeMilneWittenScore(N, article_a_idx, article_b_idx);
                if (mw_score == 0) {
                    continue;
                }
                sb.append(article_a).append("\t").append(article_b).append("\t").append(mw_score).append("\n");

                if (sb.length() > 10000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
            System.out.printf("Finished computing MW score for entity %s.\n", article_a);
            FileUtils.saveText(sb.toString(), out_file, true);
        });
    }
}
