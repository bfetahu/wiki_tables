package datastruct;

import io.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 2/7/18.
 */
public class WikiAnchorGraph {
    public Map<String, Set<String>> out_links;
    public Map<String, Set<String>> in_links;

    public Map<String, Map<String, Double>> sim_rank_scores;


    public WikiAnchorGraph() {
        out_links = new HashMap<>();
        in_links = new HashMap<>();
        sim_rank_scores = new HashMap<>();
    }


    /**
     * Load the Wikipedia anchor graph. We build the set of in-links and out-links for each article.
     *
     * @param anchor_graph_data
     * @throws IOException
     */
    public void loadAnchorGraph(String anchor_graph_data) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(anchor_graph_data);
        String line;

        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");


            //get the outlinks of an article
            String article = data[0];
            if (!out_links.containsKey(article)) {
                out_links.put(article, new HashSet<>());
            }

            Set<String> article_out_links = out_links.get(article);
            for (int i = 1; i < data.length; i++) {
                article_out_links.add(data[i]);
            }

            //we need to convert them into in-links
            article_out_links.parallelStream().forEach(out_article -> {
                if (!in_links.containsKey(out_article)) {
                    in_links.put(out_article, new HashSet<>());
                }
                in_links.get(out_article).add(article);
            });
        }
    }


    /**
     * Compute the SimRank between any two articles which have incoming connections in the Wikipedia anchor graph.
     */
    public void computeGraphSimRank() {
        //we compute the simrank score for all article pairs that have incoming edges, for the rest the score is set to zero.
        String[] articles = new String[in_links.size()];
        in_links.keySet().toArray(articles);

        double epsilon = 0.8;
        int max_iter = 0;

        //initialize the scores
        initSimRankScores();

        for (int k = 0; k < max_iter; k++) {
            for (int i = 0; i < articles.length; i++) {
                String article_a = articles[i];
                for (int j = 0; j < articles.length; j++) {
                    String article_b = articles[j];
                    double score = computeSimRank(article_a, article_b, epsilon);
                    sim_rank_scores.get(article_a).put(article_b, score);
                }
            }
        }
    }

    /**
     * Set to zero or one the scores of all pairs for the initial iteration for simrank.
     */
    public void initSimRankScores() {
        String[] articles = new String[in_links.size()];
        in_links.keySet().toArray(articles);

        for (int i = 0; i < articles.length; i++) {
            String article_a = articles[i];

            if (!sim_rank_scores.containsKey(article_a)) {
                sim_rank_scores.put(article_a, new HashMap<>());
            }

            for (int j = 0; j < articles.length; j++) {
                String article_b = articles[j];
                if (j == i) {
                    sim_rank_scores.get(article_a).put(article_b, 1.0);
                } else {
                    sim_rank_scores.get(article_a).put(article_b, 0.0);
                }

            }
        }
    }

    /**
     * Compute recursivel the simrank score between any pair of entities.
     *
     * @param article_a
     * @param article_b
     * @return
     */
    public double computeSimRank(String article_a, String article_b, double epsilon) {
        double score = 0;

        score += epsilon / (in_links.get(article_a).size() * in_links.get(article_b).size());

        //the in-links for the pair of articles
        Set<String> in_links_a = in_links.get(article_a);
        Set<String> in_links_b = in_links.get(article_b);

        for (String link_a : in_links_a) {
            for (String link_b : in_links_b) {
                score += computeSimRank(link_a, link_b, epsilon);
            }
        }
        return score;
    }
}
