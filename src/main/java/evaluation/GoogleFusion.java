package evaluation;

import datastruct.wikitable.WikiColumnHeader;
import datastruct.wikitable.WikiTable;
import io.FileUtils;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.jgrapht.alg.interfaces.MatchingAlgorithm;
import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import utils.DataUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by besnik on 7/12/18.
 */
public class GoogleFusion {
    //articles on which we operate (source and target articles)
    public static Set<String> seed_entities = new HashSet<>();
    public static Set<String> filter_entities = new HashSet<>();

    //the entity categories
    public static Map<String, Set<String>> entity_cats = new HashMap<>();
    public static Map<String, Set<String>> cats_entities = new HashMap<>();

    //the ground-truth
    public static Map<Integer, Map<Integer, Boolean>> gt_table_entities = new HashMap<>();

    public static void main(String[] args) throws IOException {
        String out_dir = "", table_data = "", article_cats = "";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-article_categories")) {
                article_cats = args[++i];
                entity_cats = DataUtils.readEntityCategoryMappingsWiki(article_cats, null);
                cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, null);
            } else if (args[i].equals("-seed_entities")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-table_data")) {
                table_data = args[++i];
            } else if (args[i].equals("-gt_table_alignment")) {
                gt_table_entities = DataUtils.loadTableAlignmentLabels(args[++i]);
            }
        }

        //load the tables first.
        Map<String, List<WikiTable>> tables = DataUtils.loadTables(table_data, filter_entities, true);
        Queue<String> features = new ConcurrentLinkedQueue<>();

        seed_entities.parallelStream().forEach(entity_a -> {
            if (!tables.containsKey(entity_a)) {
                return;
            }
            System.out.printf("Processing tables for entity %s\n", entity_a);
            List<WikiTable> tables_a = tables.get(entity_a);
            for (String entity_b : filter_entities) {
                if (!tables.containsKey(entity_b)) {
                    continue;
                }
                List<WikiTable> tables_b = tables.get(entity_b);
                for (int i = 0; i < tables_a.size(); i++) {
                    WikiTable tbl_a = tables_a.get(i);

                    StringBuffer sb = new StringBuffer();
                    for (int j = 0; j < tables_b.size(); j++) {
                        WikiTable tbl_b = tables_b.get(j);

                        //we have no evaluation for this table pair
                        if (!gt_table_entities.containsKey(tbl_a.table_id) || !gt_table_entities.get(tbl_a.table_id).containsKey(tbl_b.table_id)) {
                            continue;
                        }

                        boolean label = gt_table_entities.containsKey(tbl_a.table_id) && gt_table_entities.get(tbl_a.table_id).containsKey(tbl_b.table_id) ?
                                gt_table_entities.get(tbl_a.table_id).get(tbl_b.table_id) : false;
                        double tbl_score = computeMaximumWeightedBipartiteGraphMatch(tbl_a, tbl_b);
                        double tbl_score_ec = computeTableEntityComplementGoogleFusion(tbl_a, tbl_b, false);

                        sb.append(entity_a).append("\t").append(entity_b).append("\t").
                                append(tbl_a.table_id).append("\t").append(tbl_b.table_id).append("\t").
                                append(tbl_score).append("\t").append(tbl_score_ec).append("\t").append(label).append("\n");
                    }
                    if (sb.length() != 0)
                        features.add(sb.toString());
                }
            }
            System.out.printf("Finished processing tables for entity %s\n", entity_a);
        });

        //write the output
        String out_file = out_dir + "/google_fusion_features_eval_cs_sample_50.tsv";
        FileUtils.saveText("entity_a\tentity_b\ttbl_id_a\ttbl_id_b\ttbl_score\tlabel\n", out_file);
        StringBuffer sb = new StringBuffer();

        for (String lines : features) {
            sb.append(lines);

            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), out_file, true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
    }

    /**
     * Construct the bipartite graph for the pair of tables for matching. The vertices correspond to the column labels
     * and the column names.
     * <p>
     * From the bipartite graph we compute the maximum matching weight, and additionally compute a normalized weight
     * which takes into account the number of edges and nodes based on the approach proposed by Das Sarma et al.
     *
     * @param tbl_a
     * @param tbl_b
     * @return
     */
    public static double computeMaximumWeightedBipartiteGraphMatch(WikiTable tbl_a, WikiTable tbl_b) {
        SimpleWeightedGraph<String, DefaultWeightedEdge> g = new SimpleWeightedGraph<>(DefaultWeightedEdge.class);

        WikiColumnHeader[] cols_a = tbl_a.columns[tbl_a.columns.length - 1];
        WikiColumnHeader[] cols_b = tbl_b.columns[tbl_b.columns.length - 1];

        Set<String> v_a = new HashSet<>();
        Set<String> v_b = new HashSet<>();

        for (int i = 0; i < cols_a.length; i++) {
            for (String node_a_val : getLabels(cols_a[i])) {
                v_a.add(node_a_val + "_a");
            }
            //add the column title
            v_a.add(cols_a[i].column_name + "_a");
        }

        for (int i = 0; i < cols_b.length; i++) {
            for (String node_b_val : getLabels(cols_b[i])) {
                v_b.add(node_b_val + "_b");
            }
            //add the column title
            v_b.add(cols_b[i].column_name + "_b");
        }

        //add the vertices and edges
        v_a.forEach(n -> g.addVertex(n));
        v_b.forEach(n -> g.addVertex(n));

        //add the weighted edges, where the weight is the Levenshtein distance between the two strings
        LevenshteinDistance lv = new LevenshteinDistance();
        int num_edges = 0;
        for (String node_a : v_a) {
            String node_a_label = node_a.replace("_a", "");
            for (String node_b : v_b) {
                String node_b_label = node_b.replace("_b", "");
                int weight = lv.apply(node_a_label, node_b_label);
                DefaultWeightedEdge edge = g.addEdge(node_a, node_b);
                g.setEdgeWeight(edge, weight);
                num_edges++;
            }
        }

        MaximumWeightBipartiteMatching<String, DefaultWeightedEdge> mbm = new MaximumWeightBipartiteMatching<>(g, v_a, v_b);
        MatchingAlgorithm.Matching<String, DefaultWeightedEdge> match = mbm.getMatching();
        double W = match.getWeight();
        int n1 = v_a.size();
        int n2 = v_a.size();

        double score = W / (n1 + n2 - num_edges);
        return score;
    }


    /**
     * Get all the labels that are associated with a column.
     *
     * @return
     */
    public static Set<String> getLabels(WikiColumnHeader col) {
        Set<String> labels = new HashSet<>();

        //compute the number of entities that contain a specific category
        List<Map.Entry<Object, Integer>> val_dist = col.getSortedColumnDomain();

        for (Map.Entry<Object, Integer> value : val_dist) {
            //if the value is an entity, then add it to the domain of this
            String label = value.getKey().toString();
            if (!entity_cats.containsKey(label)) {
                continue;
            }

            Set<String> cats = entity_cats.get(label);
            labels.addAll(cats);
        }

        return labels;
    }


    /**
     * Compute the similarity between the entity set of two tables based on the labelled weights.
     *
     * @return
     */
    public static double computeTableEntityComplementGoogleFusion(WikiTable tbl_a, WikiTable tbl_b, boolean isUniform) {
        //compute the number of entities that contain a specific category
        Map<String, Set<String>> label_a_domain = getLabelDomain(tbl_a);
        Set<String> entities_a = new HashSet<>();
        label_a_domain.keySet().forEach(l -> entities_a.addAll(label_a_domain.get(l)));
        //from the second label domain, we need to remove entities that appear in the first table
        Set<String> entities_b = new HashSet<>();
        Map<String, Set<String>> label_b_domain = getLabelDomain(tbl_b, entities_a);
        label_b_domain.keySet().forEach(l -> entities_b.addAll(label_b_domain.get(l)));

        Map<String, Double> w_a = new HashMap<>();
        Map<String, Double> w_b = new HashMap<>();

        if (isUniform) {
            label_a_domain.keySet().forEach(c -> w_a.put(c, 1.0));
            label_b_domain.keySet().forEach(c -> w_b.put(c, 1.0));
        }

        //compute label weights
        label_a_domain.keySet().forEach(c -> w_a.put(c, 1.0 / label_a_domain.get(c).size()));
        label_b_domain.keySet().forEach(c -> w_b.put(c, 1.0 / label_b_domain.get(c).size()));

        Map<String, Double> non_linear_w_a = computeColumnWeightsGoogleFusion(w_a, entities_a, 2, 1);
        Map<String, Double> non_linear_w_b = computeColumnWeightsGoogleFusion(w_b, entities_b, 2, 0);


        //compute the cosine similarity between the two vectors.
        double score = DataUtils.computeCosine(non_linear_w_a, non_linear_w_b);
        return score;
    }

    /***
     * Compute the column label weights based on the approach propsed by Das Sarma et al.
     * @param w_col
     * @param entities
     * @param n
     * @param m
     * @return
     */
    public static Map<String, Double> computeColumnWeightsGoogleFusion(Map<String, Double> w_col, Set<String> entities, int n, int m) {
        Map<String, Double> w = new HashMap<>();

        //get the total number of entities in this column.
        int total_entities = entities.size();

        //for each label
        for (String label : w_col.keySet()) {
            double sum_weight = 0.0;
            for (String entity : entities) {
                if (!entity_cats.containsKey(entity)) {
                    continue;
                }
                sum_weight += w_col.get(label);
            }
            sum_weight = Math.pow(sum_weight, n) / Math.pow(total_entities, m);
            w.put(label, sum_weight);
        }

        return w;
    }

    /**
     * For a given table, get the domain of a label/category.
     *
     * @param tbl
     * @return
     */
    public static Map<String, Set<String>> getLabelDomain(WikiTable tbl, Set<String> entities) {
        Map<String, Set<String>> cat_domain = new HashMap<>();

        WikiColumnHeader[] cols = tbl.columns[tbl.columns.length - 1];
        for (WikiColumnHeader col : cols) {
            List<Map.Entry<Object, Integer>> val_dist = col.getSortedColumnDomain();
            for (Map.Entry<Object, Integer> value : val_dist) {
                //if the value is an entity, then add it to the domain of this
                String entity = value.getKey().toString();
                if (!entity_cats.containsKey(entity) || entities.contains(entity)) {
                    continue;
                }

                Set<String> cats = entity_cats.get(entity);
                for (String label : cats) {
                    if (!cat_domain.containsKey(label)) {
                        cat_domain.put(label, new HashSet<>());
                    }
                    cat_domain.get(label).add(entity);
                }
            }
        }
        return cat_domain;
    }

    /**
     * For a given table, get the domain of a label/category.
     *
     * @param tbl
     * @return
     */
    public static Map<String, Set<String>> getLabelDomain(WikiTable tbl) {
        Map<String, Set<String>> cat_domain = new HashMap<>();

        WikiColumnHeader[] cols = tbl.columns[tbl.columns.length - 1];
        for (WikiColumnHeader col : cols) {
            List<Map.Entry<Object, Integer>> val_dist = col.getSortedColumnDomain();
            for (Map.Entry<Object, Integer> value : val_dist) {
                //if the value is an entity, then add it to the domain of this
                String entity = value.getKey().toString();
                if (!entity_cats.containsKey(entity)) {
                    continue;
                }

                Set<String> cats = entity_cats.get(entity);
                for (String label : cats) {
                    if (!cat_domain.containsKey(label)) {
                        cat_domain.put(label, new HashSet<>());
                    }
                    cat_domain.get(label).add(entity);
                }
            }
        }
        return cat_domain;
    }
}
