package evaluation;

import datastruct.wikitable.WikiColumnHeader;
import datastruct.wikitable.WikiTable;
import edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath;
import edu.uci.ics.jung.graph.Graph;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import representation.CategoryEntityGraph;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Compute the relatedness between Wikipedia articles based on their category representation and other features we consider
 * in our approach for generating candidates.
 * Created by besnik on 12/6/17.
 */
public class ArticleCandidates {
    private CategoryRepresentation root_category;
    private Map<String, CategoryRepresentation> cat_to_map;
    private Map<String, TIntDoubleHashMap> cat_weights;

    //the entity dictionaries which include the entities for which we have the ground-truth (seed_entities, gt_entities),
    // and the entities which contain a table (filter_entities)
    public static Set<String> seed_entities = new HashSet<>();
    public static Set<String> filter_entities = new HashSet<>();
    public static Map<String, Set<String>> gt_entities = new HashMap<>();

    //the entity categories
    public static Map<String, Set<String>> entity_cats = new HashMap<>();

    //load the category-entity graph and the corresponding graph embedding.
    public static CategoryEntityGraph ceg = new CategoryEntityGraph();
    public static Graph<Integer, Integer> ceg_graph;
    public static DijkstraShortestPath<Integer, Integer> sp;

    //load the word embeddings.
    public static Map<String, TDoubleArrayList> word2vec;

    //load the wikipedia tables
    public Map<String, Map<String, List<WikiTable>>> tables;

    //keep the set of entity pairs that are not computed.
    public static Set<String> finished_gt_seeds = new HashSet<>();

    //keep the entity abstracts
    public static Map<String, String> entity_abstracts = new HashMap<>();

    public ArticleCandidates(CategoryRepresentation root_category) {
        cat_to_map = new HashMap<>();
        this.root_category = root_category;
        this.root_category.loadIntoMapChildCats(cat_to_map);

        Map<String, Double> attribute_max_level_category = DataUtils.computeMaxLevelAttributeCategory(cat_to_map);
        cat_weights = DataUtils.computeCategoryAttributeWeights(attribute_max_level_category, cat_to_map);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cat_rep_path = "", out_dir = "", table_data = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cat_rep")) {
                cat_rep_path = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-article_categories")) {
                entity_cats = DataUtils.readEntityCategoryMappingsWiki(args[++i], null);
            } else if (args[i].equals("-seed_entities")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-gt_pairs")) {
                gt_entities = FileUtils.readMapSet(args[++i], "\t");
            } else if (args[i].equals("-table_data")) {
                table_data = args[++i];
            } else if (args[i].equals("-word2vec")) {
                word2vec = DataUtils.loadWord2Vec(args[++i]);
            } else if (args[i].equals("-graph_emb")) {
                ceg.loadEmbedding(args[++i]);
            } else if (args[i].equals("-finished_gt_seeds")) {
                finished_gt_seeds = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-abstracts")) {
                entity_abstracts = DataUtils.loadEntityAbstracts(args[++i]);
            }
        }
        ceg_graph = ceg.constructUndirectedGraph();
        sp = new DijkstraShortestPath<>(ceg_graph);
        System.out.println("Finished constructing the undirected graph.");

        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(cat_rep_path);
        ArticleCandidates ac = new ArticleCandidates(cat);

        ac.tables = DataUtils.loadTables(table_data, filter_entities, false);
        System.out.printf("Finished loading the data tables for %d entities.\n", ac.tables.size());

        System.out.println("Loaded the article candidate object.");

        //compute the features.
        ac.scoreTableCandidatesApproach(out_dir);
    }

    /**
     * Generates the feature representation for each of our ground-truth articles based on our approach which consists
     * of several features.
     *
     * @param out_dir
     */
    public void scoreTableCandidatesApproach(String out_dir) throws IOException {
        StringBuffer header = new StringBuffer();
        header.append("entity_a\tentity_b\tmin_cat_rep_sim\tmax_cat_rep_sim\tmean_cat_rep_sim\t");
        header.append("cat_level_diff_lca\tmin_shortest_path_lca\tmax_shortest_path_lca\tmean_shortest_path_lca\t");
        header.append("section_w2v_sim\tmin_distance_col\tmax_distance_col\tmean_distance_col\tmin_col_w2v_sim\tmax_col_w2v_sim\tmean_col_w2v_sim\t");
        header.append("e_n2v_ec\tavg_n2v_cat_sim\tcat_n2v_min_sim\tcat_n2v_max_sim\tcat_n2v_mean_sim\tabs_sim\tlabel\n");
        FileUtils.saveText(header.toString(), out_dir + "/candidate_features.tsv");

        //since we reuse the average word vectors we create them first and then reuse.
        Map<String, TDoubleArrayList> avg_w2v = new HashMap<>();
        seed_entities.forEach(entity -> avg_w2v.put(entity, DataUtils.computeAverageWordVector(entity_abstracts.get(entity), word2vec)));
        //compute the average vectors of the categories of an entity.
        Map<String, TDoubleArrayList> avg_cat_n2v = new HashMap<>();
        seed_entities.forEach(entity -> avg_cat_n2v.put(entity, DataUtils.computeAverageWordVector(entity_cats.get(entity), ceg)));

        for (String entity : seed_entities) {
            if (!tables.containsKey(entity) || finished_gt_seeds.contains(entity)) {
                continue;
            }
            TDoubleArrayList entity_abs_w2v_avg = avg_w2v.get(entity);
            Set<String> entity_cats_a = entity_cats.get(entity);
            List<String> feature_lines = new ArrayList<>();
            List<String> concurrent_fl = Collections.synchronizedList(feature_lines);

            filter_entities.parallelStream().forEach(entity_candidate -> {
                if (!tables.containsKey(entity_candidate)) {
                    return;
                }
                StringBuffer sb = new StringBuffer();
                TDoubleArrayList entity_candidate_abs_w2v_avg = avg_w2v.get(entity_candidate);
                Set<String> entity_cats_candidate = entity_cats.get(entity);

                //create for each of these pairs the features
                boolean label = gt_entities.containsKey(entity) && gt_entities.get(entity).contains(entity_candidate);

                //add all the category representation similarities
                double[] cat_rep_sim = computeCategorySim(entity_cats_a, entity_cats_candidate);

                //check the category taxonomy, LCA, distance between categories, depth level
                Set<String> lca_cats = DataUtils.findLCACategories(entity_cats_a, entity_cats_candidate, cat_to_map);
                double[] lca_features = computeCategoryTaxonomyFeatures(lca_cats, entity_cats_a, entity_cats_candidate);

                //add all the table column similarities
                double[] tbl_sim = computeTableFeatures(entity, entity_candidate);

                //add all the node2vec similarities for the instances in the table
                double[] cat_emb_sim = computeCategoryEmbeddSim(entity_cats_a, entity_cats_candidate);
                double e_n2v_ec = DataUtils.computeCosineSim(ceg.getEmbeddingByKey(entity), ceg.getEmbeddingByKey(entity_candidate));
                double avg_cat_n2v_sim = DataUtils.computeCosineSim(avg_cat_n2v.get(entity), avg_cat_n2v.get(entity_candidate));

                //add the features.
                sb.append(entity).append("\t").append(entity_candidate).append("\t");

                IntStream.range(0, cat_rep_sim.length).forEach(i -> sb.append(cat_rep_sim[i]).append("\t"));
                IntStream.range(0, lca_features.length).forEach(i -> sb.append(lca_features[i]).append("\t"));
                IntStream.range(0, tbl_sim.length).forEach(i -> sb.append(tbl_sim[i]).append("\t"));
                IntStream.range(0, cat_emb_sim.length).forEach(i -> sb.append(cat_emb_sim[i]).append("\t"));
                sb.append(e_n2v_ec).append("\t").append(avg_cat_n2v_sim).append("\t");


                //compute the w2v sim between the entity abstracts
                double abs_sim = DataUtils.computeCosineSim(entity_abs_w2v_avg, entity_candidate_abs_w2v_avg);
                sb.append(abs_sim).append("\t").append(label).append("\n");
                concurrent_fl.add(sb.toString());
            });

            StringBuffer sb = new StringBuffer();
            for (String line : concurrent_fl) {
                sb.append(line);
                if (sb.length() > 10000) {
                    FileUtils.saveText(sb.toString(), out_dir + "/candidate_features.tsv", true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), out_dir + "/candidate_features.tsv", true);
            System.out.printf("Finished processing features for entity %s.\n", entity);
        }
    }

    /**
     * Computes the min/max/mean distance between the categories of entity A and B against the LCA categories. Next,
     * compute the shortest path in the category taxonomy between the categories of entities A and B
     * against the LCA categories.
     *
     * @param lca_cats
     * @param cats_a
     * @param cats_b
     * @return
     */
    public double[] computeCategoryTaxonomyFeatures(Set<String> lca_cats, Set<String> cats_a, Set<String> cats_b) {
        if (lca_cats == null || lca_cats.isEmpty() || cats_a.isEmpty() || cats_b.isEmpty()) {
            return new double[4];
        }
        int lca_cat_level = lca_cats.stream().mapToInt(c -> cat_to_map.containsKey(c) ? cat_to_map.get(c).level : 0).max().getAsInt();
        int cat_a_level = cats_a.stream().mapToInt(c -> cat_to_map.containsKey(c) ? cat_to_map.get(c).level : 0).max().getAsInt();
        int cat_b_level = cats_b.stream().mapToInt(c -> cat_to_map.containsKey(c) ? cat_to_map.get(c).level : 0).max().getAsInt();

        //check if both category sets are in similar difference
        double cat_level_diff = Math.abs(Math.abs(cat_a_level - lca_cat_level) - Math.abs(cat_b_level - lca_cat_level)) / 2.0;

        //get the shortest paths to LCA categories
        List<Integer> sp_a = new ArrayList<>();
        List<Integer> sp_b = new ArrayList<>();
        for (String lca_cat : lca_cats) {
            int lca_cat_id = ceg.node_index.get(lca_cat);
            for (String cat_a : cats_a) {
                List<Integer> path = sp.getPath(lca_cat_id, ceg.node_index.get(cat_a));
                if (path != null) {
                    sp_a.add(path.size());
                }
            }
            for (String cat_b : cats_b) {
                List<Integer> path = sp.getPath(lca_cat_id, ceg.node_index.get(cat_b));
                if (path != null) {
                    sp_b.add(path.size());
                }
            }
        }

        double min_sp_a = sp_a.stream().mapToDouble(l -> l).min().getAsDouble();
        double min_sp_b = sp_b.stream().mapToDouble(l -> l).min().getAsDouble();
        double max_sp_a = sp_a.stream().mapToDouble(l -> l).max().getAsDouble();
        double max_sp_b = sp_b.stream().mapToDouble(l -> l).max().getAsDouble();
        double mean_sp_a = sp_a.stream().mapToDouble(l -> l).average().getAsDouble();
        double mean_sp_b = sp_b.stream().mapToDouble(l -> l).average().getAsDouble();


        return new double[]{cat_level_diff, Math.abs(min_sp_a - min_sp_b) / 2.0, Math.abs(max_sp_a - max_sp_b) / 2.0, Math.abs(mean_sp_a - mean_sp_b) / 2.0};
    }

    /**
     * Compute the category representation similarity between the categories of two entities.
     *
     * @param cats_a
     * @param cats_b
     * @return
     */
    public double[] computeCategorySim(Set<String> cats_a, Set<String> cats_b) {
        List<Double> scores = new ArrayList<>();
        for (String cat_a : cats_a) {
            if (!cat_weights.containsKey(cat_a)) {
                continue;
            }
            TIntDoubleHashMap cat_weights_a = cat_weights.get(cat_a);
            for (String cat_b : cats_b) {
                if (!cat_weights.containsKey(cat_b)) {
                    continue;
                }
                TIntDoubleHashMap cat_weights_b = cat_weights.get(cat_b);
                double score = DataUtils.computeEuclideanDistance(cat_weights_a, cat_weights_b);
                scores.add(score);
            }
        }
        if (scores.isEmpty()) {
            return new double[3];
        }

        double min = scores.stream().mapToDouble(x -> x).min().getAsDouble();
        double max = scores.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean = scores.stream().mapToDouble(x -> x).average().getAsDouble();
        return new double[]{min, max, mean};
    }

    /**
     * Compute the similarity of categories assigned to entities A and B based on their graph embeddings.
     *
     * @param cats_a
     * @param cats_b
     * @return
     */
    public double[] computeCategoryEmbeddSim(Set<String> cats_a, Set<String> cats_b) {
        List<Double> scores = new ArrayList<>();

        int num_dimensions = ceg.getEmbeddDimension();
        for (String cat_a : cats_a) {
            for (String cat_b : cats_b) {
                if (!ceg.node_index.containsKey(cat_a) || !ceg.node_index.containsKey(cat_b)) {
                    System.out.printf("Categories %finished_gt_seeds \t %finished_gt_seeds are missing.\n", cat_a, cat_b);
                    continue;
                }
                TDoubleArrayList cat_a_embedd = ceg.getEmbeddingByKey(cat_a);
                TDoubleArrayList cat_b_embedd = ceg.getEmbeddingByKey(cat_b);

                //compute the cosine similarity
                double score = 0;
                for (int i = 0; i < num_dimensions; i++) {
                    score += cat_a_embedd.get(i) * cat_b_embedd.get(i);
                }
                double sum_a = Math.sqrt(Arrays.stream(cat_a_embedd.toArray()).map(x -> Math.pow(x, 2)).sum());
                double sum_b = Math.sqrt(Arrays.stream(cat_b_embedd.toArray()).map(x -> Math.pow(x, 2)).sum());

                score /= (sum_a * sum_b);
                scores.add(score);
            }
        }

        double min = scores.stream().mapToDouble(x -> x).min().getAsDouble();
        double max = scores.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean = scores.stream().mapToDouble(x -> x).average().getAsDouble();
        return new double[]{min, max, mean};
    }

    /**
     * Compute the similarity features between two entities in terms of their tables, respectively the table schemas.
     *
     * @param entity_a
     * @param entity_b
     * @return
     */
    public double[] computeTableFeatures(String entity_a, String entity_b) {
        Map<String, List<WikiTable>> tables_a = tables.get(entity_a);
        Map<String, List<WikiTable>> tables_b = tables.get(entity_b);

        List<Double> table_schema_pos = new ArrayList<>();
        List<Double> table_schema_sim = new ArrayList<>();

        //compute the cross-table match.
        Map<Integer, Map.Entry<Integer, Double>> max_match = null;
        double section_sim = 0;
        for (String section_a : tables_a.keySet()) {
            for (WikiTable table_a : tables_a.get(section_a)) {
                for (String section_b : tables_b.keySet()) {
                    //check which is the highest matching table for table_a. The comparison is done in the amount of
                    //columns that have high matching column names and close index.
                    for (WikiTable table_b : tables_b.get(section_b)) {
                        Map<Integer, Map.Entry<Integer, Double>> tbl_match = computeTableSchemaSimilarity(table_a, table_b);

                        if (max_match == null || isBetterMatch(max_match, tbl_match)) {
                            max_match = tbl_match;
                            section_sim = computeWord2VecSim(section_a, section_b);
                        }
                    }
                }
            }
        }

        //add the features which correspond to col idx differences and col name match
        for (int col_idx : max_match.keySet()) {
            Map.Entry<Integer, Double> match = max_match.get(col_idx);
            table_schema_pos.add(Math.abs((double) (col_idx - match.getKey())));
            table_schema_sim.add(match.getValue());
        }

        double min_distance = table_schema_pos.stream().mapToDouble(x -> x).min().getAsDouble();
        double max_distance = table_schema_pos.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean_distance = table_schema_pos.stream().mapToDouble(x -> x).average().getAsDouble();

        double min_sim = table_schema_sim.stream().mapToDouble(x -> x).min().getAsDouble();
        double max_sim = table_schema_sim.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean_sim = table_schema_sim.stream().mapToDouble(x -> x).average().getAsDouble();

        return new double[]{section_sim, min_distance, max_distance, mean_distance, min_sim, max_sim, mean_sim};
    }

    /**
     * Check which of the table matches is a better one for a given table.
     *
     * @param match_a
     * @param match_b
     * @return
     */
    public boolean isBetterMatch(Map<Integer, Map.Entry<Integer, Double>> match_a, Map<Integer, Map.Entry<Integer, Double>> match_b) {
        int better = 0;

        for (int col_idx : match_a.keySet()) {
            if (!match_b.containsKey(col_idx)) {
                continue;
            }

            Map.Entry<Integer, Double> col_a_match = match_a.get(col_idx);
            Map.Entry<Integer, Double> col_b_match = match_b.get(col_idx);

            boolean is_better_col_idx = col_b_match.getKey() < col_a_match.getKey();
            boolean is_better_sim_match = col_b_match.getValue() > col_a_match.getValue();

            if (is_better_sim_match) {
                if (is_better_col_idx || Math.abs(col_a_match.getKey() - col_b_match.getKey()) <= 2) {
                    better++;
                }
            }
        }

        return better > (match_a.size() / 2.0);
    }

    /**
     * Compute the similarity of column names between the two tables.
     *
     * @param a
     * @param b
     */
    public Map<Integer, Map.Entry<Integer, Double>> computeTableSchemaSimilarity(WikiTable a, WikiTable b) {
        WikiColumnHeader[] cols_a = a.columns[a.columns.length - 1];
        WikiColumnHeader[] cols_b = b.columns[b.columns.length - 1];

        Map<Integer, Map.Entry<Integer, Double>> scores = new HashMap<>();
        for (int i = 0; i < cols_a.length; i++) {
            double max_match = 0.0;
            int max_indice = cols_b.length - 1;
            WikiColumnHeader col_a = cols_a[i];
            for (int j = 0; j < cols_b.length; j++) {
                WikiColumnHeader col_b = cols_b[j];

                double sim = computeWord2VecSim(col_a.column_name, col_b.column_name);
                if (sim > max_match) {
                    max_match = sim;
                    max_indice = j;
                }
            }
            scores.put(i, new AbstractMap.SimpleEntry<>(max_indice, max_match));
        }
        return scores;
    }

    /**
     * Compute the similarity between two strings based on their word vector representation.
     *
     * @param str_a
     * @param str_b
     * @return
     */
    public double computeWord2VecSim(String str_a, String str_b) {
        //compute average word vectors
        TDoubleArrayList avg_a = DataUtils.computeAverageWordVector(str_a, word2vec);
        TDoubleArrayList avg_b = DataUtils.computeAverageWordVector(str_b, word2vec);
        return DataUtils.computeCosineSim(avg_a, avg_b);
    }


}