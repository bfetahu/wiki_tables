package evaluation;

import datastruct.TableCandidateFeatures;
import gnu.trove.list.array.TDoubleArrayList;
import io.FileUtils;
import org.apache.commons.lang3.tuple.Triple;
import representation.CategoryRepresentation;
import representation.WikiAnchorGraph;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by besnik on 2/7/18.  
 */
public class BaselineCandidatePairStrategies {
    public static Map<String, CategoryRepresentation> cat_to_map = new HashMap<>();
    public static Map<String, Set<String>> entity_cats;
    public static Map<String, Set<String>> cat_entities;

    //all the article-candidate pairs which are also in our ground-truth.
    public static Map<String, Set<String>> gt_pairs;
    public static Set<String> filter_entities;
    public static Set<String> seed_entities;


    //load the word embeddings.
    public static Map<String, TDoubleArrayList> node2vec;

    //similarity cutoffs
    public static double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

    public static void main(String[] args) throws IOException {
        String all_pairs = "", out_dir = "", option = "", article_categories = "",
                category_path = "", anchor_data = "", wiki_articles = "",
                cat_type = "";
        double damping_factor = 0.6;
        int iterations = 5;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-all_pairs")) {
                all_pairs = args[++i];
            } else if (args[i].equals("-gt_pairs")) {
                gt_pairs = FileUtils.readMapSet(args[++i], "\t");
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-option")) {
                option = args[++i];
            } else if (args[i].equals("-category_path")) {
                category_path = args[++i];
            } else if (args[i].equals("-article_categories")) {
                article_categories = args[++i];
            } else if (args[i].equals("-damping")) {
                damping_factor = Double.valueOf(args[++i]);
            } else if (args[i].equals("-iterations")) {
                iterations = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-anchor_data")) {
                anchor_data = args[++i];
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-wiki_articles")) {
                wiki_articles = args[++i];
            } else if (args[i].equals("-seed_entities")) {
                //seed entities consist of all our sampled entities for which we perform the evaluation.
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-cat_type")) {
                cat_type = args[++i];
            } else if (args[i].equals("-node2vec")) {
                node2vec = DataUtils.loadWord2Vec(args[++i]);
            }
        }

        //evaluate the different candidate pair strategies.
        if (option.equals("level")) {
            computeEntityPairCategoryCoverage(article_categories, category_path, out_dir, cat_type);
        } else if (option.equals("level_debug")) {
            debugEntityPairCategoryCoverage(article_categories, category_path, out_dir);
        } else if (option.equals("rep_sim")) {
            computeCategoryRepSimilarityCoverage(category_path, article_categories, all_pairs, out_dir);
        } else if (option.equals("simrank")) {
            computeSimRankGraphSimple(damping_factor, iterations, all_pairs, anchor_data, out_dir, wiki_articles);
        } else if (option.equals("greedy")) {
            computeGreedyCoverage(out_dir);
        } else if (option.equals("mw")) {
            computeMWRelatednessScores(wiki_articles, anchor_data, out_dir);
        } else if (option.equals("lca_scoring")) {
            scoreLCATableCandidatesCategoryRep(category_path, article_categories, out_dir);
        } else if (option.equals("n2vec_entity")) {
            computeNode2VecCoverage(article_categories, category_path, out_dir, true);
        } else if (option.equals("n2vec_category")) {
            computeNode2VecCoverage(article_categories, category_path, out_dir, false);
        }
    }

    /**
     * Compute the coverage of ground truth entities by assessing the similarity of GT entities with other candidate
     * entities based on the node2vec embeddings or the category similarity based on node2vec.
     *
     * @param out_dir
     * @param isEntity
     * @param article_categories
     * @param category_path
     */
    public static void computeNode2VecCoverage(String article_categories, String category_path, String out_dir, boolean isEntity) throws IOException {
        if (!isEntity) {
            loadEntityCategoryDataStructures(article_categories, category_path);
        }

        //check for each entity the coverage similarity.
        for (String entity : seed_entities) {
            TDoubleArrayList entity_emb = node2vec.get(entity.replaceAll(" ", "_"));
            Set<String> gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            int gt_total = gt_entities.size();

            DecimalFormat df = new DecimalFormat("#.00");
            Map<Double, Set<String>> candidates = new TreeMap<>();

            if (isEntity) {
                for (String candidate_entity : filter_entities) {
                    TDoubleArrayList candidate_entity_emb = node2vec.get(candidate_entity.replaceAll(" ", "_"));
                    double score = Double.parseDouble(df.format(DataUtils.computeCosineSim(entity_emb, candidate_entity_emb)));
                    if (!candidates.containsKey(score)) {
                        candidates.put(score, new HashSet<>());
                    }
                    candidates.get(score).add(candidate_entity);
                }
            } else {
                List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cat_to_map.containsKey(cat)).map(cat -> cat_to_map.get(cat)).collect(Collectors.toList());
                for (CategoryRepresentation cat : categories) {
                    TDoubleArrayList cat_emb = node2vec.get(cat.label.replaceAll(" ", "_"));
                    for (String candidate_category : cat_to_map.keySet()) {
                        double score = 0.0;
                        if (cat.label.equals(candidate_category)) {
                            score = 1.0;
                        } else {
                            TDoubleArrayList candidate_cat_emb = node2vec.get(candidate_category.replaceAll(" ", "_"));
                            score = Double.parseDouble(df.format(DataUtils.computeCosineSim(cat_emb, candidate_cat_emb)));
                        }

                        if (!candidates.containsKey(score)) {
                            candidates.put(score, new HashSet<>());
                        }
                        candidates.get(score).addAll(cat_to_map.get(candidate_category).entities);
                    }
                }
            }

            //output the data
            StringBuffer sb = new StringBuffer();
            Double[] scores = new Double[candidates.size()];
            candidates.keySet().toArray(scores);
            for (int i = 0; i < scores.length; i++) {
                double score = scores[i];
                Set<String> sub_entities = new HashSet<>();
                candidates.keySet().stream().filter(s -> s >= score).forEach(s -> sub_entities.addAll(candidates.get(s)));
                sub_entities.retainAll(filter_entities);

                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_entities);

                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                double aligned_ratio = gt_total == 0 ? 0.0 : (double) overlapping / gt_total;
                double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
                sb.append(entity).append("\t").append(gt_total).append("\t").append(score).append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio).append("\n");
            }
            String out_file = isEntity ? out_dir + "/entity_embedding_coverage.tsv" : out_dir + "/category_embedding_coverage.tsv";
            FileUtils.saveText(sb.toString(), out_file, true);
        }
    }

    /**
     * Score the generated article candidates for table alignment, based on their similarity on the category representations,
     * distance to the lowest common ancestor between the two categories.
     *
     * @param article_categories
     * @param out_dir
     */
    public static void scoreLCATableCandidatesCategoryRep(String category_path, String article_categories, String out_dir) throws IOException {
        loadEntityCategoryDataStructures(article_categories, category_path);
        //load the article categories
        Map<String, Set<String>> entity_cats = DataUtils.readEntityCategoryMappingsWiki(article_categories, null);
        System.out.printf("Loaded %d categories, and %d entities", cat_to_map.size(), entity_cats.size());

        //compute for all pairs the min, max, average distance of the categories for article A  and B to their LCA category
        Map<String, Map<String, Triple<Double, Double, Double>>> pairs = TableCandidateFeatures.computeLCAEntityCandidatePairScores(seed_entities, filter_entities, cat_to_map, entity_cats);

        //cut-offs to filter with
        double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 5.0, 10.0, Double.MAX_VALUE};

        String out_file_min = out_dir + "/lca_min_entity_pairs.tsv";
        String out_file_max = out_dir + "/lca_max_entity_pairs.tsv";
        String out_file_mean = out_dir + "/lca_mean_entity_pairs.tsv";

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tlevel\tgt_total");

        for (int i = 0; i < cutoffs.length; i++) {
            sb.append("\tall_candidates[").append(cutoffs[i]).append("]\toverlap[").append(cutoffs[i]).append("]\tunaligned_entities[").append(cutoffs[i]).append("]");
            sb.append("\taligned_ratio[").append(cutoffs[i]).append("]\tunaligned_ratio[").append(cutoffs[i]).append("]");
        }
        sb.append("\n");

        StringBuffer sb_min = new StringBuffer(sb.toString());
        StringBuffer sb_max = new StringBuffer(sb.toString());
        StringBuffer sb_mean = new StringBuffer(sb.toString());
        for (String entity : seed_entities) {
            Set<String> sub_entity_gt_pairs = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            int gt_total = sub_entity_gt_pairs.size();
            sb_min.append(entity).append("\t").append(0).append("\t").append(gt_total);
            sb_max.append(entity).append("\t").append(0).append("\t").append(gt_total);
            sb_mean.append(entity).append("\t").append(0).append("\t").append(gt_total);

            //compute the overlap for each cut-off point
            for (double cutoff : cutoffs) {
                long sub_pairs_min_total = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getLeft()) >= cutoff).count();
                long sub_pairs_max_total = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getMiddle()) >= cutoff).count();
                long sub_pairs_mean_total = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getRight()) >= cutoff).count();

                long sub_pairs_min_overlap = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getLeft()) >= cutoff).filter(s -> sub_entity_gt_pairs.contains(s)).count();
                long sub_pairs_max_overlap = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getMiddle()) >= cutoff).filter(s -> sub_entity_gt_pairs.contains(s)).count();
                long sub_pairs_mean_overlap = pairs.get(entity).entrySet().stream().filter(s -> Math.abs(s.getValue().getRight()) >= cutoff).filter(s -> sub_entity_gt_pairs.contains(s)).count();

                long sub_pairs_min_additional = sub_pairs_min_total - sub_pairs_min_overlap;
                long sub_pairs_max_additional = sub_pairs_max_total - sub_pairs_max_overlap;
                long sub_pairs_mean_additional = sub_pairs_mean_total - sub_pairs_mean_overlap;

                double sub_pairs_min_aligned_ratio = gt_total == 0 ? 0.0 : sub_pairs_min_overlap / (double) gt_total;
                double sub_pairs_max_aligned_ratio = gt_total == 0 ? 0.0 : sub_pairs_max_overlap / (double) gt_total;
                double sub_pairs_mean_aligned_ratio = gt_total == 0 ? 0.0 : sub_pairs_mean_overlap / (double) gt_total;

                double sub_pairs_min_unaligned_ratio = sub_pairs_min_additional / (double) sub_pairs_min_total;
                double sub_pairs_max_unaligned_ratio = sub_pairs_max_additional / (double) sub_pairs_max_total;
                double sub_pairs_mean_unaligned_ratio = sub_pairs_mean_additional / (double) sub_pairs_mean_total;

                sb_min.append("\t").append(sub_pairs_min_total).append("\t").append(sub_pairs_min_overlap).append("\t").append(sub_pairs_min_additional).append("\t").append(sub_pairs_min_aligned_ratio).append("\t").append(sub_pairs_min_unaligned_ratio);
                sb_max.append("\t").append(sub_pairs_max_total).append("\t").append(sub_pairs_max_overlap).append("\t").append(sub_pairs_max_additional).append("\t").append(sub_pairs_max_aligned_ratio).append("\t").append(sub_pairs_max_unaligned_ratio);
                sb_mean.append("\t").append(sub_pairs_mean_total).append("\t").append(sub_pairs_mean_overlap).append("\t").append(sub_pairs_mean_additional).append("\t").append(sub_pairs_mean_aligned_ratio).append("\t").append(sub_pairs_mean_unaligned_ratio);
            }
            sb_min.append("\n");
            sb_max.append("\n");
            sb_mean.append("\n");
        }

        FileUtils.saveText(sb_min.toString(), out_file_min);
        FileUtils.saveText(sb_max.toString(), out_file_max);
        FileUtils.saveText(sb_mean.toString(), out_file_mean);
    }

    /**
     * Compute the Milne-Witten score between entity pairs which contain possible table candidates for alignment.
     *
     * @param wiki_articles
     * @param anchor_data
     * @param out_dir
     * @throws IOException
     */
    public static void computeMWRelatednessScores(String wiki_articles, String anchor_data, String out_dir) throws IOException {
        WikiAnchorGraph wg = new WikiAnchorGraph();
        wg.isOutLinks = false;
        System.out.println("Loading Wikipedia in-degree anchor graph.");
        wg.loadEntityIndex(wiki_articles, true, out_dir);
        wg.loadInDegreeAnchorGraph(anchor_data, out_dir);

        //compute the MW scores
        DecimalFormat df = new DecimalFormat("#.00");
        Map<String, Set<String>> entity_pairs = new HashMap<>();
        seed_entities.forEach(e -> entity_pairs.put(e, filter_entities));
        Map<String, Map<String, Set<String>>> pair_scores = wg.computeMilneWittenScorePairs(entity_pairs, out_dir, df);
        System.out.printf("Finished computing the MW scores for %d entities.\n", entity_pairs.size());

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tscore\tgt_total\taligned\tunaligned\taligned_ratio\tunaligned_ratio\n");


        for (String entity : seed_entities) {
            if (!pair_scores.containsKey(entity)) {
                System.out.printf("Entity %s is missing.\n", entity);
                continue;
            }

            Map<String, Set<String>> sub_pairs = pair_scores.get(entity);
            //compute all the measures.
            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            for (String score : sub_pairs.keySet()) {
                Set<String> sub_entities = sub_pairs.get(score);
                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());

                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                double aligned_ratio = gt_total == 0 ? 0.0 : (double) overlapping / gt_total;
                double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
                sb.append(entity).append("\t").append(gt_total).append("\t").append(score).append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio).append("\n");
            }
        }

        FileUtils.saveText(sb.toString(), out_dir + "/mw_entity_pairs.tsv");
    }

    /**
     * Check how many of the entity pairs which we generate in a greedy way are in the ground-truth .
     *
     * @param out_dir
     */
    public static void computeGreedyCoverage(String out_dir) throws IOException {
        StringBuffer sb = new StringBuffer();
        String out_file = out_dir + "/greedy_entity_pair_coverage.tsv";
        sb.append("entity\tlevel\tall_candidates\tgt_total\toverlap\tunaligned_entities\taligned_ratio\tunaligned_ratio\n");

        for (String entity : seed_entities) {
            Set<String> sub_gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            Set<String> sub_greedy_entities = new HashSet<>(filter_entities);

            //compute all the measures.
            int candidate_total = sub_greedy_entities.size();
            int gt_total = sub_gt_entities.size();

            sub_greedy_entities.retainAll(sub_gt_entities);
            int overlapping = sub_greedy_entities.size();
            int additional = candidate_total - overlapping;
            double aligned_ratio = (double) overlapping / gt_total;
            double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
            sb.append(entity).append("\t").append(0).append("\t").append(gt_total).append("\t").append(candidate_total).
                    append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).
                    append("\t").append(unaligned_ratio).append("\n");
        }
        FileUtils.saveText(sb.toString(), out_file);
    }

    /**
     * Compute the basic SimRank approach.
     *
     * @param damping_factor
     * @param iterations
     * @param entity_pairs
     * @param anchor_data
     * @param out_dir
     * @param wiki_articles
     * @throws IOException
     */
    public static void computeSimRankGraphSimple(double damping_factor, int iterations, String entity_pairs, String anchor_data, String out_dir, String wiki_articles) throws IOException {
        WikiAnchorGraph wg = new WikiAnchorGraph();
        System.out.println("Loading Wikipedia in-degree anchor graph.");
        wg.loadEntityIndex(wiki_articles, true, out_dir);
        if (!FileUtils.fileExists(out_dir + "/simrank_scores_subsample_50.tsv", false)) {
            wg.loadInDegreeAnchorGraph(anchor_data, out_dir);
            System.out.println("Initializing the feature weights.");
            wg.initialize();
            System.out.println("Loading the filters for which we wanna compute the sim-rank scores.");
            wg.readEntityFilterFiles(entity_pairs);

            wg.computeGraphSimRank(damping_factor, iterations);
            wg.writeSimRankScores(out_dir);
        }

        //load the simrank scores
        String[] lines = FileUtils.readText(out_dir + "/simrank_scores_subsample_50.tsv").split("\n");
        Map<Integer, Map<Integer, Double>> simrank = new HashMap<>();
        for (String line : lines) {
            String[] tmp = line.split("\t");
            try {
                int a = Integer.valueOf(tmp[0]);
                int b = Integer.valueOf(tmp[1]);
                double score = Double.parseDouble(tmp[2]);

                if (!simrank.containsKey(a)) {
                    simrank.put(a, new HashMap<>());
                }
                simrank.get(a).put(b, score);
            } catch (Exception e) {
                System.out.println(line);
            }
        }

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tlevel\tgt_total");

        for (int i = 0; i < cutoffs.length; i++) {
            sb.append("\tall_candidates[").append(cutoffs[i]).append("]\toverlap[").append(cutoffs[i]).append("]\tunaligned_entities[").append(cutoffs[i]).append("]");
            sb.append("\taligned_ratio[").append(cutoffs[i]).append("]\tunaligned_ratio[").append(cutoffs[i]).append("]");
        }
        sb.append("\n");

        for (String entity : seed_entities) {
            if (!wg.index_entities.containsKey(entity)) {
                System.out.printf("Entity %s is missing from the index.\n", entity);
                continue;
            }
            int entity_idx = wg.index_entities.get(entity);
            if (!simrank.containsKey(entity_idx)) {
                System.out.printf("Entity %d with %s is missing.\n", entity_idx, entity);
                continue;
            }

            Map<Integer, Double> sub_pairs = simrank.get(entity_idx);
            //compute all the measures.
            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            sb.append(entity).append("\t").append(0).append("\t").append(gt_total);
            for (double val : cutoffs) {
                Set<String> sub_entities = sub_pairs.entrySet().stream().filter(s -> s.getValue() >= val).map(s -> wg.entities.get(s.getKey())).collect(Collectors.toSet());
                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                double aligned_ratio = (double) overlapping / gt_total;
                double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;

                sb.append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio);
            }
            sb.append("\n");
        }
        FileUtils.saveText(sb.toString(), out_dir + "/sim_rank_pairs_coverage.tsv");
    }

    /**
     * DEBUG: Compute the coverage of entity pairs for table alignment if we pick entities that belong to the category
     * directly associated with our seed entity and is the deepest in the Wikipedia category taxonomy.
     *
     * @param article_categories
     * @param category_path
     */
    public static void debugEntityPairCategoryCoverage(String article_categories, String category_path, String out_dir) throws IOException {
        loadEntityCategoryDataStructures(article_categories, category_path);
        //we perform the experiments only for this subset.
        System.out.println("Measuring coverage for " + gt_pairs.size() + " entities.");

        for (String entity : seed_entities) {
            StringBuffer sb = new StringBuffer();
            List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cat_to_map.containsKey(cat)).map(cat -> cat_to_map.get(cat)).collect(Collectors.toList());
            Map<String, Set<String>> cat_direct_debug = new HashMap<>();

            // add the entities for debugging purposes.
            categories.forEach(cat -> cat_direct_debug.put(cat.label, cat.entities));
            categories.forEach(cat -> cat.parents.values().forEach(parent -> cat_direct_debug.put(cat.label + "\t" + parent.label, parent.entities)));

            //check the overlap with each of the categories
            Set<String> sub_gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            int gt_total = sub_gt_entities.size();
            for (String cat : cat_direct_debug.keySet()) {
                Set<String> sub_cat_entities = cat_direct_debug.get(cat);
                int total = sub_cat_entities.size();

                //check the overlap
                sub_cat_entities.retainAll(sub_gt_entities);

                double aligned_ratio = gt_total == 0 ? 0.0 : sub_cat_entities.size() / (double) gt_total;
                double unaligned_ratio = (total - sub_cat_entities.size()) / (double) total;
                int aligned = sub_cat_entities.size();
                int unaligned = total - aligned;

                sb.append(entity).append("\t").append(cat).append("\t").append(gt_total).append("\t").append(aligned).append("\t").append(unaligned).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio).append("\n");
            }
            FileUtils.saveText(sb.toString(), out_dir + "/category_taxonomy_same_level_debug.tsv", true);
        }
    }

    /**
     * Compute the coverage of entity pairs for table alignment if we pick entities that belong to the category
     * directly associated with our seed entity and is the deepest in the Wikipedia category taxonomy.
     *
     * @param article_categories
     * @param category_path
     */
    public static void computeEntityPairCategoryCoverage(String article_categories, String category_path, String out_dir, String cat_type) throws IOException {
        //we perform the experiments only for this subset.
        System.out.println("Measuring coverage for " + gt_pairs.size() + " entities.");

        //for each entity check the additional pairs that are extracted from the same category, and additionally check its coverage.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tcat_level\tgt_total\tall_candidates\toverlap\tunaligned_entities\taligned_ratio\tunaligned_ratio\n");
        Map<String, Map.Entry<Integer, Set<String>>> max_level_entities = loadEntitiesByCategory(article_categories, category_path, cat_type);
        System.out.printf("Loaded the corresponding pairs for %d entities for %s.\n", max_level_entities.size(), cat_type);

        for (String entity : seed_entities) {
            if (!max_level_entities.containsKey(entity)) {
                System.out.printf("Entity %s is missing.\n", entity);
                continue;
            }
            //we need to check here if there are any items from the ground-truth, otherwise
            Map.Entry<Integer, Set<String>> entity_pairs = max_level_entities.get(entity);

            Set<String> pairs = entity_pairs.getValue();
            pairs.retainAll(filter_entities);
            int candidate_total = entity_pairs.getValue().size();

            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            if (gt_pairs.containsKey(entity)) {
                pairs.retainAll(gt_pairs.get(entity));
            }
            int overlapping = pairs.size();

            int additional = candidate_total - overlapping;
            double aligned_ratio = gt_total == 0 ? 0.0 : (double) overlapping / gt_total;
            double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
            sb.append(entity).append("\t").append(entity_pairs.getKey()).append("\t").append(gt_total).append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio).append("\n");
        }
        String out_file = out_dir + "/coverage_taxonomy_cat_" + cat_type + ".tsv";
        FileUtils.saveText(sb.toString(), out_file);
    }


    /**
     * Compute the coverage of entity pairs for table alignment if we pick entities that belong to the category
     * directly associated with our seed entity and is the deepest in the Wikipedia category taxonomy.
     *
     * @param out_dir
     * @param cat_rep_sim
     */
    public static void computeCategoryRepSimilarityCoverage(String cat_rep_path, String article_categories, String cat_rep_sim, String out_dir) throws IOException {
        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(cat_rep_path);
        Map<String, Map<String, Double>> entity_cat_rep_score = loadCategorySimilarity(cat, seed_entities, cat_rep_sim);
        cat_entities = DataUtils.readCategoryMappingsWiki(article_categories, null);

        double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 5.0, 10.0, Double.MAX_VALUE};

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tlevel\tgt_total");

        for (int i = 0; i < cutoffs.length; i++) {
            sb.append("\tall_candidates[").append(cutoffs[i]).append("]\toverlap[").append(cutoffs[i]).append("]\tunaligned_entities[").append(cutoffs[i]).append("]");
            sb.append("\taligned_ratio[").append(cutoffs[i]).append("]\tunaligned_ratio[").append(cutoffs[i]).append("]");
        }
        sb.append("\n");

        //output the results
        for (String entity : seed_entities) {
            if (!entity_cat_rep_score.containsKey(entity)) {
                System.out.println("There is no information for entity " + entity);
                continue;
            }
            System.out.println("Processing entity " + entity);
            Map<String, Double> category_info = entity_cat_rep_score.get(entity);

            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            sb.append(entity).append("\t").append(0).append("\t").append(gt_total);

            for (double val : cutoffs) {
                Set<String> sub_cats = category_info.entrySet().stream().filter(s -> s.getValue() <= val).map(s -> s.getKey()).collect(Collectors.toSet());
                Set<String> sub_entities = new HashSet<>();

                if (sub_cats != null && !sub_cats.isEmpty()) {
                    sub_cats.stream().filter(s -> cat_entities.containsKey(s)).forEach(s -> sub_entities.addAll(cat_entities.get(s)));
                    sub_entities.retainAll(filter_entities);
                }

                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                double aligned_ratio = (double) overlapping / gt_total;
                double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
                sb.append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio);
            }
            sb.append("\n");
        }

        FileUtils.saveText(sb.toString(), out_dir + "/coverage_cat_rep_sim.tsv");
    }


    /**
     * Get the entities that are associated with categories of a specific depth, entities that belong to a set of
     * directly connected categories, and parents of those categories.
     *
     * @param category_path
     * @param article_categories
     * @return
     */
    public static Map<String, Map.Entry<Integer, Set<String>>> loadEntitiesByCategory(String article_categories, String category_path, String cat_type) throws IOException {
        loadEntityCategoryDataStructures(article_categories, category_path);
        Map<String, Map.Entry<Integer, Set<String>>> max_level_entities = new HashMap<>();
        for (String entity : seed_entities) {
            //an entity is directly associated to multiple categories.
            Set<String> entity_pairs = new HashSet<>();
            if (!entity_cats.containsKey(entity)) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cat_to_map.containsKey(cat)).map(cat -> cat_to_map.get(cat)).collect(Collectors.toList());
            if (categories == null || categories.isEmpty()) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            //retrieve the categories that belong at the same depth in the category taxonomy.
            int max_level = categories.stream().mapToInt(cat -> cat.level).max().getAsInt();

            System.out.printf("Entity %s has the maximal level %d and has %d categories.\n", entity, max_level, categories.size());

            if (cat_type == "deepest") {
                //add all the entities that belong to the deepest category directly associated with our seed entity
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("same_level")) {
                cat_to_map.keySet().stream().filter(x -> cat_to_map.get(x).level == max_level).forEach(x -> entity_pairs.addAll(cat_to_map.get(x).entities));
            } else if (cat_type.equals("direct")) {
                categories.forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("direct_parents")) {
                categories.forEach(cat -> cat.parents.values().forEach(parent -> entity_pairs.addAll(parent.entities)));
                categories.forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("deepest_parents")) {
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> cat.parents.values().forEach(parent -> entity_pairs.addAll(parent.entities)));
            }

            entity_pairs.retainAll(filter_entities);
            max_level_entities.put(entity, new AbstractMap.SimpleEntry<>(max_level, entity_pairs));
        }
        return max_level_entities;
    }


    /**
     * Load the category taxonomy and the entity-category data structures.
     *
     * @param article_categories
     * @param category_path
     * @throws IOException
     */
    public static void loadEntityCategoryDataStructures(String article_categories, String category_path) throws IOException {
        //load the entity and category information.
        cat_entities = DataUtils.readCategoryMappingsWiki(article_categories, null);
        entity_cats = DataUtils.getArticleCategories(cat_entities);
        CategoryRepresentation cats = CategoryRepresentation.readCategoryGraph(category_path);
        DataUtils.updateCatsWithEntities(cats, cat_entities);

        cat_to_map = new HashMap<>();
        cats.loadIntoMapChildCats(cat_to_map);

        System.out.printf("There are %d categories.\n", cat_entities.size());
        System.out.printf("There are %d entities.\n", entity_cats.size());
        System.out.printf("There are %d cat-to-map and the root has %d entities.\n", cat_to_map.size(), cats.entities.size());
    }


    /**
     * Load the category similarities for a given set of seed entities. For each entity we load the similarity to all
     * its associated categories.
     *
     * @param entities
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, Double>> loadCategorySimilarity(CategoryRepresentation cat,
                                                                          Set<String> entities,
                                                                          String file) throws IOException {
        Map<String, Map<String, Double>> csim = new HashMap<>();
        //load first the category representation for each entity by traversing the category graph.
        Map<String, Set<String>> entity_cats = new HashMap<>();
        traverseEntityCats(entities, cat, entity_cats);

        //load all the categories of the seed entities into a set to filter out the file.
        Set<String> seed_cats = entity_cats.keySet();

        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            String cat_a = data[0];
            String cat_b = data[1];
            double score = Double.parseDouble(data[5]);

            if (!seed_cats.contains(cat_a)) {
                continue;
            }

            //load the data for each category
            for (String entity : entity_cats.get(cat_a)) {
                if (!csim.containsKey(entity)) {
                    csim.put(entity, new HashMap<>());
                }
                Map<String, Double> e_csim = csim.get(entity);
                e_csim.put(cat_b, score);
            }
        }
        return csim;
    }


    /**
     * Load all the categories for a set of seed entities.
     *
     * @param seed_entities
     * @param cat
     * @param cat_entity
     */
    public static void traverseEntityCats(Set<String> seed_entities, CategoryRepresentation cat, Map<String, Set<String>> cat_entity) {
        if (!cat.entities.isEmpty()) {
            seed_entities.stream().filter(entity -> cat.entities.contains(entity)).forEach(entity -> {
                if (!cat_entity.containsKey(cat.label)) {
                    cat_entity.put(cat.label, new HashSet<>());
                }
                cat_entity.get(cat.label).add(entity);
            });
        }

        //we proceed to the children of this category
        cat.children.entrySet().forEach(child -> traverseEntityCats(seed_entities, child.getValue(), cat_entity));
    }
}
