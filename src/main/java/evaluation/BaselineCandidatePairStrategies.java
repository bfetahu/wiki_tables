package evaluation;

import datastruct.TableCandidateFeatures;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;
import org.apache.commons.lang3.tuple.Triple;
import representation.CategoryRepresentation;
import representation.WikiAnchorGraph;
import utils.DataUtils;

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
                category_path = "", anchor_data = "", wiki_articles = "", cat_type = "";
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
        } else if (option.equals("rep_sim")) {
            computeCategoryRepSimilarityCoverage(category_path, all_pairs, out_dir, false);
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
        Map<String, Double> cat_sums = new HashMap<>();
        if (!isEntity) {
            loadEntityCategoryDataStructures(article_categories, category_path);

            //compute the sums
            double epsilon = 1e-3;
            for (String cat : cat_to_map.keySet()) {
                TDoubleArrayList emb = node2vec.get(cat.replaceAll(" ", "_"));
                if (emb == null) {
                    cat_sums.put(cat, epsilon);
                } else {
                    double sum = Arrays.stream(emb.toArray()).map(x -> Math.pow(x, 2)).sum();
                    cat_sums.put(cat, sum);
                }
            }
        }

        Map<String, Integer> entity_idx = new HashMap<>();
        filter_entities.forEach(entity -> entity_idx.put(entity, entity.hashCode()));

        String out_file = isEntity ? out_dir + "/entity_embedding_coverage.tsv" : out_dir + "/category_embedding_coverage.tsv";
        FileUtils.saveText("entity\tgt_total\tsim\tall_candidates\toverlap\tunaligned_entities\taligned_ratio\tunaligned_ratio\n", out_file);

        //check for each entity the coverage similarity.
        for (String entity : seed_entities) {
            System.out.println("Processing seed entity " + entity);
            TDoubleArrayList entity_emb = node2vec.get(entity.replaceAll(" ", "_"));
            Set<String> gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            int gt_total = gt_entities.size();

            DecimalFormat df = new DecimalFormat("#.0");
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
                    double sum_a = cat_sums.get(cat.label);
                    System.out.println("Computing the category similarity for entity " + entity + " and category " + cat.label + " with " + cat.entities.size());
                    cat_to_map.values().forEach(candidate_category -> {
                        double sum_b = cat_sums.get(candidate_category.label);
                        double score = 0.0;
                        if (cat.label.equals(candidate_category.label)) {
                            score = 1.0;
                        } else {
                            TDoubleArrayList candidate_cat_emb = node2vec.get(candidate_category.label.replaceAll(" ", "_"));
                            score = Double.parseDouble(df.format(DataUtils.computeCosineSim(cat_emb, candidate_cat_emb, sum_a, sum_b)));
                        }

                        if (!candidates.containsKey(score)) {
                            candidates.put(score, new HashSet<>());
                        }
                        candidates.get(score).add(candidate_category.label);
                    });
                }
                System.out.println("Finished computing category similarity for entity " + entity);
            }

            //group the entities based on the existing thresholds.
            Map<Double, TIntHashSet> entities = new TreeMap<>();
            TIntHashSet gt_idx = new TIntHashSet();
            gt_entities.forEach(e -> gt_idx.add(entity_idx.get(e)));

            for (double score : candidates.keySet()) {
                if (!isEntity) {
                    Set<String> sub_entities = new HashSet<>();
                    candidates.get(score).forEach(cat -> sub_entities.addAll(cat_to_map.get(cat).entities));
                    sub_entities.retainAll(filter_entities);

                    TIntHashSet sub_set = new TIntHashSet();
                    sub_entities.forEach(e -> sub_set.add(entity_idx.get(e)));
                    entities.put(score, sub_set);
                } else {
                    Set<String> sub_entities = candidates.get(score);
                    TIntHashSet sub_set = new TIntHashSet();
                    sub_entities.forEach(e -> sub_set.add(entity_idx.get(e)));
                    entities.put(score, sub_set);
                }
            }

            Map<Double, Map.Entry<Integer, Integer>> cumm_entities = new TreeMap<>();
            for (double score : candidates.keySet()) {
                TIntHashSet cumm = new TIntHashSet(entities.get(score));
                candidates.keySet().stream().filter(score_cmp -> score_cmp >= score).forEach(score_cmp -> cumm.addAll(entities.get(score_cmp)));
                cumm.retainAll(gt_idx);
                cumm_entities.put(score, new AbstractMap.SimpleEntry<>(cumm.size(), gt_idx.size()));
            }

            //output the data
            String coverage_stats = writeCoverageStats(cumm_entities, gt_total, entity);
            FileUtils.saveText(coverage_stats, out_file, true);
        }
    }

    /**
     * Write the coverage statistics in a standard format we are using.
     * @param cumm_entities
     * @param gt_total
     * @param entity
     * @return
     */
    public static String writeCoverageStats(Map<Double, Map.Entry<Integer, Integer>> cumm_entities, int gt_total, String entity) {
        //output the data
        StringBuffer sb = new StringBuffer();
        for (double score : cumm_entities.keySet()) {
            int candidate_total = cumm_entities.get(score).getKey();
            int overlapping = cumm_entities.get(score).getValue();
            int additional = candidate_total - overlapping;
            double aligned_ratio = gt_total == 0 ? 0.0 : (double) overlapping / gt_total;
            double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
            sb.append(entity).append("\t").append(gt_total).append("\t").append(score).append("\t").append(candidate_total).
                    append("\t").append(overlapping).append("\t").append(additional).append("\t").append(aligned_ratio).
                    append("\t").append(unaligned_ratio).append("\n");
        }
        return sb.toString();
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
        sb.append("entity\tlevel\tgt_total\tall_candidates\toverlap\tunaligned_entities\taligned_ratio\tunaligned_ratio\n");

        for (String entity : seed_entities) {
            Set<String> sub_gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            Set<String> sub_greedy_entities = new HashSet<>(filter_entities);

            //compute all the measures.
            int candidate_total = sub_greedy_entities.size();
            int gt_total = sub_gt_entities.size();

            sub_greedy_entities.retainAll(sub_gt_entities);
            int overlapping = sub_greedy_entities.size();
            int additional = candidate_total - overlapping;
            double aligned_ratio = gt_total == 0 ? 0 : (double) overlapping / gt_total;
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
    public static void computeCategoryRepSimilarityCoverage(String cat_rep_path, String cat_rep_sim, String out_dir, boolean isEuclidean) throws IOException {
        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(cat_rep_path);
        Map<String, CategoryRepresentation> cat_to_map = new HashMap<>();
        cat.loadIntoMapChildCats(cat_to_map);
        TIntObjectHashMap<TIntDoubleHashMap> cat_sim = loadCategorySimilarity(cat, cat_to_map, seed_entities, cat_rep_sim, isEuclidean);


        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tgt_total\tsim\tall_candidates\toverlap\tunalgined\taligned_ratio\tunaligned_ratio\n");

        Map<String, Map<Double, Set<String>>> entity_candidates = new HashMap<>();

        DecimalFormat df = new DecimalFormat("0.#");
        for (String entity : seed_entities) {
            Map<Double, Set<String>> sub_candidates = new TreeMap<>();
            entity_candidates.put(entity, sub_candidates);
            for (String cat_a : entity_cats.get(entity)) {
                int cat_a_hash = cat_a.hashCode();
                for (String cat_b : cat_to_map.keySet()) {
                    int cat_b_has = cat_b.hashCode();

                    if (cat_sim.containsKey(cat_a_hash) && cat_sim.get(cat_a_hash).containsKey(cat_b_has)) {
                        double sim = Double.parseDouble(df.format(cat_sim.get(cat_a_hash).get(cat_b_has)));
                        if (!sub_candidates.containsKey(sim)) {
                            sub_candidates.put(sim, new HashSet<>());
                        }
                        sub_candidates.get(sim).addAll(cat_to_map.get(cat_b).entities);
                    } else {
                        if (!sub_candidates.containsKey(0.0)) {
                            sub_candidates.put(0.0, new HashSet<>());
                        }
                        sub_candidates.get(0.0).addAll(cat_to_map.get(cat_b).entities);
                    }
                }
            }
        }
        //output the results
        for (String entity : seed_entities) {
            System.out.println("Processing entity " + entity);
            Map<Double, Set<String>> sub_pairs = entity_candidates.get(entity);

            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            for (double val : sub_pairs.keySet()) {
                Set<String> sub_entities = new HashSet<>();
                sub_pairs.keySet().stream().filter(d -> d >= val).forEach(d -> sub_entities.addAll(sub_pairs.get(d)));
                sub_entities.retainAll(filter_entities);

                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                double aligned_ratio = gt_total == 0 ? 0.0 : (double) overlapping / gt_total;
                double unaligned_ratio = (double) (candidate_total - overlapping) / candidate_total;
                sb.append(entity).append("\t").append(val).append("\t").append(gt_total).append("\t").
                        append(candidate_total).append("\t").append(overlapping).append("\t").
                        append(additional).append("\t").append(aligned_ratio).append("\t").append(unaligned_ratio).append("\n");
            }
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

        System.out.println("There are " + filter_entities.size() + " filter entities.");

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
            int max_sum = categories.stream().filter(cat -> cat.level == max_level).mapToInt(cat -> cat.entities.size()).sum();

            System.out.printf("Entity %s has the maximal level %d and has %d categories with %d entities from the deepest categories.\n", entity, max_level, categories.size(), max_sum);

            if (cat_type.equals("deepest")) {
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
    public static TIntObjectHashMap<TIntDoubleHashMap> loadCategorySimilarity(CategoryRepresentation cat,
                                                                              Map<String, CategoryRepresentation> cat_to_map,
                                                                              Set<String> entities,
                                                                              String file, boolean isEuclidean) throws IOException {
        Map<String, Map<String, Double>> scores = new HashMap<>();
        //load first the category representation for each entity by traversing the category graph.
        Map<String, Set<String>> entity_cats = new HashMap<>();
        traverseEntityCats(entities, cat, entity_cats);

        //load all the categories of the seed entities into a set to filter out the file.
        Set<String> seed_cats = entity_cats.keySet();

        //if the similarity file does not exist, compute the category similarities.
        if (!FileUtils.fileExists(file, false)) {
            Map<String, Double> attribute_max_level_category = DataUtils.computeMaxLevelAttributeCategory(cat_to_map);
            Map<String, TIntDoubleHashMap> cat_weights = DataUtils.computeCategoryAttributeWeights(attribute_max_level_category, cat_to_map);

            Set<String> all_cats = cat_to_map.keySet();
            seed_cats.forEach(cat_a -> scores.put(cat_a, new HashMap<>()));

            seed_cats.parallelStream().forEach(cat_a -> {
                Map<String, Double> sub_scores = scores.get(cat_a);
                TIntDoubleHashMap cat_a_weights = cat_weights.get(cat_a);

                if (cat_a_weights == null) {
                    return;
                }
                for (String cat_b : all_cats) {
                    TIntDoubleHashMap cat_b_weights = cat_weights.get(cat_b);

                    if (cat_b_weights == null) {
                        continue;
                    }
                    double score = isEuclidean ? DataUtils.computeEuclideanDistance(cat_a_weights, cat_b_weights) : DataUtils.computeCosine(cat_a_weights, cat_b_weights);
                    sub_scores.put(cat_b, score);
                }
            });

            //write the output
            StringBuffer sb = new StringBuffer();
            for (String cat_a : scores.keySet()) {
                for (String cat_b : scores.get(cat_a).keySet()) {
                    sb.append(cat_a).append("\t").append(cat_b).append("\t").append(scores.get(cat_a).get(cat_b)).append("\n");

                    if (sb.length() > 10000) {
                        FileUtils.saveText(sb.toString(), file, true);
                        sb.delete(0, sb.length());
                    }
                }
            }
            FileUtils.saveText(sb.toString(), file, true);

        }
        return DataUtils.loadCategoryRepSim(file);
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
