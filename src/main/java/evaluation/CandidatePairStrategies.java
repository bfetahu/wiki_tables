package evaluation;

import datastruct.WikiAnchorGraph;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by besnik on 2/7/18.
 */
public class CandidatePairStrategies {
    public static Map<String, CategoryRepresentation> cat_to_map;
    public static Map<String, Set<String>> entity_cats;
    public static Map<String, Set<String>> cat_entities;

    //all the article-candidate pairs which are also in our ground-truth.
    public static Map<String, Set<String>> gt_pairs;
    public static Set<String> filter_entities;
    public static Set<String> seed_entities;

    public static void main(String[] args) throws IOException {
        String all_pairs = "", out_dir = "", option = "", article_categories = "", category_path = "", anchor_data = "", wiki_articles = "";
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
            }
        }

        //evaluate the different candidate pair strategies.
        if (option.equals("level")) {
            computeEntityPairTaxonomyLevelCoverage(article_categories, category_path, out_dir);
        } else if (option.equals("rep_sim")) {
            computeCategoryRepSimilarityCoverage(article_categories, category_path, all_pairs, out_dir);
        } else if (option.equals("simrank")) {
            computeSimRankGraphSimple(damping_factor, iterations, all_pairs, anchor_data, out_dir, wiki_articles);
        } else if (option.equals("greedy")) {
            computeGreedyCoverage(out_dir);
        } else if (option.equals("mw")) {
            computeMWRelatednessScores(wiki_articles, anchor_data, out_dir);
        }
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
        Map<String, Set<String>> entity_pairs = new HashMap<>();
        seed_entities.forEach(e -> entity_pairs.put(e, filter_entities));
        Map<String, Map<String, Double>> pair_scores = wg.computeMilneWittenScorePairs(entity_pairs, out_dir);
        System.out.printf("Finished computing the MW scores for %d entities.\n", entity_pairs.size());

        double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tlevel\tgt_total");

        for (int i = 0; i < cutoffs.length; i++) {
            sb.append("\tall_candidates[").append(cutoffs[i]).append("]\toverlap[").append(cutoffs[i]).append("]\tunaligned_entities[").append(cutoffs[i]).append("]");
        }
        sb.append("\n");

        for (String entity : seed_entities) {
            if (!pair_scores.containsKey(entity)) {
                System.out.printf("Entity %s is missing.\n", entity);
                continue;
            }

            Map<String, Double> sub_pairs = pair_scores.get(entity);
            //compute all the measures.
            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            sb.append(entity).append("\t").append(0).append("\t").append(gt_total);
            for (double val : cutoffs) {
                Set<String> sub_entities = sub_pairs.entrySet().stream().filter(s -> s.getValue() >= val).map(s -> s.getKey()).collect(Collectors.toSet());
                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                sb.append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional);
            }
            sb.append("\n");
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
        sb.append("entity\tlevel\tall_candidates\tgt_total\toverlap\tunaligned_entities\n");

        for (String entity : seed_entities) {
            Set<String> sub_gt_entities = gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>();
            Set<String> sub_greedy_entities = new HashSet<>(filter_entities);
            sub_greedy_entities.remove(entity);

            //compute all the measures.
            int candidate_total = sub_greedy_entities.size();
            int gt_total = sub_gt_entities.size();

            sub_greedy_entities.retainAll(sub_gt_entities);
            int overlapping = sub_greedy_entities.size();
            int additional = candidate_total - overlapping;

            sb.append(entity).append("\t").append(0).append("\t").append(candidate_total).append("\t").append(gt_total).append("\t").append(overlapping).append("\t").append(additional).append("\n");
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
        double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

        //compute for different relatedness scores the number of relevant/irrelevant pairs.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tlevel\tgt_total");

        for (int i = 0; i < cutoffs.length; i++) {
            sb.append("\tall_candidates[").append(cutoffs[i]).append("]\toverlap[").append(cutoffs[i]).append("]\tunaligned_entities[").append(cutoffs[i]).append("]");
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
                Set<String> sub_entities = sub_pairs.entrySet().stream().filter(s -> s.getValue() >= val).map(s -> wg.entities.get(s)).collect(Collectors.toSet());
                int candidate_total = sub_entities.size();
                sub_entities.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
                int overlapping = sub_entities.size();
                int additional = candidate_total - overlapping;
                sb.append("\t").append(candidate_total).append("\t").append(overlapping).append("\t").append(additional);
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
    public static void computeEntityPairTaxonomyLevelCoverage(String article_categories, String category_path, String out_dir) throws IOException {
        //we perform the experiments only for this subset.
        System.out.println("Measuring coverage for " + gt_pairs.size() + " entities.");

        //for each entity check the additional pairs that are extracted from the same category, and additionally check its coverage.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tcat_max_level\tall_candidates\tgt_total\toverlap\tunaligned_entities\n");
        Map<String, Map.Entry<Integer, Set<String>>> max_level_entities = loadEntitiesDeepestCategory(article_categories, category_path);
        for (String entity : seed_entities) {
            if (!max_level_entities.containsKey(entity)) {
                System.out.printf("Entity %s is missing.\n", entity);
                continue;
            }
            //retrieve the categories that belong at the same depth in the category taxonomy.
            int candidate_total = 0, gt_total = 0, overlapping = 0;

            //we need to check here if there are any items from the ground-truth, otherwise
            Map.Entry<Integer, Set<String>> entity_pairs = max_level_entities.get(entity);

            Set<String> pairs = entity_pairs.getValue();
            pairs.retainAll(filter_entities);
            candidate_total = entity_pairs.getValue().size();

            gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;
            if (gt_pairs.containsKey(entity)) {
                pairs.retainAll(gt_pairs.get(entity));
            }
            overlapping = pairs.size();

            int additional = candidate_total - overlapping;
            sb.append(entity).append("\t").append(entity_pairs.getKey()).append("\t").append(candidate_total).append("\t").append(gt_total).append("\t").append(overlapping).append("\t").append(additional).append("\n");
        }

        FileUtils.saveText(sb.toString(), out_dir + "/coverage_taxonomy_cat_max_level.tsv");
    }


    /**
     * Compute the coverage of entity pairs for table alignment if we pick entities that belong to the category
     * directly associated with our seed entity and is the deepest in the Wikipedia category taxonomy.
     *
     * @param out_dir
     * @param cat_rep_sim
     */
    public static void computeCategoryRepSimilarityCoverage(String article_categories, String category_path, String cat_rep_sim, String out_dir) throws IOException {
        loadEntityCategoryDataStructures(article_categories, category_path);
        StringBuffer sb = new StringBuffer();

        String line;
        BufferedReader reader = FileUtils.getFileReader(cat_rep_sim);
        Map<String, Map.Entry<String, Double>> entity_cat_rep_score = new HashMap<>();

        int total = 0;
        while ((line = reader.readLine()) != null) {
            try {
                String[] data = line.split("\t");
                total++;

                if (total % 100000 == 0) {
                    System.out.println("Processed " + total);
                }

                String article_a = data[0];
                String article_b = data[1];
                String cat_a = data[3];
                double score = Double.parseDouble(data[4]);

                if (!cat_to_map.containsKey(cat_a) || !filter_entities.contains(article_b)) {
                    continue;
                }

                if (!entity_cat_rep_score.containsKey(article_a)) {
                    entity_cat_rep_score.put(article_a, new AbstractMap.SimpleEntry<>(cat_a, score));
                } else if (entity_cat_rep_score.get(article_a).getValue() < score) {
                    entity_cat_rep_score.put(article_a, new AbstractMap.SimpleEntry<>(cat_a, score));
                }
            } catch (Exception e) {
                System.out.println(line);
            }
        }

        //output the results
        for (String entity : seed_entities) {
            if (!entity_cat_rep_score.containsKey(entity)) {
                System.out.println("There is no information for entity " + entity);
                continue;
            }
            System.out.println("Processing entity " + entity);
            Map.Entry<String, Double> category_info = entity_cat_rep_score.get(entity);
            //here we get the level of the category from the candidate articles
            int level = cat_to_map.get(category_info.getKey()).level;

            //compute all the measures.
            Set<String> entity_pairs = cat_entities.get(category_info.getKey());
            int candidate_total = entity_pairs.size();
            int gt_total = gt_pairs.containsKey(entity) ? gt_pairs.get(entity).size() : 0;

            entity_pairs.retainAll(gt_pairs.containsKey(entity) ? gt_pairs.get(entity) : new HashSet<>());
            int overlapping = entity_pairs.size();
            int additional = candidate_total - overlapping;

            sb.append(entity).append("\t").append(level).append("\t").append(candidate_total).append("\t").append(gt_total).append("\t").append(overlapping).append("\t").append(additional).append("\n");
        }

        FileUtils.saveText(sb.toString(), out_dir + "/coverage_cat_rep_sim.tsv");
    }


    /**
     * Get the entities that are associated with the deepest category for a set of seed entities.
     *
     * @param category_path
     * @param article_categories
     * @return
     */
    public static Map<String, Map.Entry<Integer, Set<String>>> loadEntitiesDeepestCategory(String article_categories, String category_path) throws IOException {
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

            //add all the entities that belong to the deepest category directly associated with our seed entity
            categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));

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
    }
}
