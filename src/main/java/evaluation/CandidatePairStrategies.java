package evaluation;

import datastruct.WikiAnchorGraph;
import io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
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
    //all the article-candidate pairs which are also in our ground-truth.
    public static Map<String, Set<String>> gt_pairs;

    public static void main(String[] args) throws IOException {
        String all_pairs = "", out_dir = "", option = "", article_categories = "", category_path = "", anchor_data = "";
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
            }
        }

        //load the entity and category information.
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_categories, null);
        Map<String, Set<String>> entity_to_cats = DataUtils.getArticleCategories(cats_entities);
        CategoryRepresentation cats = CategoryRepresentation.readCategoryGraph(category_path);
        DataUtils.updateCatsWithEntities(cats, cats_entities);
        Map<String, CategoryRepresentation> cat_to_map = new HashMap<>();
        cats.loadIntoMapChildCats(cat_to_map);


        //evaluate the different candidate pair strategies.
        if (option.equals("level")) {
            computeEntityPairTaxonomyLevelCoverage(cat_to_map, entity_to_cats, out_dir);
        } else if (option.equals("rep_sim")) {
            computeCategoryRepSimilarityCoverage(cat_to_map, all_pairs, out_dir);
        } else if (option.equals("sim_rank")) {
            computeSimRankGraphCoverage(cat_to_map, entity_to_cats, anchor_data, out_dir, iterations, damping_factor);
        }
    }

    /**
     * Compute the SimRank scores for our seed entities and entity candidates that belong to the deepest categories
     * in the Wikipedia category taxonomy which are directly associated with our seed entities.
     *
     * @param cats
     * @param entity_cats
     * @param anchor_data
     * @param out_dir
     * @param iterations
     * @param damping_factor
     * @throws IOException
     */
    public static void computeSimRankGraphCoverage(Map<String, CategoryRepresentation> cats,
                                                   Map<String, Set<String>> entity_cats,
                                                   String anchor_data,
                                                   String out_dir, int iterations,
                                                   double damping_factor) throws IOException {

        Map<String, Set<String>> max_level_entities = loadEntitiesDeepestCategory(cats, entity_cats);
        WikiAnchorGraph wg = new WikiAnchorGraph();
        System.out.println("Loading Wikipedia in-degree anchor graph.");
        wg.loadInDegreeAnchorGraph(anchor_data, out_dir);
        wg.readEntityFilterFiles(max_level_entities);
        System.out.println("Loading the filters for which we wanna compute the sim-rank scores.");
        System.out.println("Initializing the feature weights.");
        wg.initialize();
        wg.computeGraphSimRank(damping_factor, iterations);
        wg.writeSimRankScores(out_dir);
    }

    /**
     * Compute the coverage of entity pairs for table alignment if we pick entities that belong to the category
     * directly associated with our seed entity and is the deepest in the Wikipedia category taxonomy.
     *
     * @param cats
     * @param entity_cats
     */
    public static void computeEntityPairTaxonomyLevelCoverage(Map<String, CategoryRepresentation> cats, Map<String, Set<String>> entity_cats, String out_dir) {
        //we perform the experiments only for this subset.
        System.out.println("Measuring coverage for " + gt_pairs.size() + " entities.");

        //for each entity check the additional pairs that are extracted from the same category, and additionally check its coverage.
        StringBuffer sb = new StringBuffer();
        sb.append("entity\tcat_max_level\tall_candidates\tgt_total\toverlap\tunaligned_entities\n");
        Map<String, Set<String>> max_level_entities = loadEntitiesDeepestCategory(cats, entity_cats);
        for (String entity : max_level_entities.keySet()) {
            List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cats.containsKey(cat)).map(cat -> cats.get(cat)).collect(Collectors.toList());
            if (categories == null || categories.isEmpty()) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            //retrieve the categories that belong at the same depth in the category taxonomy.
            int max_level = categories.stream().mapToInt(cat -> cat.level).max().getAsInt();

            Set<String> entity_pairs = max_level_entities.get(entity);
            //compute all the measures.
            int candidate_total = entity_pairs.size();
            int gt_total = gt_pairs.get(entity).size();

            entity_pairs.retainAll(gt_pairs.get(entity));
            int overlapping = entity_pairs.size();
            int additional = candidate_total - overlapping;

            sb.append(entity).append("\t").append(max_level).append("\t").append(candidate_total).append("\t").append(gt_total).append("\t").append(overlapping).append("\t").append(additional).append("\n");
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
    public static void computeCategoryRepSimilarityCoverage(Map<String, CategoryRepresentation> cats, String cat_rep_sim, String out_dir) throws IOException {
        StringBuffer sb = new StringBuffer();

        String line;
        BufferedReader reader = FileUtils.getFileReader(cat_rep_sim);

        Map<String, Set<String>> entity_pairs = new HashMap<>();
        Map<String, Triple<String, String, Double>> entity_cat_rep_score = new HashMap<>();

        int total = 0;
        while ((line = reader.readLine()) != null) {
            try {
                String[] data = line.split("\t");
                total++;
                if (data.length != 5) {
                    continue;
                }

                if (total % 10000 == 0) {
                    System.out.println("Processed " + total);
                }

                String article_a = data[0].intern();
                String article_b = data[1].intern();
                String cat_a = data[2].intern();
                String cat_b = data[3].intern();

                double score = Double.parseDouble(data[4]);

                if (!entity_cat_rep_score.containsKey(article_a)) {
                    entity_cat_rep_score.put(article_a, new ImmutableTriple<>(cat_a, cat_b, score));
                } else if (entity_cat_rep_score.get(article_a).getRight() > score) {
                    entity_cat_rep_score.put(article_a, new ImmutableTriple<>(cat_a, cat_b, score));
                    //we remove these entities as they do not come from a category with the highest similarity
                    entity_pairs.get(article_a).clear();
                }

                if (!entity_pairs.containsKey(article_a)) {
                    entity_pairs.put(article_a, new HashSet<>());
                }
                entity_pairs.get(article_a).add(article_b);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //output the results
        for (String entity : gt_pairs.keySet()) {
            System.out.println("Processing entity " + entity);
            Triple<String, String, Double> category_info = entity_cat_rep_score.get(entity);
            //here we get the level of the category from the candidate articles
            int level = cats.get(category_info.getMiddle()).level;

            //compute all the measures.
            int candidate_total = entity_pairs.get(entity).size();
            int gt_total = gt_pairs.get(entity).size();

            entity_pairs.get(entity).retainAll(gt_pairs.get(entity));
            int overlapping = entity_pairs.size();
            int additional = candidate_total - overlapping;

            sb.append(entity).append("\t").append(level).append("\t").append(candidate_total).append("\t").append(gt_total).append("\t").append(overlapping).append("\t").append(additional).append("\n");
        }

        FileUtils.saveText(sb.toString(), out_dir + "/coverage_cat_rep_sim.tsv");
    }


    /**
     * Get the entities that are associated with the deepest category for a set of seed entities.
     *
     * @param cats
     * @param entity_cats
     * @return
     */
    public static Map<String, Set<String>> loadEntitiesDeepestCategory(Map<String, CategoryRepresentation> cats, Map<String, Set<String>> entity_cats) {
        Map<String, Set<String>> max_level_entities = new HashMap<>();
        for (String entity : gt_pairs.keySet()) {
            //an entity is directly associated to multiple categories.
            Set<String> entity_pairs = new HashSet<>();

            List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cats.containsKey(cat)).map(cat -> cats.get(cat)).collect(Collectors.toList());
            if (categories == null || categories.isEmpty()) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            //retrieve the categories that belong at the same depth in the category taxonomy.
            int max_level = categories.stream().mapToInt(cat -> cat.level).max().getAsInt();

            //add all the entities that belong to the deepest category directly associated with our seed entity
            categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));

            max_level_entities.put(entity, entity_pairs);
        }
        return max_level_entities;
    }
}
