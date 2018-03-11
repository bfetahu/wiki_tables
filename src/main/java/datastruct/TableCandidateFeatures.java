package datastruct;

import gnu.trove.map.hash.TIntDoubleHashMap;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by besnik on 12/8/17.
 */
public class TableCandidateFeatures implements Serializable {
    private String article_a;
    private String article_b;

    private int max_level_a;
    private int max_level_b;

    private Set<String> article_categories_a;
    private Set<String> article_categories_b;

    public Set<String> lowest_common_ancestors;

    public TableCandidateFeatures(String article_a, String article_b) {
        lowest_common_ancestors = new HashSet<>();
        this.article_a = article_a;
        this.article_b = article_b;
    }

    /**
     * Set the categories that are directly associated with the first candidate article.
     * <p>
     * Additionally, set the maximum or deepest level of the category associated with the article
     * based on the Wikipedia category hierarchy.
     *
     * @param article_categories_a
     * @param cat_to_map
     */
    public void setArticleACategories(Set<String> article_categories_a, Map<String, CategoryRepresentation> cat_to_map) {
        this.article_categories_a = article_categories_a;

        max_level_a = article_categories_a.stream().mapToInt(cat -> cat_to_map.containsKey(cat) ? cat_to_map.get(cat).level : 0).max().getAsInt();
    }

    /**
     * Set the categories that are directly associated with the second candidate article.
     * <p>
     * Additionally, set the maximum or deepest level of the category associated with the article
     * based on the Wikipedia category hierarchy.
     *
     * @param article_categories_b
     * @param cat_to_map
     */
    public void setArticleBCategories(Set<String> article_categories_b, Map<String, CategoryRepresentation> cat_to_map) {
        this.article_categories_b = article_categories_b;
        max_level_b = article_categories_b.stream().mapToInt(cat -> cat_to_map.containsKey(cat) ? cat_to_map.get(cat).level : 0).max().getAsInt();
    }

    /**
     * Get the directly associated categories with the fist candidate article.
     *
     * @return
     */
    public Set<String> getArticleACategories() {
        return article_categories_a;
    }

    /**
     * Get the directly associated categories with the second candidate article.
     *
     * @return
     */
    public Set<String> getArticleBCategories() {
        return article_categories_b;
    }

    /**
     * Get the first article as a candidate for the table alignment.
     *
     * @return
     */
    public String getArticleA() {
        return article_a;
    }

    /**
     * Get the second article as a candidate for the table alignment.
     *
     * @return
     */
    public String getArticleB() {
        return article_b;
    }

    /**
     * Return the deepest category associated directly to the article from the Wikipedia category hierarchy.
     *
     * @return
     */
    public int getMaxLevelA() {
        return max_level_a;
    }

    /**
     * Return the deepest category associated directly to the article from the Wikipedia category hierarchy.
     *
     * @return
     */
    public int getMaxLevelB() {
        return max_level_b;
    }


    /**
     * Compute the category similarity between categories that are directly associated with the articles.
     * <p>
     * The similarity is computed between the attributes associated with a specific category and the wieght is computed
     * as following. For a category c, and attribute p the weight is :
     * weight(p, c) = \lambda_c / \max\lambda_c_j * \frac{|\cup \langle p, o\rangle|}{|\langle p, o\rangle|}
     * where \lambda_c is the level of category c, and \max_lambda_c_j is the maximum length (farthest to the root) in
     * which the property p appears, whereas the fraction consists of the number of unique instantiations of p in c, and
     * the total number of instantiations of p in c.
     *
     * @param cats
     */
    public double[] computeLCACategoryRepresentationSim(Map<String, CategoryRepresentation> cats, Map<String, TIntDoubleHashMap> cat_weights) {
        //compute the similarity of the directly related categories with both Wikipedia articles to their LCA categories.
        List<Double> a_score = new ArrayList<>();
        List<Double> b_score = new ArrayList<>();


        for (String lca_category : lowest_common_ancestors) {
            if (!cats.containsKey(lca_category)) {
                continue;
            }
            TIntDoubleHashMap lca_cat_rep_weight = cat_weights.get(lca_category);

            //compute the distance of article_b to the LCA category
            for (String cat_a : article_categories_a) {
                if (!cats.containsKey(cat_a)) {
                    continue;
                }
                TIntDoubleHashMap cat_a_weight = cat_weights.get(cat_a);
                double euclidean_sim_a = DataUtils.computeEuclideanDistance(cat_a_weight, lca_cat_rep_weight);
                a_score.add(euclidean_sim_a);
            }

            //compute the distance of article_b to the LCA category
            for (String cat_b : article_categories_b) {
                if (!cats.containsKey(cat_b)) {
                    continue;
                }
                TIntDoubleHashMap cat_b_weight = cat_weights.get(cat_b);
                double euclidean_sim_b = DataUtils.computeEuclideanDistance(cat_b_weight, lca_cat_rep_weight);
                b_score.add(euclidean_sim_b);
            }
        }
        double min_a_score = a_score.stream().mapToDouble(x -> x).min().getAsDouble();
        double max_a_score = a_score.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean_a_score = a_score.stream().mapToDouble(x -> x).average().getAsDouble();

        double min_b_score = b_score.stream().mapToDouble(x -> x).min().getAsDouble();
        double max_b_score = b_score.stream().mapToDouble(x -> x).max().getAsDouble();
        double mean_b_score = b_score.stream().mapToDouble(x -> x).average().getAsDouble();

        double[] scores = new double[]{Math.abs((min_a_score - min_b_score) / 2), Math.abs((max_a_score - max_b_score) / 2), Math.abs((mean_a_score - mean_b_score) / 2)};
        return scores;
    }


    /**
     * For a category print its entity candidates and their corresponding lowest-common-ancestors.
     *
     * @param cat
     * @return
     */
    public String printCandidates(CategoryRepresentation cat) {
        StringBuffer sb = new StringBuffer();
        sb.append(cat.label).append("\t").append(article_a).append("\t").append(article_b);

        for (String ancestors : lowest_common_ancestors) {
            sb.append("\t").append(ancestors);
        }

        return sb.toString();
    }


    /**
     * For two articles, based on their representation we check if they are suitable to become candidate pairs, for
     * analyzing if the corresponding tables should be aligned.
     *
     * @param article_a
     * @param article_b
     */
    public static TableCandidateFeatures measureArticleCandidateScore(String article_a, String article_b, Map<String, Set<String>> article_categories, Map<String, CategoryRepresentation> cat_to_map) {
        Set<String> article_a_cats = article_categories.get(article_a);
        Set<String> article_b_cats = article_categories.get(article_b);

        //check first if they come from the same categories.
        if (article_a_cats == null || article_b_cats == null) {
            //return null in this case, indicating that the articles belong to exactly the same categories
            return null;
        }
        boolean same_cats = article_a_cats.equals(article_b_cats);

        /*
             Else, we first find the lowest common ancestor between the categories of the two articles.
             Additionally, we will do this only for the categories being in the deepest category hierarchy graph,
             this allows us to avoid the match between very broad categories, and thus, we generate more
             meaningful similarity matches.
         */
        TableCandidateFeatures matching = new TableCandidateFeatures(article_a, article_b);
        matching.setArticleACategories(article_a_cats, cat_to_map);
        matching.setArticleBCategories(article_b_cats, cat_to_map);

        if (same_cats) {
            matching.lowest_common_ancestors.addAll(article_a_cats);
            return matching;
        }

        for (String cat_a_label : article_a_cats) {
            CategoryRepresentation cat_a = cat_to_map.get(cat_a_label);
            if (cat_a == null || cat_a.level < matching.getMaxLevelA()) {
                continue;
            }

            for (String cat_b_label : article_b_cats) {
                CategoryRepresentation cat_b = cat_to_map.get(cat_b_label);
                if (cat_b == null || cat_b.level < matching.getMaxLevelB()) {
                    continue;
                }

                //get the lowest common ancestors between the two categories
                Set<CategoryRepresentation> common_ancestors = DataUtils.findCommonAncestor(cat_a, cat_b, cat_to_map);
                if (common_ancestors == null || common_ancestors.isEmpty()) {
                    continue;
                }
                matching.lowest_common_ancestors.addAll(common_ancestors.stream().map(x -> x.label).collect(Collectors.toSet()));
            }
        }
        if (matching.lowest_common_ancestors.isEmpty()) {
            return null;
        }
        return matching;
    }


    /**
     * Compute the category representation similarity between the categories of two articles against their LCA category
     * and against the directly associated categories.
     *
     * @param seed_entities
     * @param all_entities
     * @param cat_to_map
     * @param entity_cats
     * @return
     */
    public static Map<String, Map<String, Triple<Double, Double, Double>>> computeLCAEntityCandidatePairScores(Set<String> seed_entities, Set<String> all_entities, Map<String, CategoryRepresentation> cat_to_map, Map<String, Set<String>> entity_cats) {
        Map<String, Map<String, Triple<Double, Double, Double>>> entity_lca_sim = new HashMap<>();
        //compute the category weights
        Map<String, Double> attribute_max_level_category = DataUtils.computeMaxLevelAttributeCategory(cat_to_map);
        Map<String, TIntDoubleHashMap> cat_weights = DataUtils.computeCategoryAttributeWeights(attribute_max_level_category, cat_to_map);

        //consider all the pairs for the given seed entities with all the remaining filter entities.
        for (String entity_a : seed_entities) {
            Map<String, Triple<Double, Double, Double>> sub_entity_lca_sim = new HashMap<>();
            all_entities.parallelStream().forEach(entity_b -> {
                TableCandidateFeatures tc = TableCandidateFeatures.measureArticleCandidateScore(entity_a, entity_b, entity_cats, cat_to_map);
                if (tc == null) {
                    sub_entity_lca_sim.put(entity_b, new ImmutableTriple<>(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE));
                } else {
                    double[] cat_sim_lca = tc.computeLCACategoryRepresentationSim(cat_to_map, cat_weights);
                    sub_entity_lca_sim.put(entity_b, new ImmutableTriple<>(cat_sim_lca[0], cat_sim_lca[1], cat_sim_lca[2]));
                }
            });
            entity_lca_sim.put(entity_a, sub_entity_lca_sim);
        }

        return entity_lca_sim;
    }
}
