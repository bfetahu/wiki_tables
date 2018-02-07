package datastruct;

import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    public void computeCategoryRepresentationSim(Map<String, CategoryRepresentation> cats, Map<String, TIntDoubleHashMap> cat_weights, String out_dir) {
        StringBuffer sb_out = new StringBuffer();
        StringBuffer sb_out_lca = new StringBuffer();
        for (String cat_a_label : article_categories_a) {
            CategoryRepresentation cat_a = cats.get(cat_a_label);
            if (!cat_weights.containsKey(cat_a_label)) {
                continue;
            }
            TIntDoubleHashMap cat_a_rep_weights = cat_weights.get(cat_a_label);
            for (String cat_b_label : article_categories_b) {
                if (!cat_weights.containsKey(cat_b_label)) {
                    continue;
                }
                CategoryRepresentation cat_b = cats.get(cat_b_label);
                if (!cat_a_label.equals(cat_b_label)) {
                    TIntDoubleHashMap cat_b_rep_weights = cat_weights.get(cat_a_label);
                    double euclidean_sim = DataUtils.computeEuclideanDistance(cat_a_rep_weights, cat_b_rep_weights);

                    sb_out.append(article_a).append("\t").append(article_b).append("\t").
                            append(cat_a.level).append("\t").append(cat_a.label).append("\t").
                            append(cat_b.level).append("\t").append(cat_b.label).append("\t").
                            append(euclidean_sim).append("\n");

                    //compute the similarity of the directly connected categories with the LCA categories too.
                    computeLCACategoryRepresentationSim(cat_a, cat_b, cat_a_rep_weights, cat_b_rep_weights, cats, cat_weights, sb_out_lca);
                }
            }
        }

        FileUtils.saveText(sb_out.toString(), out_dir + "/dca_article_candidate_cat_sim.tsv", true);
        FileUtils.saveText(sb_out_lca.toString(), out_dir + "/lca_article_candidate_cat_sim.tsv", true);
    }

    /**
     * Compute the similarity between the directly associated categories to the Wikipedia articles, and their
     * lowest-common-ancestor categories.
     *
     * @param cat_a
     * @param cat_b
     * @param cat_a_weights
     * @param cat_b_weights
     * @param cats
     * @param cat_weights
     * @param sb
     */
    public void computeLCACategoryRepresentationSim(CategoryRepresentation cat_a, CategoryRepresentation cat_b,
                                                    TIntDoubleHashMap cat_a_weights, TIntDoubleHashMap cat_b_weights,
                                                    Map<String, CategoryRepresentation> cats, Map<String, TIntDoubleHashMap> cat_weights,
                                                    StringBuffer sb) {
        //compute the similarity of the directly related categories with both Wikipedia articles to their LCA categories.
        for (String lca_category : lowest_common_ancestors) {
            if (!cats.containsKey(lca_category)) {
                continue;
            }
            CategoryRepresentation lca_cat = cats.get(lca_category);
            TIntDoubleHashMap lca_cat_rep_weights = cat_weights.get(lca_category);

            double euclidean_sim_a = DataUtils.computeEuclideanDistance(cat_a_weights, lca_cat_rep_weights);
            double euclidean_sim_b = DataUtils.computeEuclideanDistance(cat_b_weights, lca_cat_rep_weights);

            sb.append(article_a).append("\t").append(article_b).append("\t").
                    append(cat_a.level).append("\t").append(cat_a.label).append("\t").
                    append(cat_b.level).append("\t").append(cat_b.label).append("\t").
                    append(lca_cat.level).append("\t").append(lca_cat.label).append("\t").
                    append(euclidean_sim_a).append("\t").append(euclidean_sim_b).append("\n");
        }
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
}
