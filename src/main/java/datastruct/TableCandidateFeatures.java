package datastruct;

import representation.CategoryRepresentation;

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

    public List<Set<CategoryRepresentation>> lowest_common_ancestors;

    public TableCandidateFeatures(String article_a, String article_b) {
        lowest_common_ancestors = new ArrayList<>();
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
     * Compute the overlap between categories that are directly associated with the articles.
     *
     * @param cats
     */
    public void computeDirectCatRepSimilarity(Map<String, CategoryRepresentation> cats) {
        Set<CategoryRepresentation> sub_cats_a = cats.values().stream().filter(cat -> article_categories_a.contains(cat.label) && cat.level == max_level_a).collect(Collectors.toSet());
        Set<CategoryRepresentation> sub_cats_b = cats.values().stream().filter(cat -> article_categories_b.contains(cat.label) && cat.level == max_level_b).collect(Collectors.toSet());

        Map<String, Map<String, Integer>> rep_a = new HashMap<>();

    }

    /**
     * Aggregate the category representation for a group of children categories.
     *
     * @param rep
     * @param sub_cats
     * @return
     */
    public Map<String, Map<String, Integer>> updateCategoryRepresentation(Map<String, Map<String, Integer>> rep, Set<CategoryRepresentation> sub_cats) {
        Map<String, Map<String, Integer>> rep_a = new HashMap<>();
        sub_cats.forEach(cat ->
                rep.keySet().forEach(key -> {
                    if (!rep_a.containsKey(key)) {
                        rep_a.put(key, rep.get(key));
                        return;
                    }
                    Map<String, Integer> sub_rep_a = rep_a.get(key);
                    Map<String, Integer> sub_rep = rep.get(key);
                    sub_rep.keySet().forEach(val -> {
                        if (!sub_rep_a.containsKey(val)) {
                            sub_rep_a.put(val, sub_rep.get(val));
                            return;
                        }
                        sub_rep_a.put(val, sub_rep.get(val) + sub_rep_a.get(val));
                    });
                })
        );
        return rep_a;
    }
}
