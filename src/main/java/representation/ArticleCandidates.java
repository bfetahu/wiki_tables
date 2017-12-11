package representation;

import datastruct.TableCandidateFeatures;
import utils.DataUtils;
import utils.FileUtils;

import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 12/6/17.
 */
public class ArticleCandidates {
    private CategoryRepresentation root_category;
    private Map<String, CategoryRepresentation> cat_to_map;

    public ArticleCandidates(CategoryRepresentation root_category) {
        cat_to_map = new HashMap<>();
        this.root_category = root_category;
        this.root_category.loadIntoMapChildCats(cat_to_map);
    }


    /**
     * For two articles, based on their representation we check if they are suitable to become candidate pairs, for
     * analyzing if the corresponding tables should be aligned.
     *
     * @param article_a
     * @param article_b
     */
    public TableCandidateFeatures measureArticleCandidateScore(String article_a, String article_b, Map<String, Set<String>> article_categories) {
        Set<String> article_a_cats = article_categories.get(article_a);
        Set<String> article_b_cats = article_categories.get(article_b);

        //check first if they come from the same categories.
        if ((article_a_cats == null || article_b_cats == null) || article_a_cats.equals(article_b_cats)) {
            //return null in this case, indicating that the articles belong to exactly the same categories
            return null;
        }

        /*
             Else, we first find the lowest common ancestor between the categories of the two articles.
             Additionally, we will do this only for the categories being in the deepest category hierarchy graph,
             this allows us to avoid the match between very broad categories, and thus, we generate more
             meaningful similarity matches.
         */

        TableCandidateFeatures matchings = new TableCandidateFeatures(article_a, article_b);
        matchings.setArticleACategories(article_a_cats, cat_to_map);
        matchings.setArticleACategories(article_b_cats, cat_to_map);

        for (String cat_a_label : article_a_cats) {
            CategoryRepresentation cat_a = cat_to_map.get(cat_a_label);
            if (cat_a.level < matchings.getMaxLevelA()) {
                continue;
            }

            for (String cat_b_label : article_b_cats) {
                CategoryRepresentation cat_b = cat_to_map.get(cat_b_label);
                if (cat_b.level < matchings.getMaxLevelB()) {
                    continue;
                }

                //get the lowest common ancestors between the two categories
                Set<CategoryRepresentation> common_ancestors = findCommonAncestor(cat_a, cat_b);
                matchings.lowest_common_ancestors.add(common_ancestors);
            }
        }
        return matchings;
    }

    /**
     * Find the lowest common ancestor for the two categories under consideration.
     *
     * @param cat_a
     * @param cat_b
     * @return
     */
    public Set<CategoryRepresentation> findCommonAncestor(CategoryRepresentation cat_a, CategoryRepresentation cat_b) {
        Set<CategoryRepresentation> ancestors = new HashSet<>();

        Set<String> parents_a = new HashSet<>();
        Set<String> parents_b = new HashSet<>();

        gatherParents(cat_a, parents_a);
        gatherParents(cat_b, parents_b);

        //take the intersection of the two parent sets.
        parents_a.retainAll(parents_b);

        if (parents_a.isEmpty()) {
            return null;
        }

        int max_level = parents_a.stream().mapToInt(parent -> cat_to_map.get(parent).level).max().getAsInt();

        for (String parent : parents_a) {
            CategoryRepresentation cat = cat_to_map.get(parent);
            if (cat.level == max_level) {
                ancestors.add(cat);
            }
        }

        return ancestors;
    }

    /**
     * Gather all the parents of category up to the root of the category.
     *
     * @param cat
     * @param parents
     */
    public void gatherParents(CategoryRepresentation cat, Set<String> parents) {
        cat.parents.keySet().forEach(parent_label -> {
            parents.add(parent_label);

            gatherParents(cat.parents.get(parent_label), parents);
        });
    }

    public static void main(String[] args) throws IOException {
        String[] args1 = {"-cat_rep", "/Users/besnik/Desktop/wiki_tables/category_hierarchy_representation.obj",
                "-article_categories", "/Users/besnik/Desktop/wiki_tables/article_categories_en.ttl.bz2",
                "-seed_entities", "/Users/besnik/Desktop/wiki_tables/seed_entities.txt", "-out_dir", "/Users/besnik/Desktop/wiki_tables/candidates/"};
        args = args1;
        String cat_rep = "", out_dir = "", article_cats = "";
        Set<String> seed_entities = new HashSet<>();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cat_rep")) {
                cat_rep = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-article_categories")) {
                article_cats = args[++i];
            } else if (args[i].equals("-seed_entities")) {
//                seed_entities = DataUtils.loadSeedEntities(args[++i]);
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
//                StringBuffer sb = new StringBuffer();
//                seed_entities.forEach(s -> sb.append(s).append("\n"));
//                FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/wiki_tables/seed_entities.txt");
            }
        }
        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(cat_rep);
        ArticleCandidates ac = new ArticleCandidates(cat);

        //set the entity categories for the articles.
        Map<String, Set<String>> entity_categories = DataUtils.readCategoryMappingsWiki(article_cats);
        entity_categories.keySet().retainAll(seed_entities);

        //set num entities for each category.
        Map<String, CategoryRepresentation> cats = DataUtils.updateCatsWithEntities(cat, entity_categories);

        ac.cat_to_map = cats;
        ac.root_category = cat;

        /*
            we consider candidates only from the entities which are associated to categories
            that are children of the root category.
        */
        final String out_dir_f = out_dir;
        cat.children.keySet().parallelStream().forEach(child_label -> {
            CategoryRepresentation child = cat.children.get(child_label);
            if (child.entities.size() <= 1) {
                System.out.printf("Skipping category %s due to the fact that it contains only 1 entity.\n", child.label);
                return;
            }

            List<TableCandidateFeatures> candidates = ac.constructCandidateRepresentations(child.entities, entity_categories, cats);
            String out_file = out_dir_f + "/" + child.node_id;
            FileUtils.saveObject(candidates, out_file);
        });
    }

    /**
     * Generate the  set of article candidates for which we will consider the tables for the alignment process.
     *
     * @param entities
     * @param entity_categories
     * @param cats
     * @return
     */
    public List<TableCandidateFeatures> constructCandidateRepresentations(Set<String> entities, Map<String, Set<String>> entity_categories, Map<String, CategoryRepresentation> cats) {
        List<TableCandidateFeatures> candidates = new ArrayList<>();
        String[] entities_arr = new String[entities.size()];
        entities.toArray(entities_arr);

        for (int i = 0; i < entities_arr.length; i++) {
            String article_candidate_a = entities_arr[i];
            for (int j = i + 1; j < entities_arr.length; j++) {
                String article_candidate_b = entities_arr[j];

                TableCandidateFeatures tbl_candidate = measureArticleCandidateScore(article_candidate_a, article_candidate_b, entity_categories);
                if (tbl_candidate == null) {
                    continue;
                }
                candidates.add(tbl_candidate);
            }
        }

        System.out.printf("Finished generating table matching candidate representations for %d articles.\n", candidates.size());
        return candidates;
    }
}
