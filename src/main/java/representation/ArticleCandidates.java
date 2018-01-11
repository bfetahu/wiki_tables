package representation;

import datastruct.TableCandidateFeatures;
import io.FileUtils;
import utils.DataUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
            matching.lowest_common_ancestors.add(article_a_cats);
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
                Set<CategoryRepresentation> common_ancestors = findCommonAncestor(cat_a, cat_b);
                if (common_ancestors == null || common_ancestors.isEmpty()) {
                    continue;
                }
                matching.lowest_common_ancestors.add(common_ancestors.stream().map(x -> x.label).collect(Collectors.toSet()));
            }
        }
        if (matching.lowest_common_ancestors.isEmpty()) {
            return null;
        }
        return matching;
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
        parents_a.remove("root");

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
        if (cat == null || cat.parents == null) {
            return;
        }
        cat.parents.keySet().forEach(parent_label -> {
            if (parents.contains(parent_label)) {
                return;
            }
            parents.add(parent_label);

            gatherParents(cat.parents.get(parent_label), parents);
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String cat_rep_path = "", out_dir = "", article_cats = "", category_path = "", option = "";
        Set<String> seed_entities = new HashSet<>();

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cat_rep")) {
                cat_rep_path = args[++i];
            } else if (args[i].equals("-category_path")) {
                category_path = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-article_categories")) {
                article_cats = args[++i];
            } else if (args[i].equals("-seed_entities")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-option")) {
                option = args[++i];
            }
        }

        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(category_path);
        ArticleCandidates ac = new ArticleCandidates(cat);
        if (option.equals("candidates")) {
            ac.generateCandidates(seed_entities, out_dir, article_cats);
        } else if (option.equals("scoring")) {

        }
    }

    /**
     * Score the generated article candidates for table alignment, based on their similarity on the category representations,
     * distance to the lowest common ancestor between the two categories.
     *
     * @param cat_rep_path
     * @param out_dir
     */
    public void scoreTableCandidates(String cat_rep_path, String out_dir) {
        Map<String, Map<String, Map<String, Integer>>> cat_reps = (Map<String, Map<String, Map<String, Integer>>>) FileUtils.readObject(cat_rep_path);
        cat_to_map.keySet().forEach(cat_label -> cat_to_map.get(cat_label).cat_representation = cat_reps.get(cat_label));

        //take the immediate children from the root category to read the article candidates.
        List<Map.Entry<Integer, String>> child_cats = root_category.children.values().stream().map(cat -> new AbstractMap.SimpleEntry<>(cat.node_id, cat.label)).collect(Collectors.toList());

        for (Map.Entry<Integer, String> cat_entry : child_cats) {
            String out_file = out_dir + "/" + cat_entry.getKey() + ".gz";
            List<TableCandidateFeatures> candidates = (List<TableCandidateFeatures>) FileUtils.readObject(out_file);

            for (TableCandidateFeatures candidate : candidates) {

            }
        }
    }

    /**
     * Generate the article candidates for table alignment.
     *
     * @param seed_entities
     * @param out_dir
     * @param article_cats
     * @throws IOException
     * @throws InterruptedException
     */
    public void generateCandidates(Set<String> seed_entities, String out_dir, String article_cats) throws IOException, InterruptedException {
        //set the entity categories for the articles.
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, seed_entities);
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        //set num entities for each category.
        DataUtils.updateCatsWithEntities(root_category, cats_entities);

        /*
            we consider candidates only from the entities which are associated to categories
            that are children of the root category.
        */
        Set<String> finished_entities = FileUtils.readIntoSet("finished.log", "\n", false);
        AtomicInteger atm = new AtomicInteger();
        root_category.children.keySet().parallelStream().forEach(child_label -> {
            CategoryRepresentation child = root_category.children.get(child_label);
            int file_id = atm.incrementAndGet();
            String out_file = out_dir + "/" + file_id;

            if (child.entities.size() <= 1 || finished_entities.contains(out_file)) {
                return;
            }
            System.out.printf("Starting generating table matching candidate %s representations with %d articles.\n", child_label, child.entities.size());
            int total = constructCandidateRepresentations(child.entities, entity_cats, out_file, child);
            FileUtils.saveText(out_file + "\n", "finished.log", true);

            try {
                Process p = Runtime.getRuntime().exec("gzip " + out_file);
                p.waitFor();
                p.destroy();
            } catch (Exception e) {
                System.out.println("Error at category " + child_label + " with message " + e.getMessage());
            }
            System.out.printf("Finished generating table matching candidate %s representations for %d articles.\n", child_label, total);
        });
    }

    /**
     * Generate the  set of article candidates for which we will consider the tables for the alignment process.
     *
     * @param entities
     * @param entity_categories
     * @return
     */
    public int constructCandidateRepresentations(Set<String> entities, Map<String, Set<String>> entity_categories, String out_file, CategoryRepresentation cat) {
        String[] entities_arr = new String[entities.size()];
        entities.toArray(entities_arr);

        int total = 0;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < entities_arr.length; i++) {
            String article_candidate_a = entities_arr[i];
            for (int j = i + 1; j < entities_arr.length; j++) {
                String article_candidate_b = entities_arr[j];

                TableCandidateFeatures tbl_candidate = measureArticleCandidateScore(article_candidate_a, article_candidate_b, entity_categories);
                if (tbl_candidate == null) {
                    continue;
                }

                total++;
                sb.append(tbl_candidate.printCandidates(cat)).append("\n");
                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
        return total;
    }
}
