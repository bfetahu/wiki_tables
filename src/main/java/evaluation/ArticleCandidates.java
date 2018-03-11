package evaluation;

import datastruct.TableCandidateFeatures;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Compute the relatedness between Wikipedia articles based on their category representation and other features we consider
 * in our approach for generating candidates.
 * Created by besnik on 12/6/17.
 */
public class ArticleCandidates {
    private CategoryRepresentation root_category;
    private Map<String, CategoryRepresentation> cat_to_map;
    public static Set<String> seed_entities = new HashSet<>();
    public static Set<String> filter_entities = new HashSet<>();
    public static Map<String, Set<String>> gt_entities = new HashMap<>();

    public ArticleCandidates(CategoryRepresentation root_category) {
        cat_to_map = new HashMap<>();
        this.root_category = root_category;
        this.root_category.loadIntoMapChildCats(cat_to_map);
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        String cat_rep_path = "", out_dir = "", article_cats = "", category_path = "", option = "", table_data = "";

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
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-option")) {
                option = args[++i];
            } else if (args[i].equals("-gt_pairs")) {
                gt_entities = FileUtils.readMapSet(args[++i], "\t");
            }
        }

        CategoryRepresentation cat = null;
        if (!cat_rep_path.isEmpty()) {
            cat = (CategoryRepresentation) FileUtils.readObject(cat_rep_path);
        } else {
            cat = CategoryRepresentation.readCategoryGraph(category_path);
        }

        ArticleCandidates ac = new ArticleCandidates(cat);
        if (option.equals("candidates")) {
            ac.generateCandidates(out_dir, article_cats);
        }
    }

    /**
     * Generates the feature representation for each of our ground-truth articles based on our approach which consists
     * of several features.
     *
     * @param out_dir
     * @param article_categories
     */
    public void scoreTableCandidatesApproach(String out_dir, String article_categories) {
        for (String entity : seed_entities) {
            for (String entity_candidate : filter_entities) {
                //create for each of these pairs the features
                boolean label = gt_entities.containsKey(entity) && gt_entities.get(entity).contains(entity_candidate);

                //add all the category representation similarities


                //add all the table column similarities


                //add all the node2vec similarities for the instances in the table
            }
        }
    }


    /**
     * Generate the article candidates for table alignment.
     *
     * @param out_dir
     * @param article_cats
     * @throws IOException
     * @throws InterruptedException
     */
    public void generateCandidates(String out_dir, String article_cats) throws IOException, InterruptedException {
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

                TableCandidateFeatures tbl_candidate = TableCandidateFeatures.measureArticleCandidateScore(article_candidate_a, article_candidate_b, entity_categories, cat_to_map);
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
