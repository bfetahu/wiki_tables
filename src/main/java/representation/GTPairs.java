package representation;

import datastruct.TableCandidateFeatures;
import io.FileUtils;
import org.json.JSONObject;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 12/24/17.
 */
public class GTPairs {
    public static void main(String[] args) throws IOException {
        Set<String> seed_entities = new HashSet<>();
        String category_path = "", article_cats = "", out_file = "", option = "", table_data = "", candidates = "", complementary_candidates = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-seeds")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-categories")) {
                category_path = args[++i];
            } else if (args[i].equals("-article_categories")) {
                article_cats = args[++i];
            } else if (args[i].equals("-out_file")) {
                out_file = args[++i];
            } else if (args[i].equals("-option")) {
                option = args[++i];
            } else if (args[i].equals("-table_data")) {
                table_data = args[++i];
            } else if (args[i].equals("-candidates")) {
                candidates = args[++i];
            } else if (args[i].equals("-complementary_candidates")) {
                complementary_candidates = args[++i];
            }
        }

        if (option.equals("pairs")) {
            generateGTPairs(seed_entities, category_path, article_cats, out_file);
        } else if (option.equals("evaluation")) {
            constructGTTableData(seed_entities, out_file, table_data, candidates, complementary_candidates);
        }
    }

    /**
     * Construct the ground truth data for the evaluation of the sampled entities.
     *
     * @param seed_entities
     * @param out_dir
     * @param table_data
     * @param candidates
     */
    private static void constructGTTableData(Set<String> seed_entities, String out_dir, String table_data, String candidates, String complementary_candidates) throws IOException {
        Set<String> candidate_articles = loadArticleCandidates(out_dir, candidates, seed_entities);
        candidate_articles.addAll(FileUtils.readIntoSet(complementary_candidates, "\n", false));

        Map<String, Set<String>> candidate_pairs = new HashMap<>();
        Set<String> all_entities = new HashSet<>();
        candidate_articles.forEach(line -> {
            String tmp[] = line.split("\t");
            String article_a = tmp[1].trim();
            String article_b = tmp[2].trim();
            if (!candidate_pairs.containsKey(article_a)) {
                candidate_pairs.put(article_a, new HashSet<>());
            }
            candidate_pairs.get(article_a).add(article_b);
            all_entities.add(article_a);
            all_entities.add(article_b);
        });

        Map<String, String> seed_table_json = loadArticleTables(all_entities, table_data, out_dir);
        FileUtils.checkDir(out_dir + "/gt_candidates/");
        for (String article_a : candidate_pairs.keySet()) {
            StringBuffer sb = new StringBuffer();
            String out_file = out_dir + "/gt_candidates/" + article_a;

            String tbl_a = seed_table_json.get(article_a);
            sb.append(tbl_a).append("\n");
            for (String article_b : candidate_pairs.get(article_a)) {
                String tbl_b = seed_table_json.get(article_b);
                if (tbl_b == null) {
                    System.out.printf("Missing table data for entity_b %s.\n", article_b);
                    continue;
                }
                sb.append(tbl_b).append("\n");

                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), out_file, true);
        }

    }

    /**
     * Load the table data for all the articles that are coming from the sample seed set and that are considered for
     * alignment.
     *
     * @param seed_entities
     * @param table_data
     * @return
     */
    private static Map<String, String> loadArticleTables(Set<String> seed_entities, String table_data, String out_dir) throws IOException {
        Map<String, String> seed_table_json = new HashMap<>();
        String out_file = out_dir + "/article_candidate_tables.json";
        String line;

        if (FileUtils.fileExists(out_file, false)) {
            BufferedReader reader = FileUtils.getFileReader(out_file);
            while ((line = reader.readLine()) != null) {
                JSONObject json = new JSONObject(line);
                String entity = json.getString("entity");
                seed_table_json.put(entity, line);
            }

            return seed_table_json;
        }

        BufferedReader reader = FileUtils.getFileReader(table_data);
        StringBuffer sb = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            try {
                JSONObject json = new JSONObject(line);
                String entity = json.getString("entity");

                if (!seed_entities.contains(entity)) {
                    continue;
                }
                seed_table_json.put(entity, line);
                sb.append(line).append("\n");

                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            } catch (Exception e) {
                System.out.printf("Error processing table data for line \t\t%s.\n", line);
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);

        return seed_table_json;
    }

    /**
     * Load the candidate articles for a set of seed entities.
     *
     * @param out_dir
     * @param candidates
     * @param seed_entities
     * @return
     */
    private static Set<String> loadArticleCandidates(String out_dir, String candidates, Set<String> seed_entities) {
        String out_file = out_dir + "/sample_article_candidates.tsv";
        if (FileUtils.fileExists(out_file, false)) {
            return FileUtils.readIntoSet(out_file, "\n", false);
        }

        Set<String> candidate_list = new HashSet<>();
        //first read all the candidates for the sampled entities
        Set<String> files = new HashSet<>();
        FileUtils.getFilesList(candidates, files);

        files.parallelStream().forEach(file -> {
            BufferedReader reader = FileUtils.getFileReader(file);
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    String[] tmp = line.split("\t");
                    if (tmp.length < 3) {
                        continue;
                    }
                    String article_a = tmp[2].intern();
                    if (!seed_entities.contains(article_a)) {
                        continue;
                    }
                    candidate_list.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        StringBuffer sb = new StringBuffer();
        for (String line : candidate_list) {
            sb.append(line).append("\n");

            if (sb.length() > 100000) {
                FileUtils.saveText(sb.toString(), out_file, true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
        return candidate_list;
    }

    /**
     * Generate all possible GT pairs between the sample entities and the article candidates.
     *
     * @param seed_entities
     * @param category_path
     * @param article_cats
     * @param out_file
     * @throws IOException
     */
    private static void generateGTPairs(Set<String> seed_entities, String category_path, String article_cats, String out_file) throws IOException {
        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(category_path);
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, seed_entities);
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        ArticleCandidates ac = new ArticleCandidates(cat);
        StringBuffer sb = new StringBuffer();
        //set num entities for each category.
        DataUtils.updateCatsWithEntities(cat, cats_entities);
        for (String sample_entity : seed_entities) {
            System.out.printf("Processing entity %s.\n", sample_entity);
            int total = 0;
            for (String child_label : cat.children.keySet()) {
                CategoryRepresentation child = cat.children.get(child_label);
                if (child.entities.contains(sample_entity)) {
                    continue;
                }

                //we generate all possible pairs between the sampled entity and other entities that do not fall into the same category
                for (String entity : child.entities) {
                    TableCandidateFeatures tbl_candidate = ac.measureArticleCandidateScore(sample_entity, entity, entity_cats);
                    if (tbl_candidate == null) {
                        continue;
                    }

                    sb.append(tbl_candidate.printCandidates(child)).append("\n");
                    if (sb.length() > 100000) {
                        FileUtils.saveText(sb.toString(), out_file, true);
                        sb.delete(0, sb.length());
                    }
                    total++;
                }
            }
            System.out.printf("Finished processing entity %s with %d candidates.\n", sample_entity, total);
        }
    }
}