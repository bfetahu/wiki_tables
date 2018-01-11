package representation;

import datastruct.TableCandidateFeatures;
import io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 12/24/17.
 */
public class GTPairs {
    public static void main(String[] args) throws IOException {
        Set<String> seed_entities = new HashSet<>();
        String category_path = "", article_cats = "", out_file = "", option = "",
                table_data = "", candidates = "",
                wiki_summaries = "", entity_filter = "";
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
            } else if (args[i].equals("-wiki_summaries")) {
                wiki_summaries = args[++i];
            } else if (args[i].equals("-entity_filter")) {
                entity_filter = args[++i];
            }
        }

        if (option.equals("pairs")) {
            generateGTPairs(seed_entities, category_path, article_cats, out_file);
        } else if (option.equals("evaluation")) {
            constructGTTableData(out_file, table_data, candidates);
        } else if (option.equals("clean_pairs")) {
            cleanGTPairs(candidates, out_file, wiki_summaries, entity_filter);
        }
    }

    /**
     * Filter out candidates for the entity pairs which do not match the source article based on its attributes.
     * For example, we do not consider as valid pairs the tables between an entity of type actor and another
     * entity e.g. Software, with which the source has nothing to do.
     *
     * @param candidates
     * @param out_dir
     * @param wiki_summaries
     * @param entity_filter_path
     * @throws IOException
     */
    public static void cleanGTPairs(String candidates, String out_dir, String wiki_summaries, String entity_filter_path) throws IOException {
        Map<String, List<String>> entity_filters = new HashMap<>();
        String[] lines = FileUtils.readText(entity_filter_path).split("\n");
        for (String line : lines) {
            String[] tmp = line.split("\t");
            String[] filters = tmp[1].toLowerCase().split(" ");

            List<String> keys = new ArrayList<>();
            for (String key : filters) {
                String key_val = key.replaceAll("_", " ");
                if (key_val.isEmpty()) {
                    continue;
                }
                keys.add(key_val);
            }
            entity_filters.put(tmp[0].trim(), keys);
        }

        Map<String, String> wiki_sum = new HashMap<>();
        BufferedReader sum_r = FileUtils.getFileReader(wiki_summaries);
        String sum_l;
        while ((sum_l = sum_r.readLine()) != null) {
            String[] tmp = sum_l.split("\t");
            if (tmp.length != 2) {
                continue;
            }
            wiki_sum.put(tmp[0], tmp[1]);
        }

        Set<String> candidate_files = new HashSet<>();
        FileUtils.getFilesList(candidates, candidate_files);

        candidate_files.parallelStream().forEach(file -> {
            try {
                BufferedReader reader = FileUtils.getFileReader(file);
                String line;
                String entity_name = new File(file).getName().replace(".gz", "");
                System.out.println("Processing entity " + entity_name);
                List<String> filters = entity_filters.get(entity_name);
                StringBuffer sb = new StringBuffer();
                if (filters == null) {
                    System.out.println("Entity filters are missing for " + entity_name);
                }
                while ((line = reader.readLine()) != null) {
                    String[] tmp = line.split("\t");
                    if (tmp.length < 3) {
                        continue;
                    }
                    String entity = tmp[2];
                    if (!wiki_sum.containsKey(entity)) {
                        System.out.println("Summary for " + entity + " is missing");
                        continue;
                    }

                    String summary = wiki_sum.get(entity);
                    for (String filter : filters) {
                        if (summary.contains(filter)) {
                            sb.append(line).append("\n");
                            break;
                        }
                    }
                }

                FileUtils.saveText(sb.toString(), out_dir + "/" + entity_name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Construct the ground truth data for the evaluation of the sampled entities.
     *
     * @param out_dir
     * @param table_data
     * @param candidates
     */
    private static void constructGTTableData(String out_dir, String table_data, String candidates) throws IOException {
        Set<String> candidate_files = new HashSet<>();
        FileUtils.getFilesList(candidates, candidate_files);
        Map<String, Set<String>> candidate_pairs = new HashMap<>();
        Set<String> all_entities = new HashSet<>();

        candidate_files.forEach(file -> {
            String[] lines = FileUtils.readText(file).split("\n");
            for (String line : lines) {
                String[] tmp = line.split("\t");
                if (tmp.length != 2) {
                    continue;
                }
                if (!candidate_pairs.containsKey(tmp[0])) {
                    candidate_pairs.put(tmp[0], new HashSet<>());
                }
                candidate_pairs.get(tmp[0]).add(tmp[1]);
                all_entities.add(tmp[0]);
                all_entities.add(tmp[1]);
            }
        });
        Map<String, JSONObject> seed_table_json = loadArticleTables(all_entities, table_data);

        System.out.printf("Loaded %d tables for %d entities.\n", all_entities.size(), seed_table_json.size());
        for (String article_a : candidate_pairs.keySet()) {
            StringBuffer sb = new StringBuffer();
            if (!seed_table_json.containsKey(article_a)) {
                System.out.printf("Tables for article %s are missing.\n", article_a);
                continue;
            }
            JSONObject article_a_json = seed_table_json.get(article_a);
            JSONArray sections_a = article_a_json.getJSONArray("sections");

            for (int i = 0; i < sections_a.length(); i++) {
                JSONObject section_a = sections_a.getJSONObject(i);
                JSONArray tables_a = section_a.getJSONArray("tables");
                for (int j = 0; j < tables_a.length(); j++) {
                    JSONObject table_a = tables_a.getJSONObject(j);
                    for (String article_b : candidate_pairs.get(article_a)) {
                        if (!seed_table_json.containsKey(article_b)) {
                            System.out.printf("Tables for article %s are missing.\n", article_b);
                            continue;
                        }
                        JSONObject article_b_json = seed_table_json.get(article_b);
                        JSONArray sections_b = article_b_json.getJSONArray("sections");

                        for (int k = 0; k < sections_b.length(); k++) {
                            JSONObject section_b = sections_b.getJSONObject(k);
                            JSONArray tables_b = sections_b.getJSONObject(k).getJSONArray("tables");
                            for (int l = 0; l < tables_b.length(); l++) {
                                JSONObject table_b = tables_b.getJSONObject(l);

                                sb.append(article_a).append("\t").append(section_a.getString("section")).append("\t");
                                sb.append(StringEscapeUtils.escapeJson(table_a.getString("caption"))).append("\t").append(StringEscapeUtils.escapeJson(table_a.getString("table_data"))).append("\t");
                                sb.append(article_b).append("\t").append(section_b.getString("section")).append("\t");
                                sb.append(StringEscapeUtils.escapeJson(table_b.getString("caption"))).append("\t").append(StringEscapeUtils.escapeJson(table_b.getString("table_data"))).append("\n");
                            }
                        }
                    }
                }
            }

            FileUtils.saveText(sb.toString(), out_dir, true);
            sb.delete(0, sb.length());
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
    private static Map<String, JSONObject> loadArticleTables(Set<String> seed_entities, String table_data) throws IOException {
        Map<String, JSONObject> seed_table_json = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(table_data);

        String line;
        while ((line = reader.readLine()) != null) {
            try {
                JSONObject json = new JSONObject(line);
                String entity = json.getString("entity");

                if (!seed_entities.contains(entity)) {
                    continue;
                }
                seed_table_json.put(entity, json);
            } catch (Exception e) {
                System.out.printf("Error processing table data for line \t\t%s.\n", line);
            }
        }
        return seed_table_json;
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
    private static void generateGTPairs(Set<String> seed_entities, String category_path, String article_cats,
                                        String out_file) throws IOException {
        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(category_path);
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, null);
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        System.out.println(entity_cats.size());

        ArticleCandidates ac = new ArticleCandidates(cat);
        StringBuffer sb = new StringBuffer();
        //set num entities for each category.
        DataUtils.updateCatsWithEntities(cat, cats_entities);
        for (String sample_entity : seed_entities) {

            int total = 0;
            for (String child_label : cat.children.keySet()) {
                CategoryRepresentation child = cat.children.get(child_label);
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