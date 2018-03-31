package utils;

import io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import representation.CategoryRepresentation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 12/20/17.
 */
public class DBUtils {
    public static void generateCategoryList(String category_path, String article_cats, String seed_path, String cat_id_path, String out_dir) throws IOException {
        Set<String> seed_entities = FileUtils.readIntoSet(seed_path, "\n", false);
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, seed_entities);
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        CategoryRepresentation root = CategoryRepresentation.readCategoryGraph(category_path);
        Map<String, CategoryRepresentation> cats = DataUtils.updateCatsWithEntities(root, entity_cats);
        Map<String, Integer> cat_ids = FileUtils.readIntoStringIntMap(cat_id_path, "\t", false);

        StringBuffer sb = new StringBuffer();
        StringBuffer sb2 = new StringBuffer();

        for (String cat_label : cats.keySet()) {
            CategoryRepresentation cat = cats.get(cat_label);

            sb.append(cat.node_id).append("\t").append(cat.label).append("\t").append(cat.level).append("\t").append(cat.entities.size()).append("\n");

            for (String parent_label : cat.parents.keySet()) {
                CategoryRepresentation parent = cat.parents.get(parent_label);
                sb2.append(cat_ids.get(cat.label)).append("\t").append(cat_ids.get(parent.label)).append("\n");
            }

            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), out_dir + "/categories.tsv", true);
                FileUtils.saveText(sb2.toString(), out_dir + "/cat_tax.tsv", true);

                sb.delete(0, sb.length());
                sb2.delete(0, sb2.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_dir + "/categories.tsv", true);
        FileUtils.saveText(sb2.toString(), out_dir + "/cat_tax.tsv", true);
    }

    public static void generateTransitiveCategoryArticleAssociation(String category_path, String article_cats, String cat_id_path, String entity_id_path, String out_dir) throws IOException {
        Map<String, Integer> entity_ids = FileUtils.readIntoStringIntMap(entity_id_path, "\t", false);
        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, entity_ids.keySet());
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        Map<String, Integer> cat_ids = FileUtils.readIntoStringIntMap(cat_id_path, "\t", false);

        CategoryRepresentation root = CategoryRepresentation.readCategoryGraph(category_path);
        Map<String, CategoryRepresentation> cats = DataUtils.updateCatsWithEntities(root, cats_entities);

        StringBuffer sb = new StringBuffer();

        for (String cat_label : cats.keySet()) {
            CategoryRepresentation cat = cats.get(cat_label);
            int cat_id = cat_ids.get(cat_label);
            for (String entity : cat.entities) {
                sb.append(cat_id).append("\t").append(entity_ids.get(entity)).append("\n");
            }

            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), out_dir + "/cat_entities.tsv", true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_dir + "/cat_entities.tsv", true);
    }

    public static void generateEntityTables(String table_path, String entity_id_path) throws IOException {
        Map<String, Integer> entity_ids = FileUtils.readIntoStringIntMap(entity_id_path, "\t", false);

        BufferedReader reader = FileUtils.getFileReader(table_path);
        String line;

        StringBuffer sb = new StringBuffer();
        while ((line = reader.readLine()) != null) {
            try {
                JSONObject json = new JSONObject(line);
                int entity_id = entity_ids.get(json.getString("entity"));

                JSONArray sections = json.getJSONArray("sections");
                for (int i = 0; i < sections.length(); i++) {
                    JSONObject section = sections.getJSONObject(i);
                    JSONArray tables = section.getJSONArray("tables");

                    String json_table_text = tables.toString();

                    sb.append(entity_id).append("\t").append(section.getString("section")).
                            append("\t").append(StringEscapeUtils.escapeJson(json_table_text)).append("\n");
                }

                FileUtils.saveText(sb.toString(), "entity_tables_db.txt", true);
                sb.delete(0, sb.length());
            } catch (Exception e) {
                System.out.printf("Error %finished_gt_seeds processing line %finished_gt_seeds.\n", e.getMessage(), line);
            }
        }

    }

    public static void main(String[] args) throws IOException {
        generateTransitiveCategoryArticleAssociation(args[0], args[1], args[2], args[3], args[4]);
//        generateCategoryList(args[0], args[1], args[2], args[3], args[4]);
//        generateEntityTables(args[0], args[1]);
    }
}
