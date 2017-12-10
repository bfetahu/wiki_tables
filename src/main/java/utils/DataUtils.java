package utils;

import org.json.JSONObject;
import representation.CategoryRepresentation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 12/8/17.
 */
public class DataUtils {
    /**
     * Read the entity-category associations.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Set<String>> readCategoryMappings(String file) throws IOException {
        Map<String, Set<String>> entity_cats = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);

        String line;
        while ((line = reader.readLine()) != null) {
            line = line.replace("<http://dbpedia.org/resource/", "").replace(">", "");
            String[] parts = line.split("\\s+");
            String article = parts[0].replaceAll("_", " ").trim().intern();
            String category = parts[2].replace("Category:", "");

            if (!entity_cats.containsKey(category)) {
                entity_cats.put(category, new HashSet<>());
            }
            entity_cats.get(category).add(article);
        }

        reader.close();
        return entity_cats;
    }

    /**
     * Load the attributes for entities in DBpedia.
     *
     * @param files
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, Set<String>>> loadEntityAttributes(Set<String> files, Set<String> seed_entities) throws IOException {
        Map<String, Map<String, Set<String>>> entity_attributes = new HashMap<>();

        for (String file : files) {
            BufferedReader reader = FileUtils.getFileReader(file);
            String line;

            while ((line = reader.readLine()) != null) {
                String[] data = line.split("> ");
                if (data.length < 3) {
                    continue;
                }

                String entity = data[0].replace("<http://dbpedia.org/resource/", "").replaceAll("_", " ").trim().intern();
                String predicate = data[1].replace("<", "").intern();
                String value = data[2];

                if (!seed_entities.isEmpty() && !seed_entities.contains(entity)) {
                    continue;
                }
                if (value.endsWith(" .")) {
                    value = value.substring(0, value.lastIndexOf(" ")).trim();
                }

                if (!entity_attributes.containsKey(predicate)) {
                    entity_attributes.put(predicate, new HashMap<>());
                }

                if (!entity_attributes.get(predicate).containsKey(entity)) {
                    entity_attributes.get(predicate).put(entity, new HashSet<>());
                }
                entity_attributes.get(predicate).get(entity).add(value);
            }
        }

        return entity_attributes;
    }


    /**
     * Loads the set of entities for which we have extracted tables.
     *
     * @param seed_entity_path
     * @return
     */
    public static Set<String> loadSeedEntities(String seed_entity_path) throws IOException {
        Set<String> entities = new HashSet<>();

        BufferedReader reader = FileUtils.getFileReader(seed_entity_path);
        String line;
        while ((line = reader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String entity = json.getString("entity");
            entities.add(entity);
        }

        return entities;
    }

    /**
     * Associate entities to their corresponding categories, and correspondingly following the category
     * hierarchy associate the entities to the upper categories in the hierarchy.
     * <p>
     * Return the separate categories in a map data structure.
     *
     * @param cat
     * @param entity_categories
     * @return
     */
    public static Map<String, CategoryRepresentation> updateCatsWithEntities(CategoryRepresentation cat, Map<String, Set<String>> entity_categories) {
        //set num entities for each category.
        Map<String, CategoryRepresentation> cats = new HashMap<>();
        cat.loadIntoMapChildCats(cats);

        entity_categories.keySet().forEach(category -> {
            CategoryRepresentation cat_node = cats.get(category);
            if (cat_node == null) {
                return;
            }
            cat_node.entities.addAll(entity_categories.get(category));
        });

        cat.gatherEntities();
        return cats;
    }
}
