package utils;

import datastruct.wikitable.WikiTable;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import representation.CategoryRepresentation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by besnik on 12/8/17.
 */
public class DataUtils {
    public static Pattern pdates[] = {
            Pattern.compile("[0-9]{3,}–[0-9]{2,}"), Pattern.compile("[0-9]{3,}-[0-9]{2,}"),
            Pattern.compile("\\b[0-9]{3,}\\b"), Pattern.compile("[0-9]+[a-z]+\\s+"),
            Pattern.compile("[0-9]+[a-z]+-[a-zA-Z]+\\s+"), Pattern.compile("[0-9]+[a-z]+–[a-zA-Z]+\\s+"),
            Pattern.compile("from(.*?)$"), Pattern.compile("in\\s+(.*?)$"),
            Pattern.compile("of\\s+(.*?)$"), Pattern.compile("by\\s+(.*?)$")
    };

    /**
     * Remove date information from the category label.
     *
     * @param category
     * @return
     */
    public static String removeDateFromCategory(String category) {
        String new_cat_label = category;
        for (int i = 0; i < pdates.length; i++) {
            Matcher m = pdates[i].matcher(new_cat_label);
            while (m.find()) {
                String tmp = m.replaceAll("").trim();
                if (tmp.isEmpty()) {
                    continue;
                }
                new_cat_label = tmp;
            }
        }
        return new_cat_label.trim();
    }

    /**
     * Read the entity-category associations.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Set<String>> readCategoryMappingsWiki(String file, Set<String> seed_entities) throws IOException {
        Map<String, Set<String>> entity_cats = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);

        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");

            if (line.isEmpty() || parts.length < 2) {
                continue;
            }

            String article = parts[0].trim().intern();
            String category = parts[1].trim().intern();

            //in case we want to limit the category representation only to those entities which have a table.
            if (seed_entities != null && !seed_entities.isEmpty() && !seed_entities.contains(article)) {
                continue;
            }

            if (!entity_cats.containsKey(category)) {
                entity_cats.put(category, new HashSet<>());
            }
            entity_cats.get(category).add(article);
        }
        return entity_cats;
    }

    /**
     * Read the entity-category associations.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Set<String>> readEntityCategoryMappingsWiki(String file, Set<String> seed_entities) throws IOException {
        Map<String, Set<String>> entity_cats = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);

        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");

            if (line.isEmpty() || parts.length < 2) {
                continue;
            }

            String article = parts[0].trim().intern();
            String category = parts[1].trim().intern();

            //in case we want to limit the category representation only to those entities which have a table.
            if (seed_entities != null && !seed_entities.isEmpty() && !seed_entities.contains(article)) {
                continue;
            }

            if (!entity_cats.containsKey(article)) {
                entity_cats.put(article, new HashSet<>());
            }
            entity_cats.get(article).add(category);
        }
        return entity_cats;
    }

    /**
     * Load the attributes for entities in DBpedia.
     *
     * @param entity_attributes_path
     * @param seed_entities
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, TIntHashSet>> loadEntityAttributes(String entity_attributes_path, Set<String> seed_entities) throws IOException {
        Set<String> attribute_files = new HashSet<>();
        FileUtils.getFilesList(entity_attributes_path, attribute_files);
        Map<String, Map<String, TIntHashSet>> entity_attributes = new HashMap<>();

        for (String file : attribute_files) {
            BufferedReader reader = FileUtils.getFileReader(file);
            String line;

            while ((line = reader.readLine()) != null) {
                String[] data = line.split("\t");
                if (data.length < 3) {
                    continue;
                }

                String entity = data[0].trim().intern();
                String predicate = data[1].intern();
                int value = data[2].hashCode();

                if (seed_entities != null && !seed_entities.isEmpty() && !seed_entities.contains(entity)) {
                    continue;
                }

                if (!entity_attributes.containsKey(predicate)) {
                    entity_attributes.put(predicate, new HashMap<>());
                }

                if (!entity_attributes.get(predicate).containsKey(entity)) {
                    entity_attributes.get(predicate).put(entity, new TIntHashSet());
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

    /**
     * Convert the category -> article map datastructure into article -> category.
     *
     * @param cats_entities
     * @return
     */
    public static Map<String, Set<String>> getArticleCategories(Map<String, Set<String>> cats_entities) {
        Map<String, Set<String>> entity_cats = new HashMap<>();
        cats_entities.keySet().forEach(category ->
                cats_entities.get(category).forEach(entity -> {
                    if (!entity_cats.containsKey(entity)) {
                        entity_cats.put(entity, new HashSet<>());
                    }
                    entity_cats.get(entity).add(category);
                })
        );
        return entity_cats;
    }

    /**
     * Compute the attribute weights for all the categories.
     *
     * @param max_level_property
     * @return
     */
    public static Map<String, TIntDoubleHashMap> computeCategoryAttributeWeights(Map<String, Double> max_level_property, Map<String, CategoryRepresentation> cat_to_map) {
        Map<String, TIntDoubleHashMap> weights = new HashMap<>();

        for (String category : cat_to_map.keySet()) {
            TIntDoubleHashMap cat_weights = DataUtils.computeCategoryPropertyWeights(cat_to_map.get(category), max_level_property);
            weights.put(category, cat_weights);
        }
        return weights;
    }

    /**
     * Compute the weights for attributes in a category. For a category c, and attribute p the weight is :
     * weight(p, c) = \lambda_c / \max\lambda_c_j * \frac{|\cup \langle p, o\rangle|}{|\langle p, o\rangle|}
     * where \lambda_c is the level of category c, and \max_lambda_c_j is the maximum length (closest to the root) in
     * which the property p appears, whereas the fraction consists of the number of unique instantiations of p in c, and
     * the total number of instantiations of p in c.
     *
     * @param cat
     * @param min_level_property
     * @return
     */
    public static TIntDoubleHashMap computeCategoryPropertyWeights(CategoryRepresentation cat, Map<String, Double> min_level_property) {
        TIntDoubleHashMap cat_prop_weight = new TIntDoubleHashMap();

        Map<String, TIntIntHashMap> cat_rep = cat.cat_representation;
        for (String prop : cat_rep.keySet()) {
            int num_values = cat_rep.get(prop).size();
            double num_assignments = Arrays.stream(cat_rep.get(prop).values()).mapToDouble(x -> x).sum();

            double weight = cat.level / min_level_property.get(prop) * (-Math.log(num_values / num_assignments));
            cat_prop_weight.put(prop.hashCode(), weight);
        }
        return cat_prop_weight;
    }

    /**
     * Compute the Euclidean distance for any two sets of weights for a given property, weight set.
     *
     * @param weights_a
     * @param weights_b
     * @return
     */
    public static double computeEuclideanDistance(TIntDoubleHashMap weights_a, TIntDoubleHashMap weights_b) {
        double result = 0.0;
        if (weights_a.isEmpty() || weights_b.isEmpty()) {
            return Double.MAX_VALUE;
        }

        TIntHashSet all_keys = new TIntHashSet(weights_a.keySet());
        all_keys.addAll(weights_b.keySet());

        for (int key : all_keys.toArray()) {
            double val_a = weights_a.containsKey(key) ? weights_a.get(key) : 0.0;
            double val_b = weights_b.containsKey(key) ? weights_b.get(key) : 0.0;
            result += Math.pow(val_a - val_b, 2);
        }
        return Math.sqrt(result);
    }

    /**
     * Compute the deepest level with which an attribute is associated to a category.
     *
     * @param cats
     * @return
     */
    public static Map<String, Double> computeMaxLevelAttributeCategory(Map<String, CategoryRepresentation> cats) {
        Map<String, Double> attribute_max_level = new HashMap<>();

        for (String cat_label : cats.keySet()) {
            CategoryRepresentation cat = cats.get(cat_label);
            Set<String> attributes = cat.cat_representation.keySet();
            double level = cat.level;

            for (String attribute : attributes) {
                if (!attribute_max_level.containsKey(attribute)) {
                    attribute_max_level.put(attribute, level);
                } else if (attribute_max_level.get(attribute) < level) {
                    attribute_max_level.put(attribute, level);
                }
            }
        }
        return attribute_max_level;
    }


    /**
     * Find the lowest common ancestor for the two categories under consideration.
     *
     * @param cat_a
     * @param cat_b
     * @return
     */
    public static Set<CategoryRepresentation> findCommonAncestor(CategoryRepresentation cat_a, CategoryRepresentation cat_b, Map<String, CategoryRepresentation> cat_to_map) {
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
    public static void gatherParents(CategoryRepresentation cat, Set<String> parents) {
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


    /**
     * Loads the category representation similarity. The category labels are hashed in order to save space.
     *
     * @param file
     * @return
     */
    public static TIntObjectHashMap<TIntDoubleHashMap> loadCategoryRepSim(String file) throws IOException {
        TIntObjectHashMap<TIntDoubleHashMap> sim = new TIntObjectHashMap<>();

        String line;
        BufferedReader reader = FileUtils.getFileReader(file);

        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            int cat_a = data[0].hashCode();
            int cat_b = data[1].hashCode();

            double score = Double.parseDouble(data[5]);

            if (!sim.containsKey(cat_a)) {
                sim.put(cat_a, new TIntDoubleHashMap());
            }
            sim.get(cat_a).put(cat_b, score);

        }
        return sim;
    }

    /**
     * Find the lowest common ancestors between two category sets which we have extracted from an entity.
     *
     * @param article_a_cats
     * @param article_b_cats
     * @param cat_to_map
     * @return
     */
    public static Set<String> findLCACategories(Set<String> article_a_cats, Set<String> article_b_cats, Map<String, CategoryRepresentation> cat_to_map) {
        int max_level_a = article_a_cats.stream().filter(c -> cat_to_map.containsKey(c)).mapToInt(c -> cat_to_map.get(c).level).max().getAsInt();
        int max_level_b = article_b_cats.stream().filter(c -> cat_to_map.containsKey(c)).mapToInt(c -> cat_to_map.get(c).level).max().getAsInt();

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
        if (same_cats) {
            return article_a_cats;
        }

        for (String cat_a_label : article_a_cats) {
            CategoryRepresentation cat_a = cat_to_map.get(cat_a_label);
            if (cat_a == null || cat_a.level < max_level_a) {
                continue;
            }

            for (String cat_b_label : article_b_cats) {
                CategoryRepresentation cat_b = cat_to_map.get(cat_b_label);
                if (cat_b == null || cat_b.level < max_level_b) {
                    continue;
                }

                //get the lowest common ancestors between the two categories
                Set<CategoryRepresentation> common_ancestors = DataUtils.findCommonAncestor(cat_a, cat_b, cat_to_map);
                if (common_ancestors == null || common_ancestors.isEmpty()) {
                    continue;
                }
                return common_ancestors.stream().map(x -> x.label).collect(Collectors.toSet());
            }
        }

        return null;
    }

    /**
     * Load the word vectors.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, TDoubleArrayList> loadWord2Vec(String file) throws IOException {
        Map<String, TDoubleArrayList> w2v = new HashMap<>();

        String line;
        BufferedReader reader = FileUtils.getFileReader(file);
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\s+");

            String word = data[0];
            TDoubleArrayList vec = new TDoubleArrayList();
            for (int i = 1; i < data.length; i++) {
                vec.add(Double.parseDouble(data[i]));
            }
            w2v.put(word, vec);
        }

        return w2v;
    }

    /**
     * Load the tables for the group of entities of interest.
     *
     * @param structured_json
     * @param entities
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, List<WikiTable>>> loadTables(String structured_json, Set<String> entities, boolean loadColValueDist) throws IOException {
        Map<String, Map<String, List<WikiTable>>> tbls = new HashMap<>();

        String line;
        BufferedReader reader = FileUtils.getFileReader(structured_json);
        while ((line = reader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String entity = json.getString("entity");

            if (!entities.contains(entity)) {
                continue;
            }

            Map<String, List<WikiTable>> section_tables = new HashMap<>();
            JSONArray sections = json.getJSONArray("sections");
            for (int i = 0; i < sections.length(); i++) {
                JSONObject section = sections.getJSONObject(i);
                String section_label = section.getString("section");

                JSONArray tables = section.getJSONArray("tables");
                List<WikiTable> table_list = new ArrayList<>();
                for (int k = 0; k < tables.length(); k++) {
                    JSONObject table = tables.getJSONObject(k);

                    WikiTable tbl = new WikiTable();
                    tbl.loadFromStructuredJSON(table, loadColValueDist);
                    table_list.add(tbl);
                }
                section_tables.put(section_label, table_list);
            }
            tbls.put(entity, section_tables);
        }

        return tbls;
    }

    /**
     * Compute the cosine similarity between two vectors.
     *
     * @param a
     * @param b
     * @return
     */
    public static double computeCosineSim(TDoubleArrayList a, TDoubleArrayList b) {
        double score = 0.0;

        for (int i = 0; i < a.size(); i++) {
            score += a.get(i) * b.get(i);
        }
        double sum_a = Arrays.stream(a.toArray()).map(x -> Math.pow(x, 2)).sum();
        double sum_b = Arrays.stream(b.toArray()).map(x -> Math.pow(x, 2)).sum();

        score /= Math.sqrt(sum_a) * Math.sqrt(sum_b);
        return score;
    }
}
