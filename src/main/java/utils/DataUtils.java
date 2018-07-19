package utils;

import datastruct.wikitable.WikiColumnHeader;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by besnik on 12/8/17.
 */
public class DataUtils {
    //keep the precomputed LCA categories so that the process is faster.
    public static Map<String, Map<String, Set<String>>> lca_cats = new ConcurrentHashMap<>();
    public static boolean log_lca_cats = false;

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
            return -1;
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
     * Compute the Euclidean distance for any two sets of weights for a given property, weight set.
     *
     * @param weights_a
     * @param weights_b
     * @return
     */
    public static double computeCosine(TIntDoubleHashMap weights_a, TIntDoubleHashMap weights_b) {
        if (weights_a == null || weights_b == null || weights_a.isEmpty() || weights_b.isEmpty()) {
            return 0.0;
        }

        TIntHashSet all_keys = new TIntHashSet(weights_a.keySet());
        all_keys.addAll(weights_b.keySet());

        TIntHashSet common_keys = new TIntHashSet(weights_a.keySet());
        common_keys.retainAll(weights_b.keys());

        if (common_keys.isEmpty()) {
            return 0.0;
        }

        double[] a = new double[all_keys.size()];
        double[] b = new double[all_keys.size()];

        int[] keys = all_keys.toArray();

        for (int i = 0; i < keys.length; i++) {
            a[i] = weights_a.containsKey(keys[i]) ? weights_a.get(keys[i]) : 0;
            b[i] = weights_b.containsKey(keys[i]) ? weights_b.get(keys[i]) : 0;
        }

        double score = Arrays.stream(common_keys.toArray()).mapToDouble(key -> weights_a.get(key) * weights_b.get(key)).sum();
        double sum_a = Arrays.stream(a).map(x -> Math.pow(x, 2)).sum();
        double sum_b = Arrays.stream(b).map(x -> Math.pow(x, 2)).sum();

        return score / (Math.sqrt(sum_a) * Math.sqrt(sum_b));
    }

    /**
     * Compute the Euclidean distance for any two sets of weights for a given property, weight set.
     *
     * @param weights_a
     * @param weights_b
     * @return
     */
    public static double computeCosine(Map<String, Double> weights_a, Map<String, Double> weights_b) {
        if (weights_a == null || weights_b == null || weights_a.isEmpty() || weights_b.isEmpty()) {
            return 0.0;
        }

        Set<String> all_keys = new HashSet<>(weights_a.keySet());
        all_keys.addAll(weights_b.keySet());

        Set<String> common_keys = new HashSet<>(weights_a.keySet());
        common_keys.retainAll(weights_b.keySet());

        if (common_keys.isEmpty()) {
            return 0.0;
        }

        double[] a = new double[all_keys.size()];
        double[] b = new double[all_keys.size()];

        String[] keys = new String[all_keys.size()];
        all_keys.toArray(keys);

        for (int i = 0; i < keys.length; i++) {
            a[i] = weights_a.containsKey(keys[i]) ? weights_a.get(keys[i]) : 0;
            b[i] = weights_b.containsKey(keys[i]) ? weights_b.get(keys[i]) : 0;
        }

        double score = Arrays.stream(common_keys.toArray()).mapToDouble(key -> weights_a.get(key) * weights_b.get(key)).sum();
        double sum_a = Arrays.stream(a).map(x -> Math.pow(x, 2)).sum();
        double sum_b = Arrays.stream(b).map(x -> Math.pow(x, 2)).sum();

        return score / (Math.sqrt(sum_a) * Math.sqrt(sum_b));
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
     * Find the lowest common ancestor for the two categories under consideration.
     *
     * @param parents_a
     * @param parents_b
     * @param cat_to_map
     * @return
     */
    public static Set<String> findCommonAncestorSimple(TIntHashSet parents_a, TIntHashSet parents_b, Map<Integer, String> cat_index, Map<String, CategoryRepresentation> cat_to_map) {
        Set<String> ancestors = new HashSet<>();

        TIntHashSet common_parents = new TIntHashSet(parents_a);
        //take the intersection of the two parent sets.
        common_parents.retainAll(parents_b);

        if (common_parents.isEmpty()) {
            return null;
        }

        int max_level = -1;
        for (int common_cat_id : common_parents.toArray()) {
            String cat_label = cat_index.get(common_cat_id);
            if (cat_to_map.containsKey(cat_label)) {
                CategoryRepresentation cat = cat_to_map.get(cat_label);
                if (max_level < cat.level) {
                    max_level = cat.level;
                }
            }
        }

        if (max_level == -1) {
            return null;
        }

        for (int parent_id : common_parents.toArray()) {
            String parent = cat_index.get(parent_id);
            if (!cat_to_map.containsKey(parent)) {
                continue;
            }
            CategoryRepresentation cat = cat_to_map.get(parent);
            if (cat.level == max_level) {
                ancestors.add(parent);
            }
        }

        return ancestors;
    }

    /**
     * Find the lowest common ancestor for the two categories under consideration.
     *
     * @param parents_a
     * @param parents_b
     * @param cat_to_map
     * @return
     */
    public static Set<CategoryRepresentation> findCommonAncestor(Set<String> parents_a, Set<String> parents_b, Map<String, CategoryRepresentation> cat_to_map) {
        Set<CategoryRepresentation> ancestors = new HashSet<>();

        Set<String> common_parents = new HashSet<>(parents_a);
        //take the intersection of the two parent sets.
        common_parents.retainAll(parents_b);
        common_parents.remove("root");

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
     * Gather all the parents of category up to the root of the category.
     *
     * @param cat
     * @param parents
     */
    public static void gatherParents(CategoryRepresentation cat, TIntHashSet parents) {
        if (cat == null || cat.parents == null) {
            return;
        }
        cat.parents.keySet().forEach(parent_label -> {
            if (parents.contains(parent_label.hashCode())) {
                return;
            }
            parents.add(parent_label.hashCode());

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

            double score = Double.parseDouble(data[2]);

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
    public static Set<String> findLCACategories(Set<String> article_a_cats, Set<String> article_b_cats, Map<String, CategoryRepresentation> cat_to_map, int max_level_a, int max_level_b) {
        //check first if they come from the same categories.
        if (article_a_cats == null || article_b_cats == null || article_b_cats.isEmpty() || article_a_cats.isEmpty()) {
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

            if (log_lca_cats && !lca_cats.containsKey(cat_a_label)) {
                lca_cats.put(cat_a_label, new ConcurrentHashMap<>());
            }

            for (String cat_b_label : article_b_cats) {
                CategoryRepresentation cat_b = cat_to_map.get(cat_b_label);
                if (cat_b == null || cat_b.level < max_level_b) {
                    continue;
                }

                if (log_lca_cats && lca_cats.get(cat_a_label).containsKey(cat_b_label)) {
                    return lca_cats.get(cat_a_label).get(cat_b_label);
                }

                //get the lowest common ancestors between the two categories
                Set<CategoryRepresentation> common_ancestors = DataUtils.findCommonAncestor(cat_a, cat_b, cat_to_map);
                if (common_ancestors == null || common_ancestors.isEmpty()) {
                    continue;
                }
                Set<String> lca = common_ancestors.stream().map(x -> x.label).collect(Collectors.toSet());

                if (log_lca_cats) {
                    lca_cats.get(cat_a_label).put(cat_b_label, lca);
                }
                return lca;
            }
        }

        return null;
    }


    /**
     * Find the lowest common ancestors between two category sets which we have extracted from an entity.
     *
     * @param article_a_cats
     * @param article_b_cats
     * @param cat_to_map
     * @param cat_index
     * @return
     */
    public static Set<String> findLCACategories(TIntHashSet article_a_cats, TIntHashSet article_b_cats,
                                                Map<Integer, String> cat_index, Map<String, CategoryRepresentation> cat_to_map) {
        //check first if they come from the same categories.
        if (article_a_cats == null || article_b_cats == null || article_b_cats.isEmpty() || article_a_cats.isEmpty()) {
            //return null in this case, indicating that the articles belong to exactly the same categories
            return null;
        }

        boolean same_cats = article_a_cats.equals(article_b_cats);
        if (same_cats) {
            Set<String> cats = new HashSet<>();
            Arrays.asList(article_a_cats).stream().filter(i -> cat_index.containsKey(i)).forEach(i -> cats.add(cat_index.get(i)));
            return cats;
        }


        //get the lowest common ancestors between the two categories
        return DataUtils.findCommonAncestorSimple(article_a_cats, article_b_cats, cat_index, cat_to_map);
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
        //check first if they come from the same categories.
        if (article_a_cats == null || article_b_cats == null || article_b_cats.isEmpty() || article_a_cats.isEmpty()) {
            //return null in this case, indicating that the articles belong to exactly the same categories
            return null;
        }
        if (article_a_cats.stream().filter(c -> cat_to_map.containsKey(c)).count() == 0 || article_b_cats.stream().filter(c -> cat_to_map.containsKey(c)).count() == 0) {
            return null;
        }
        boolean same_cats = article_a_cats.equals(article_b_cats);

        int max_level_a = article_a_cats.stream().filter(c -> cat_to_map.containsKey(c)).mapToInt(c -> cat_to_map.get(c).level).max().getAsInt();
        int max_level_b = article_b_cats.stream().filter(c -> cat_to_map.containsKey(c)).mapToInt(c -> cat_to_map.get(c).level).max().getAsInt();

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
    public static Map<String, List<WikiTable>> loadTables(String structured_json, Set<String> entities, boolean loadColValueDist) throws IOException {
        Map<String, List<WikiTable>> tbls = new HashMap<>();

        String line;
        BufferedReader reader = FileUtils.getFileReader(structured_json);
        while ((line = reader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String entity = json.getString("entity");

            if (!entities.contains(entity)) {
                continue;
            }

            List<WikiTable> table_list = new ArrayList<>();
            JSONArray sections = json.getJSONArray("sections");
            for (int i = 0; i < sections.length(); i++) {
                JSONObject section = sections.getJSONObject(i);
                String section_label = section.getString("section");

                JSONArray tables = section.getJSONArray("tables");
                for (int k = 0; k < tables.length(); k++) {
                    JSONObject table = tables.getJSONObject(k);

                    WikiTable tbl = new WikiTable();
                    tbl.loadFromStructuredJSON(table, loadColValueDist);

                    tbl.entity = entity;
                    tbl.section = section_label;
                    table_list.add(tbl);
                }
            }
            tbls.put(entity, table_list);
        }
        int table_counter = tbls.values().stream().mapToInt(x -> x.size()).sum();
        System.out.printf("Loaded %d tables from %d entities.\n", table_counter, tbls.size());

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
        if (a == null || b == null || a.isEmpty() || b.isEmpty()) {
            return 0.0;
        }
        double score = 0.0;

        for (int i = 0; i < a.size(); i++) {
            score += a.get(i) * b.get(i);
        }
        double sum_a = Arrays.stream(a.toArray()).map(x -> Math.pow(x, 2)).sum();
        double sum_b = Arrays.stream(b.toArray()).map(x -> Math.pow(x, 2)).sum();

        score /= Math.sqrt(sum_a) * Math.sqrt(sum_b);
        return score;
    }


    /**
     * Compute the cosine similarity between two vectors.
     *
     * @param a
     * @param b
     * @return
     */
    public static double computeCosineSim(TDoubleArrayList a, TDoubleArrayList b, double sum_a, double sum_b) {
        if (a == null || b == null || a.isEmpty() || b.isEmpty()) {
            return 0.0;
        }
        double score = 0.0;

        for (int i = 0; i < a.size(); i++) {
            score += a.get(i) * b.get(i);
        }

        score /= Math.sqrt(sum_a) * Math.sqrt(sum_b);
        return score;
    }


    /**
     * Generate an average word vector for a given text.
     *
     * @param text
     * @param word2vec
     * @return
     */
    public static TDoubleArrayList computeAverageWordVector(String text, Map<String, TDoubleArrayList> word2vec) {
        if (text == null || text.isEmpty() || word2vec == null) {
            return null;
        }
        String[] a = text.toLowerCase().replaceAll("\\W+", " ").split(" ");

        //compute average word vectors
        TDoubleArrayList avg_a = new TDoubleArrayList();
        for (String key : a) {
            if (!word2vec.containsKey(key)) {
                continue;
            }

            double[] w2v_arr = word2vec.get(key).toArray();
            if (avg_a.isEmpty()) {
                avg_a.addAll(w2v_arr);
            } else {
                for (int i = 0; i < w2v_arr.length; i++) {
                    avg_a.set(i, avg_a.get(i) + w2v_arr[i]);
                }
            }
        }

        TDoubleArrayList avg = new TDoubleArrayList();
        for (double val : avg_a.toArray()) {
            double score = val / avg_a.size();
            avg.add(score);
        }
        return avg;
    }


    /**
     * Generate an average word vector for a given text.
     *
     * @param text
     * @param word2vec
     * @return
     */
    public static TDoubleArrayList computeAverageWordVector(String text, Set<String> stopwords, Map<String, TDoubleArrayList> word2vec) {
        if (text == null || text.isEmpty() || word2vec == null) {
            return null;
        }
        String[] a = text.toLowerCase().replaceAll("\\W+", " ").split(" ");

        int total = 0;
        //compute average word vectors
        TDoubleArrayList avg_a = new TDoubleArrayList();
        for (String key : a) {
            if (!word2vec.containsKey(key) || stopwords.contains(key)) {
                continue;
            }

            total++;
            double[] w2v_arr = word2vec.get(key).toArray();
            if (avg_a.isEmpty()) {
                avg_a.addAll(w2v_arr);
            } else {
                for (int i = 0; i < w2v_arr.length; i++) {
                    avg_a.set(i, avg_a.get(i) + w2v_arr[i]);
                }
            }
        }

        TDoubleArrayList avg = new TDoubleArrayList();
        for (double val : avg_a.toArray()) {
            double score = val / total;
            avg.add(score);
        }
        return avg;
    }

    /**
     * Generate an average word vector for a given text.
     *
     * @param words
     * @param word2vec
     * @return
     */
    public static TDoubleArrayList computeAverageWordVector(Set<String> words, Map<String, TDoubleArrayList> word2vec) {
        if (words == null || words.isEmpty() || word2vec == null) {
            return null;
        }

        //compute average word vectors
        TDoubleArrayList avg_a = new TDoubleArrayList();
        int total = 0;
        for (String key : words) {
            if (!word2vec.containsKey(key)) {
                continue;
            }
            total++;

            double[] w2v_arr = word2vec.get(key).toArray();
            if (avg_a.isEmpty()) {
                avg_a.addAll(w2v_arr);
            } else {
                for (int i = 0; i < w2v_arr.length; i++) {
                    avg_a.set(i, avg_a.get(i) + w2v_arr[i]);
                }
            }
        }

        TDoubleArrayList avg = new TDoubleArrayList();
        for (double val : avg_a.toArray()) {
            double score = val / total;
            avg.add(score);
        }
        return avg;
    }


    /**
     * Compute the average category vectors.
     *
     * @param categories
     * @param emb
     * @return
     */
    public static TDoubleArrayList computeCategoryAverageVector(Set<String> categories, Map<String, TDoubleArrayList> emb) {
        //compute average word vectors
        TDoubleArrayList avg_a = new TDoubleArrayList();
        int total = 0;
        for (String category : categories) {
            String key = "Category:" + category.replaceAll(" ", "_");
            if (!emb.containsKey(key)) {
                continue;
            }

            total++;

            double[] w2v_arr = emb.get(key).toArray();
            if (avg_a.isEmpty()) {
                avg_a.addAll(w2v_arr);
            } else {
                for (int i = 0; i < w2v_arr.length; i++) {
                    avg_a.set(i, avg_a.get(i) + w2v_arr[i]);
                }
            }
        }

        TDoubleArrayList avg = new TDoubleArrayList();
        for (double val : avg_a.toArray()) {
            double score = val / total;
            avg.add(score);
        }
        return avg;
    }

    /**
     * Load the lead section  or abstract of Wikipedia pages.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, String> loadEntityAbstracts(String file, Set<String> filter_entities) throws IOException {
        //load the entity abstracts
        Map<String, String> entity_abstracts = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            if (data.length != 2) {
                continue;
            }
            String entity = data[0].trim();
            if (!filter_entities.contains(entity)) {
                continue;
            }
            String abstract_text = data[1].toLowerCase().replaceAll("\\W+", " ");
            entity_abstracts.put(entity, abstract_text);
        }
        System.out.printf("Loaded entity abstracts with %d entries.\n", entity_abstracts.size());
        return entity_abstracts;
    }


    /**
     * Load the lead section  or abstract of Wikipedia pages.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Set<String>> loadEntityTypes(String file, Set<String> filter_entities) throws IOException {
        //load the entity abstracts
        Map<String, Set<String>> types = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            if (data.length != 2) {
                continue;
            }
            String entity = data[0].trim();
            if (!filter_entities.contains(entity)) {
                continue;
            }

            if (!types.containsKey(data[0])) {
                types.put(data[0], new HashSet<>());
            }
            types.get(data[0]).add(data[1]);
        }
        System.out.printf("Loaded entity types with %d entries.\n", types.size());
        return types;
    }

    /**
     * Load the lead section  or abstract of Wikipedia pages.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, String> loadEntityAbstracts(String file) throws IOException {
        //load the entity abstracts
        Map<String, String> entity_abstracts = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            if (data.length != 2) {
                continue;
            }
            String entity = data[0].trim();
            String abstract_text = data[1].toLowerCase().replaceAll("[^a-z0-9 ]", " ").replaceAll("<ref(.*?)</ref>", "").replaceAll("\\{\\{cite(.*?)\\}\\}", "");
            entity_abstracts.put(entity, abstract_text);
        }
        System.out.printf("Loaded entity abstracts with %d entries.\n", entity_abstracts.size());
        return entity_abstracts;
    }


    /**
     * Compute the TF-IDF scores of the entity abstracts.
     *
     * @param ea
     * @return
     */
    public static Map<String, TIntDoubleHashMap> computeTFIDF(Map<String, String> ea, Set<String> stop_words) {
        TIntObjectHashMap<TIntHashSet> idf = new TIntObjectHashMap<>();
        TIntObjectHashMap<TIntIntHashMap> tf = new TIntObjectHashMap<>();

        Map<String, Integer> dict = new HashMap<>();

        int word_id = 0;
        for (String entity : ea.keySet()) {
            int entity_id = entity.hashCode();
            String text = ea.get(entity);
            String[] words = text.split("\\s+");

            TIntIntHashMap sub_tf = new TIntIntHashMap();
            tf.put(entity.hashCode(), sub_tf);
            for (String word : words) {
                if (stop_words.contains(word)) {
                    continue;
                }

                if (!dict.containsKey(word)) {
                    dict.put(word, word_id);
                    word_id++;
                }

                int word_index = dict.get(word);
                if (!idf.containsKey(word_index)) {
                    idf.put(word_index, new TIntHashSet());
                }
                idf.get(word_index).add(entity_id);
                if (!sub_tf.containsKey(word_index)) {
                    sub_tf.put(word_index, 0);
                }
                sub_tf.put(word_index, sub_tf.get(word_index) + 1);
            }
        }

        Map<String, TIntDoubleHashMap> tfidf_scores = new HashMap<>();
        double N = ea.size();
        for (String entity : ea.keySet()) {
            int entity_id = entity.hashCode();
            TIntIntHashMap tf_scores = tf.get(entity_id);
            TIntDoubleHashMap scores = new TIntDoubleHashMap();
            tfidf_scores.put(entity, scores);

            for (int word_index : tf_scores.keys()) {
                double idf_score = Math.log(1 + N / idf.get(word_index).size());
                double tf_score = Math.log(1 + tf_scores.get(word_index));
                double score = tf_score * idf_score;
                scores.put(word_index, score);
            }
        }
        return tfidf_scores;
    }

    /**
     * Load the pre-computed similarity scores.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, Double>> loadEntityTFIDFSim(String file) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;

        Map<String, Map<String, Double>> sim = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            String entity_a = data[0];
            String entity_b = data[2];
            double score = Double.parseDouble(data[3]);

            if (!sim.containsKey(entity_a)) {
                sim.put(entity_a, new HashMap<>());
            }
            sim.get(entity_a).put(entity_b, score);
        }
        return sim;
    }

    /**
     * Compute the jaccard similarity over two sets.
     *
     * @param a
     * @param b
     * @return
     */
    public static double computeJaccardSimilarity(Set<String> a, Set<String> b) {
        if (a == null || b == null || a.isEmpty() || b.isEmpty()) {
            return 0.0;
        }
        Set<String> common = new HashSet<>(a);
        common.retainAll(b);

        return common.size() / (double) (a.size() + b.size() - common.size());
    }


    /**
     * Get the entities that are associated with categories of a specific depth, entities that belong to a set of
     * directly connected categories, and parents of those categories.
     *
     * @return
     */
    public static Map<String, Map.Entry<Integer, Set<String>>> loadEntitiesByCategory(Set<String> seed_entities,
                                                                                      Set<String> filter_entities,
                                                                                      Map<String, CategoryRepresentation> cat_to_map,
                                                                                      Map<String, Set<String>> entity_cats,
                                                                                      String cat_type) throws IOException {
        Map<String, Map.Entry<Integer, Set<String>>> max_level_entities = new HashMap<>();

        System.out.println("There are " + filter_entities.size() + " filter entities.");

        for (String entity : seed_entities) {
            //an entity is directly associated to multiple categories.
            Set<String> entity_pairs = new HashSet<>();
            if (!entity_cats.containsKey(entity)) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            List<CategoryRepresentation> categories = entity_cats.get(entity).stream().filter(cat -> cat_to_map.containsKey(cat)).map(cat -> cat_to_map.get(cat)).collect(Collectors.toList());
            if (categories == null || categories.isEmpty()) {
                System.out.printf("Entity %s is missing its categories %s.\n", entity, entity_cats.get(entity));
                continue;
            }
            //retrieve the categories that belong at the same depth in the category taxonomy.
            int max_level = categories.stream().mapToInt(cat -> cat.level).max().getAsInt();
            int max_sum = categories.stream().filter(cat -> cat.level == max_level).mapToInt(cat -> cat.entities.size()).sum();

            System.out.printf("Entity %s has the maximal level %d and has %d categories with %d entities from the deepest categories.\n", entity, max_level, categories.size(), max_sum);

            if (cat_type.equals("deepest")) {
                //add all the entities that belong to the deepest category directly associated with our seed entity
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("same_level")) {
                cat_to_map.keySet().stream().filter(x -> cat_to_map.get(x).level == max_level).forEach(x -> entity_pairs.addAll(cat_to_map.get(x).entities));
            } else if (cat_type.equals("direct")) {
                categories.forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("direct_parents")) {
                categories.forEach(cat -> cat.parents.values().forEach(parent -> entity_pairs.addAll(parent.entities)));
                categories.forEach(cat -> entity_pairs.addAll(cat.entities));
            } else if (cat_type.equals("deepest_parents")) {
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> entity_pairs.addAll(cat.entities));
                categories.stream().filter(cat -> cat.level == max_level).forEach(cat -> cat.parents.values().forEach(parent -> entity_pairs.addAll(parent.entities)));
            }
            entity_pairs.retainAll(filter_entities);
            max_level_entities.put(entity, new AbstractMap.SimpleEntry<>(max_level, entity_pairs));
        }
        return max_level_entities;
    }


    /**
     * Compute the similarity of two entities, specifically their associated categories based on pre-computed embedding.
     *
     * @param ea
     * @param eb
     * @return
     */
    public static double computeAverageCategoryEmbSim(String ea, String eb, Map<String, TDoubleArrayList> emb, Map<String, Set<String>> entity_cats) {
        if (!entity_cats.containsKey(ea) || !entity_cats.containsKey(eb)) {
            return 0.0;
        }
        Set<String> cats_a = entity_cats.get(ea);
        Set<String> cats_b = entity_cats.get(eb);

        TDoubleArrayList avg_a = DataUtils.computeCategoryAverageVector(cats_a, emb);
        TDoubleArrayList avg_b = DataUtils.computeCategoryAverageVector(cats_b, emb);
        return DataUtils.computeCosineSim(avg_a, avg_b);
    }


    /**
     * Compute the embedding similarity of two category sets.
     *
     * @param cat_a
     * @param cat_b
     * @return
     */
    public static double computeAverageCategoryEmbSim(Set<String> cat_a, Set<String> cat_b, Map<String, TDoubleArrayList> emb) {
        if (cat_a == null || cat_b == null || cat_a.isEmpty() || cat_b.isEmpty()) {
            return 0.0;
        }

        TDoubleArrayList avg_a = DataUtils.computeCategoryAverageVector(cat_a, emb);
        TDoubleArrayList avg_b = DataUtils.computeCategoryAverageVector(cat_b, emb);
        return DataUtils.computeCosineSim(avg_a, avg_b);
    }


    /**
     * Compute the similarity between two pieces of text based on their average word vectors
     *
     * @param text_a
     * @param text_b
     * @return
     */
    public static double computeTextSim(String text_a, String text_b, Map<String, TDoubleArrayList> emb) {
        //generate the sets from the titles
        Set<String> entity_a_tokens = new HashSet<>(Arrays.asList(text_a.toLowerCase().replaceAll("\\W+", " ").split("\\s+")));
        Set<String> entity_b_tokens = new HashSet<>(Arrays.asList(text_b.toLowerCase().replaceAll("\\W+", " ").split("\\s+")));

        TDoubleArrayList avg_a = computeAverageWordVector(entity_a_tokens, emb);
        TDoubleArrayList avg_b = computeAverageWordVector(entity_b_tokens, emb);
        return DataUtils.computeCosineSim(avg_a, avg_b);
    }


    /**
     * Compute the RDF2Vec similarity of two entities.
     *
     * @param ea
     * @param eb
     * @return
     */
    public static double computeRDF2VecSim(String ea, String eb, Map<String, TDoubleArrayList> emb) {
        ea = ea.replaceAll(" ", "_");
        eb = eb.replaceAll(" ", "_");
        if (!emb.containsKey(ea) || !emb.containsKey(eb)) {
            return 0.0;
        }
        return DataUtils.computeCosineSim(emb.get(ea), emb.get(eb));
    }


    /**
     * Read from a matrix the similarity scores.
     *
     * @param cat_sim
     * @return
     * @throws IOException
     */
    public static Map<String, Map<String, Double>> readCategoryIDFSimData(String cat_sim) throws IOException {
        Map<String, Map<String, Double>> cat_sim_data = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(cat_sim);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");
            if (data.length != 3) {
                continue;
            }
            if (!cat_sim_data.containsKey(data[0])) {
                cat_sim_data.put(data[0], new HashMap<>());
            }

            cat_sim_data.get(data[0]).put(data[1], Double.parseDouble(data[2]));
        }

        return cat_sim_data;
    }


    /**
     * Get the average word vector for the column values.
     *
     * @param col
     * @return
     */
    public static TDoubleArrayList getAverageW2VFromColValueDist(WikiColumnHeader col, Map<String, TDoubleArrayList> word2vec) {
        Set<String> words = new HashSet<>();

        for (Object val : col.value_dist.keySet()) {
            words.addAll(Arrays.asList(val.toString().toLowerCase().replaceAll("\\W+", " ").split("\\s+")));
        }

        TDoubleArrayList avg_w2v = DataUtils.computeAverageWordVector(words, word2vec);
        return avg_w2v;
    }


    /**
     * Compute the IDF based similarity between categories.
     *
     * @param cats_a
     * @param cats_b
     * @return
     */
    public static List<Double> computeCategoryIDFSimilarity(Set<String> cats_a, Set<String> cats_b, Map<String, Map<String, Double>> cat_sim) {
        List<Double> rst = new ArrayList<>();
        if (cats_a == null || cats_a.isEmpty() || cats_b == null || cats_b.isEmpty()) {
            rst.add(-100.0);
            return rst;
        }
        for (String cat_a : cats_a) {
            if (!cat_sim.containsKey(cat_a)) {
                continue;
            }
            for (String cat_b : cats_b) {
                if (!cat_sim.get(cat_a).containsKey(cat_b) || cat_sim.get(cat_a).get(cat_b) == Double.MAX_VALUE) {
                    continue;
                }
                rst.add(cat_sim.get(cat_a).get(cat_b));
            }
        }

        if (rst.isEmpty()) {
            rst.add(-100.0);
        }
        return rst;
    }


    /**
     * For each column in  a table compute as features w.r.t other columns in a table, specifically consider for
     * features the columns that are closes in terms of column title and value distribution in the target table.
     *
     * @param scores
     * @return
     */
    public static double[][] getMaxColumnSimilarities(List<List<Object>> scores) {
        TIntDoubleHashMap title_max = new TIntDoubleHashMap();
        TIntDoubleHashMap val_max = new TIntDoubleHashMap();
        TIntIntHashMap indice_dist = new TIntIntHashMap();

        for (List<Object> score : scores) {
            int col_a = (Integer) score.get(0);
            int col_b = (Integer) score.get(1);
            double col_title_sim = (Double) score.get(2);
            double col_val_dist_sim = (Double) score.get(3);


            double avg_sim = (col_title_sim + col_val_dist_sim) / 2.0;

            if (!title_max.containsKey(col_a)) {
                title_max.put(col_a, col_title_sim);
                val_max.put(col_a, col_val_dist_sim);
                indice_dist.put(col_a, col_a - col_b);
            } else {
                double avg_tmp = (title_max.get(col_a) + val_max.get(col_a)) / 2.0;
                if (avg_sim > avg_tmp) {
                    title_max.put(col_a, col_title_sim);
                    val_max.put(col_a, col_val_dist_sim);
                    indice_dist.put(col_a, col_a - col_b);
                }
            }
        }

        double[][] results = new double[title_max.size()][];
        int[] col_a_idx = title_max.keys();

        for (int i = 0; i < col_a_idx.length; i++) {
            int col_a = col_a_idx[i];
            double title_sim = title_max.get(col_a);
            double col_val_sim = val_max.get(col_a);
            double dist = indice_dist.get(col_a);

            results[i] = new double[]{title_sim, col_val_sim, dist};
        }
        return results;
    }


    /**
     * Load the table alignments, where for each table pair identified by their IDs we have a label saying if they are
     * related or not.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static Map<Integer, Map<Integer, Boolean>> loadTableAlignmentLabels(String file) throws IOException {
        Map<Integer, Map<Integer, Boolean>> labels = new HashMap<>();
        String line;
        BufferedReader reader = FileUtils.getFileReader(file);

        int idx = 0;
        while ((line = reader.readLine()) != null) {
            if (idx == 0) {
                idx++;
                continue;
            }
            String[] data = line.split("\t");

            int tbl_id_a = Integer.parseInt(data[2]);
            int tbl_id_b = Integer.parseInt(data[3]);
            if (!labels.containsKey(tbl_id_a)) {
                labels.put(tbl_id_a, new HashMap<>());
            }
            boolean label = data[4].toLowerCase().trim().equals("equivalent") || data[4].toLowerCase().trim().equals("subpartof");
            labels.get(tbl_id_a).put(tbl_id_b, label);
        }
        return labels;
    }
}
