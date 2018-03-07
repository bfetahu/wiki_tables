package utils;

import cern.colt.matrix.tdouble.DoubleMatrix2D;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;
import org.json.JSONObject;
import org.ojalgo.matrix.store.SparseStore;
import representation.CategoryRepresentation;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

            double weight = cat.level / min_level_property.get(prop) * num_values / num_assignments;
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
            result += Math.pow(2, val_a - val_b);
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
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @param b
     * @param col_dim provides the column dimensions of the target matrix
     * @param row_dim provides the column dimensions of the target matrix
     * @return
     */
    public static TIntObjectHashMap<TIntDoubleHashMap> multiply(TIntObjectHashMap<TIntDoubleHashMap> a,
                                                                TIntObjectHashMap<TIntDoubleHashMap> b,
                                                                int row_dim, int col_dim) {
        TIntObjectHashMap<TIntDoubleHashMap> c = new TIntObjectHashMap<>();

        Arrays.stream(a.keys()).forEach(row_idx -> {
            TIntDoubleHashMap c_row = new TIntDoubleHashMap();
            TIntDoubleHashMap row = a.get(row_idx);
            for (int col_idx = 0; col_idx < col_dim; col_idx++) {
                TIntDoubleHashMap col = b.get(col_idx);
                double sum = multiplyRowCol(row, col, row_dim);
                c_row.put(col_idx, sum);
            }
            c.put(row_idx, c_row);
        });

        return c;
    }

    /**
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @param b
     * @return
     */
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> multiplyParallel(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a,
                                                                                                  ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> b) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> c = new ConcurrentHashMap<>();

        a.keySet().parallelStream().forEach(row_idx -> {
            ConcurrentHashMap<Integer, Double> c_row = new ConcurrentHashMap<>();
            c.put(row_idx, c_row);

            ConcurrentHashMap<Integer, Double> row = a.get(row_idx);
            b.keySet().parallelStream().forEach(col_idx -> {
                ConcurrentHashMap<Integer, Double> col = b.get(col_idx);
                double sum = multiplyRowCol(row, col);
                c_row.put(col_idx, sum);
            });
        });

        return c;
    }

    /**
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @param b
     * @param row_dim provides the column dimensions of the target matrix
     * @return
     */
    public static TIntDoubleHashMap multiply(TIntObjectHashMap<TIntDoubleHashMap> a,
                                             TIntDoubleHashMap b,
                                             int row_dim) {
        TIntDoubleHashMap c = new TIntDoubleHashMap();

        Arrays.stream(a.keys()).forEach(row_idx -> {
            TIntDoubleHashMap row = a.get(row_idx);
            double sum = multiplyRowCol(row, b, row_dim);
            c.put(row_idx, sum);
        });

        return c;
    }

    /**
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @param b
     * @return
     */
    public static ConcurrentHashMap<Integer, Double> multiplyMatrixVectorParallel(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a,
                                                                                  ConcurrentHashMap<Integer, Double> b) {
        ConcurrentHashMap<Integer, Double> c = new ConcurrentHashMap<>();
        a.keySet().parallelStream().forEach(row_idx -> {
            ConcurrentHashMap<Integer, Double> row = a.get(row_idx);
            double sum = multiplyRowCol(row, b);
            c.put(row_idx, sum);
        });

        return c;
    }

    /**
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @return
     */
    public static TIntObjectHashMap<TIntDoubleHashMap> multiply(TIntObjectHashMap<TIntDoubleHashMap> a,
                                                                double scalar) {
        TIntObjectHashMap<TIntDoubleHashMap> r = new TIntObjectHashMap<>();
        Arrays.stream(a.keys()).forEach(row_idx -> {
            if (!r.containsKey(row_idx)) {
                r.put(row_idx, new TIntDoubleHashMap());
            }
            for (int col_dix : a.get(row_idx).keys()) {
                double val = a.get(row_idx).get(col_dix) * scalar;
                r.get(row_idx).put(col_dix, val);
            }
        });

        return r;
    }


    /***
     * Multiply matrices on row by column technique.
     * @param row
     * @param col
     * @return
     */
    public static double multiplyRowCol(TIntDoubleHashMap row, TIntDoubleHashMap col, int n) {
        double sum = 0;
        for (int i = 0; i < n; i++) {
            if (!row.containsKey(i) || !col.containsKey(i)) {
                continue;
            }
            sum += row.get(i) * col.get(i);
        }
        return sum;
    }

    /**
     * Convert a row based indexing of a matrix into a column indexed one.
     *
     * @param a
     * @return
     */
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> columnIndexParallel(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> b = new ConcurrentHashMap<>();

        a.keySet().parallelStream().forEach(row_idx -> {
            a.get(row_idx).keySet().forEach(col_idx -> {
                double value = a.get(row_idx).get(col_idx);

                if (!b.containsKey(col_idx)) {
                    b.put(col_idx, new ConcurrentHashMap<>());
                }
                b.get(col_idx).put(row_idx, value);
            });
        });

        return b;
    }


    /**
     * Multiply matrices which are represented in terms of map data structures. In this way we have a compressed form
     * and also we can multiply in parallel.
     *
     * @param a
     * @return
     */
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> multiplyParallel(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a,
                                                                                                  double scalar) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> r = new ConcurrentHashMap<>();
        a.keySet().parallelStream().forEach(row_idx -> {
            ConcurrentHashMap<Integer, Double> new_row = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Double> row = a.get(row_idx);
            r.put(row_idx, new_row);

            row.keySet().forEach(col_idx -> {
                double val = row.get(col_idx) * scalar;
                new_row.put(col_idx, val);
            });
        });

        return r;
    }


    /***
     * Multiply matrices on row by column technique.
     * @param row
     * @param col
     * @return
     */
    public static double multiplyRowCol(ConcurrentHashMap<Integer, Double> row, ConcurrentHashMap<Integer, Double> col) {
        double sum = 0;
        TIntHashSet all = new TIntHashSet(row.keySet());
        all.retainAll(col.keySet());
        for (int i : all.toArray()) {
            sum += row.get(i) * col.get(i);
        }
        return sum;
    }

    /**
     * Convert a row based indexing of a matrix into a column indexed one.
     *
     * @param a
     * @return
     */
    public static TIntObjectHashMap<TIntDoubleHashMap> columnIndex(TIntObjectHashMap<TIntDoubleHashMap> a) {
        TIntObjectHashMap<TIntDoubleHashMap> b = new TIntObjectHashMap<>();

        for (int row_idx : a.keys()) {
            Arrays.stream(a.get(row_idx).keys()).forEach(col_idx -> {
                double value = a.get(row_idx).get(col_idx);

                if (!b.containsKey(col_idx)) {
                    b.put(col_idx, new TIntDoubleHashMap());
                }
                b.get(col_idx).put(row_idx, value);
            });
        }

        return b;
    }


    public static void loadMatrix(TIntObjectHashMap<TIntDoubleHashMap> sparse_list, SparseStore<Double> matrix) {
        Arrays.stream(sparse_list.keys()).forEach(row_idx -> {
            Arrays.stream(sparse_list.get(row_idx).keys()).forEach(col_idx -> {
                double value = sparse_list.get(row_idx).get(col_idx);
                try {
                    matrix.set(row_idx, col_idx, value);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.printf("%d=%d \tMatrix has dim %dx%d.\n", row_idx, col_idx, matrix.countRows(), matrix.countColumns());

                }
            });
        });
    }

    public static void loadMatrix(TIntObjectHashMap<TIntDoubleHashMap> sparse_list, DoubleMatrix2D matrix) {
        Arrays.stream(sparse_list.keys()).forEach(row_idx -> {
            Arrays.stream(sparse_list.get(row_idx).keys()).forEach(col_idx -> {
                double value = sparse_list.get(row_idx).get(col_idx);
                matrix.set(row_idx, col_idx, value);
            });
        });
    }

    /**
     * Add vector b to vector a.
     *
     * @param a
     * @param b
     * @return
     */
    public static TIntDoubleHashMap add(TIntDoubleHashMap a, TIntDoubleHashMap b) {
        for (int key : b.keys()) {
            double val = b.get(key);

            if (!a.containsKey(key)) {
                a.put(key, val);
            } else {
                a.put(key, a.get(key) + val);
            }
        }
        return a;
    }

    /**
     * Add vector b to vector a.
     *
     * @param a
     * @param b
     * @return
     */
    public static ConcurrentHashMap<Integer, Double> addParallel(ConcurrentHashMap<Integer, Double> a, ConcurrentHashMap<Integer, Double> b) {
        b.keySet().parallelStream().forEach(key -> {
            double val = b.get(key);

            if (!a.containsKey(key)) {
                a.put(key, val);
            } else {
                a.put(key, a.get(key) + val);
            }

        });
        return a;
    }

    /**
     * Print a sparse matrix which is stored into a map data structure.
     *
     * @param a
     */
    public static void printMatrix(TIntObjectHashMap<TIntDoubleHashMap> a) {
        for (int row_id : a.keys()) {
            for (int col_id : a.get(row_id).keys()) {
                System.out.printf("<%d, %d>=%.2f\t", row_id, col_id, a.get(row_id).get(col_id));
            }
            System.out.println();
        }
        System.out.println();
    }

    /**
     * Print a sparse matrix which is stored into a map data structure.
     *
     * @param a
     */
    public static void printMatrix(TIntDoubleHashMap a) {
        for (int row_id : a.keys()) {
            System.out.printf("%d=%.2f\n", row_id, a.get(row_id));
        }
        System.out.println();
    }


    /**
     * Print a sparse matrix which is stored into a map data structure.
     *
     * @param a
     */
    public static void printMatrix(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a) {
        for (int row_id : a.keySet()) {
            for (int col_id : a.get(row_id).keySet()) {
                System.out.printf("<%d, %d>=%.2f\t", row_id, col_id, a.get(row_id).get(col_id));
            }
            System.out.println();
        }
        System.out.println();
    }

    /**
     * Print a sparse matrix which is stored into a map data structure.
     *
     * @param a
     */
    public static void printMatrixVector(ConcurrentHashMap<Integer, Double> a) {
        for (int row_id : a.keySet()) {
            System.out.printf("%d=%.2f\n", row_id, a.get(row_id));
        }
        System.out.println();
    }

    /**
     * Transposes a matrix.
     *
     * @param a
     * @return
     */
    public static TIntObjectHashMap<TIntDoubleHashMap> transposeMatrix(TIntObjectHashMap<TIntDoubleHashMap> a) {
        TIntObjectHashMap<TIntDoubleHashMap> at = new TIntObjectHashMap<>();

        for (int i : a.keys()) {
            for (int j : a.get(i).keys()) {
                double val = a.get(i).get(j);

                if (!at.containsKey(j)) {
                    at.put(j, new TIntDoubleHashMap());
                }
                at.get(j).put(i, val);
            }
        }

        return at;
    }

    /**
     * Transposes a matrix.
     *
     * @param a
     * @return
     */
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> transposeMatrixParallel(ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> a) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Double>> at = new ConcurrentHashMap<>();

        a.keySet().forEach(i -> {
            a.get(i).keySet().forEach(j -> {
                double val = a.get(i).get(j);

                if (!at.containsKey(j)) {
                    at.put(j, new ConcurrentHashMap<>());
                }
                at.get(j).put(i, val);
            });
        });

        return at;
    }
}
