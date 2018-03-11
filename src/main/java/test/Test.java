package test;

import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by besnik on 3/4/18.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        String category_path = args[0];
        String out_dir = args[1];
        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(category_path);

        Map<String, CategoryRepresentation> cat_to_map = new HashMap<>();
        cat.loadIntoMapChildCats(cat_to_map);

        Map<String, Double> attribute_max_level_category = DataUtils.computeMaxLevelAttributeCategory(cat_to_map);
        Map<String, TIntDoubleHashMap> cat_weights = DataUtils.computeCategoryAttributeWeights(attribute_max_level_category, cat_to_map);

        //compute the similarity between categories under the same parent and categories against their parents.
        String outfile = out_dir + "/category_rep_sim.tsv";
        computeCategorySimilarity(cat, cat_weights, outfile);
    }

    public static void testCatSim(String file, String cat_level) throws IOException {
        Map<String, Integer> catl = loadCatLevel(cat_level);

        Set<String> cats = new HashSet<>();
        cats.add("Wars involving Austria");
        cats.add("Social networks");
        cats.add("Peer-to-peer");
        cats.add("LGBT social networking services");
        cats.add("root");

        Map<String, Map<String, Map.Entry<Integer, Integer>>> cat_rep = new HashMap<>();
        Map<String, Integer> prop_level = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            String cat = data[0];
            int level = catl.containsKey(cat) ? catl.get(cat) : 1;
            for (int i = 3; i < data.length; i++) {
                String prop = data[i];
                prop = prop.substring(0, prop.indexOf("="));

                if (!prop_level.containsKey(prop)) {
                    prop_level.put(prop, level);
                } else if (prop_level.get(prop) < level) {
                    prop_level.put(prop, level);
                }
            }

            if (cats.contains(cat)) {
                Map<String, Map.Entry<Integer, Integer>> catprops = new HashMap<>();
                for (int i = 3; i < data.length; i++) {
                    String prop = data[i];
                    prop = prop.substring(0, prop.indexOf("="));

                    int numval = Integer.parseInt(data[i].substring(data[i].indexOf("=") + 1, data[i].indexOf(";")));
                    int numassign = Integer.parseInt(data[i].substring(data[i].indexOf(";") + 1));

                    catprops.put(prop, new AbstractMap.SimpleEntry<>(numval, numassign));
                }
                cat_rep.put(cat, catprops);
            }
        }

        Map<String, TIntDoubleHashMap> weights = new HashMap<>();
        for (String category : cat_rep.keySet()) {
            TIntDoubleHashMap catweight = new TIntDoubleHashMap();

            for (String prop : cat_rep.get(category).keySet()) {
                double weight = (double)catl.get(category) / prop_level.get(prop) * cat_rep.get(category).get(prop).getKey() / (double) cat_rep.get(category).get(prop).getValue();
                catweight.put(prop.hashCode(), weight);
            }
            weights.put(category, catweight);
        }

        Set<String> keys = weights.keySet();
        for (String a : keys) {
            for (String b : keys) {
                double score = DataUtils.computeEuclideanDistance(weights.get(a), weights.get(b));
                System.out.printf("%s\t%s\t%.2f\n", a, b, score);
            }
        }
    }

    private static Map<String, Integer> loadCatLevel(String file) throws IOException {
        Map<String, Integer> l = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            String cat = line.trim();
            cat = cat.substring(0, cat.indexOf("="));
            int level = data.length;

            l.put(cat, level);
        }

        return l;
    }


    /**
     * Compute the category similarity between categories and their parents, and categories under the same parent.
     *
     * @param cat
     * @param cat_weights
     * @param outfile
     */
    public static void computeCategorySimilarity(CategoryRepresentation cat, Map<String, TIntDoubleHashMap> cat_weights, String outfile) {
        TIntDoubleHashMap cat_weight = cat_weights.get(cat.label);
        if (!cat.children.isEmpty()) {
            System.out.printf("Computing the category representation similarity for %s\n", cat.label);

            int i = 0;
            for (String child_label : cat.children.keySet()) {
                StringBuffer sb = new StringBuffer();
                i++;
                CategoryRepresentation child = cat.children.get(child_label);

                TIntDoubleHashMap child_cat_weight = cat_weights.get(child_label);
                double euclidean_sim = DataUtils.computeEuclideanDistance(cat_weight, child_cat_weight);
                sb.append(cat.label).append("\t").append(child_label).append("\tPC\t").append(cat.level).append("\t").append(child.level).append("\t").append(euclidean_sim).append("\n");

                //compute the similarity amongst all the children
                int j = 0;
                for (String child_sibling_label : cat.children.keySet()) {
                    j++;
                    if (j <= i) {
                        continue;
                    }
                    CategoryRepresentation child_sibling = cat.children.get(child_sibling_label);

                    TIntDoubleHashMap child_sibling_weight = cat_weights.get(child_sibling_label);
                    euclidean_sim = DataUtils.computeEuclideanDistance(child_cat_weight, child_sibling_weight);
                    sb.append(child_label).append("\t").append(child_sibling_label).append("\tCC\t").append(child.level).append("\t").append(child_sibling.level).append("\t").append(euclidean_sim).append("\n");
                }

                //write the output
                FileUtils.saveText(sb.toString(), outfile, true);
                computeCategorySimilarity(child, cat_weights, outfile);
            }
        }
    }
}
