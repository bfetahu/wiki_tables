package test;

import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.IOException;
import java.util.Map;

/**
 * Created by besnik on 3/4/18.
 */
public class CatRepSim {
    public static void main(String[] args) throws IOException {

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
            System.out.printf("Computing the category representation similarity for %finished_gt_seeds\n", cat.label);

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
