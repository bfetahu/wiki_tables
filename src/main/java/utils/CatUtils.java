package utils;

import evaluation.BaselineCandidatePairStrategies;
import io.FileUtils;
import representation.CategoryRepresentation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 3/11/18.
 */
public class CatUtils {
    public static void main(String[] args) {
        Set<String> seed_entities = FileUtils.readIntoSet(args[0], "\n", false);
        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(args[1]);

        Map<String, Set<String>> cat_entity = new HashMap<>();
        BaselineCandidatePairStrategies.traverseEntityCats(seed_entities, cat, cat_entity);

        //store the traversed categories
        StringBuffer sb = new StringBuffer();
        for (String entity : cat_entity.keySet()) {
            for (String category : cat_entity.get(entity)) {
                sb.append(entity).append("\t").append(category).append("\n");

                if (sb.length() > 10000) {
                    FileUtils.saveText(sb.toString(), "entity_category_traversed.tsv", true);
                    sb.delete(0, sb.length());
                }
            }
        }
        FileUtils.saveText(sb.toString(), "entity_category_traversed.tsv", true);
    }
}
