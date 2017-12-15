package test;

import representation.CategoryRepresentation;
import utils.DataUtils;
import utils.FileUtils;

import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String category_path = args[0];
        String seed_path = args[1];
        String entity_categories_path = args[2];

        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(category_path);
        Set<String> seeds = FileUtils.readIntoSet(seed_path, "\n", false);

        Map<String, Set<String>> entity_categories = DataUtils.readCategoryMappingsWiki(entity_categories_path, seeds);
        System.out.println("Finished reading category to article mappings for " + entity_categories.size() + " entities.");

        //set num entities for each category.
        DataUtils.updateCatsWithEntities(cat, entity_categories);

        System.out.println(cat.entities.size());
    }
}
