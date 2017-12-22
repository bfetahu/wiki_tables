package test;

import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String category_path = "/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv.gz";
        String article_file = "/Users/besnik/Desktop/wiki_tables/article_cats_201708.tsv.gz";
        Set<String> seed_entities = FileUtils.readIntoSet("/Users/besnik/Desktop/wiki_tables/seed_entities.txt", "\n", false);

        Map<String, Set<String>> cats_entities = DataUtils.readCategoryMappingsWiki(article_file, seed_entities);
        Map<String, Set<String>> entity_cats = DataUtils.getArticleCategories(cats_entities);

        CategoryRepresentation root = CategoryRepresentation.readCategoryGraph(category_path);
        Map<String, CategoryRepresentation> cats = DataUtils.updateCatsWithEntities(root, cats_entities);

        System.out.println(cats.get(args[1]).node_id);
    }

}
