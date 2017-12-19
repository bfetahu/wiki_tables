package test;

import io.FileUtils;
import representation.CategoryRepresentation;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String file = "/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv.gz";

        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(file);

        StringBuffer sb = new StringBuffer();
        cat.printCategories("/Users/besnik/Desktop/wiki_tables/wiki_cat_taxonomy.tsv", sb);
        FileUtils.saveText(sb.toString(), "/Users/besnik/Desktop/wiki_tables/wiki_cat_taxonomy.tsv", true);
        sb.delete(0, sb.length());

        Map<String, CategoryRepresentation> cats = new HashMap<>();
        cat.loadIntoMapChildCats(cats);

        String root_cats = cat.children.keySet().toString().replaceAll(", ", "\n");
        root_cats = root_cats.substring(1, root_cats.length() - 2);
        FileUtils.saveText(root_cats, "/Users/besnik/Desktop/wiki_tables/collapsed_categories_root.txt");
    }

}
