package test;

import representation.CategoryHierarchy;
import utils.FileUtils;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        CategoryHierarchy cat = (CategoryHierarchy) FileUtils.readObject("/Users/besnik/Desktop/dbpedia/out/category_hierarchy_representation.obj");
        System.out.println(cat.children.size());
    }
}
