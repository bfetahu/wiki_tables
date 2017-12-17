package test;

import datastruct.TableCandidateFeatures;
import io.FileUtils;

import java.util.List;

/**
 * Created by besnik on 6/6/17.
 */
public class Test {
    public static void main(String[] args) throws Exception {
//        String file = "/Users/besnik/Desktop/wiki_tables/wiki_cats_201708.tsv.gz";

//        CategoryRepresentation cat = CategoryRepresentation.readCategoryGraph(file);
//        Map<String, CategoryRepresentation> cats = new HashMap<>();
//        cat.loadIntoMapChildCats(cats);

        String candidate_file = "/Users/besnik/Desktop/wiki_tables/candidates/151750.gz";

        List<TableCandidateFeatures> candidates = (List<TableCandidateFeatures>) FileUtils.readObject(candidate_file);
        for (TableCandidateFeatures tbl : candidates) {
            System.out.println(tbl.getArticleA() + "\t" + tbl.getArticleB());
            tbl.lowest_common_ancestors.forEach(System.out::println);
        }
    }

}
