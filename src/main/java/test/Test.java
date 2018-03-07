package test;

import evaluation.CandidatePairStrategies;
import io.FileUtils;

import java.io.IOException;

/**
 * Created by besnik on 3/4/18.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        String base_dir = "/Users/besnik/Desktop/";
        CandidatePairStrategies.seed_entities = FileUtils.readIntoSet(base_dir + "/sample_of_50_entities.tsv", "\n", false);
        CandidatePairStrategies.gt_pairs = FileUtils.readMapSet(base_dir +  "gt_entity_pairs.tsv", "\t");
        CandidatePairStrategies.computeSimRankGraphSimple(0, 10, "", "", base_dir, base_dir + "/wiki_article_index.tsv.gz");
    }
}
