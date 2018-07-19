package evaluation;

import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import utils.DataUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 6/24/18.
 */
public class ArticleCandidatesTFIDF {
    //keep the entity abstracts
    public static Map<String, String> entity_abstracts = new HashMap<>();

    // and the entities which contain a table (filter_entities)
    public static Set<String> seed_entities = new HashSet<>();
    public static Set<String> filter_entities = new HashSet<>();
    public static Map<String, Set<String>> gt_entities = new HashMap<>();

    public static Set<String> stop_words;

    public static void main(String[] args) throws IOException {
        String out = "";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-out")) {
                out = args[++i];
            } else if (args[i].equals("-seed_entities")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-gt_pairs")) {
                gt_entities = FileUtils.readMapSet(args[++i], "\t");
            } else if (args[i].equals("-abstracts")) {
                entity_abstracts = DataUtils.loadEntityAbstracts(args[++i], filter_entities);
            } else if (args[i].equals("-stop_words")) {
                stop_words = FileUtils.readIntoSet(args[++i], "\n", false);
            }
        }

        System.out.println("Started computing the TF-IDF scores");
        //compute the TF-IDF scores
        Map<String, TIntDoubleHashMap> tfidf = DataUtils.computeTFIDF(entity_abstracts, stop_words);

        //compute the cosine similarity between each of the entity pairs.
        StringBuffer sb = new StringBuffer();
        for (String ea : seed_entities) {
            TIntDoubleHashMap ea_tfidf_vec = tfidf.get(ea);
            for (String eb : filter_entities) {
                TIntDoubleHashMap eb_tfidf_vec = tfidf.get(eb);
                double score = DataUtils.computeCosine(ea_tfidf_vec, eb_tfidf_vec);
                boolean label = gt_entities.containsKey(ea) && gt_entities.get(ea).contains(eb);
                sb.append(ea).append("\t").append(eb).append("\t").append(score).append("\t").append(label).append("\n");

                if (sb.length() > 10000) {
                    FileUtils.saveText(sb.toString(), out, true);
                    sb.delete(0, sb.length());
                }
            }
        }
        FileUtils.saveText(sb.toString(), out, true);
    }
}
