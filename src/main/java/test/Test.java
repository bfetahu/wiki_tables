package test;

import evaluation.BaselineCandidatePairStrategies;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import io.FileUtils;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by besnik on 3/12/18.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Map<String, TDoubleArrayList> node2vec = DataUtils.loadWord2Vec(args[0]);
        Set<String> seeds = FileUtils.readIntoSet(args[1], "\n", false);
        Set<String> filter = FileUtils.readIntoSet(args[2], "\n", false);

        StringBuffer sb = new StringBuffer();
        DecimalFormat df = new DecimalFormat("#.0");
        for (String entity_a : seeds) {
            TDoubleArrayList emb_a = node2vec.get(entity_a.replaceAll(" ", "_"));
            for (String entity_b : filter) {
                TDoubleArrayList emb_b = node2vec.get(entity_b.replaceAll(" ", "_"));
                double score = Double.parseDouble(df.format(DataUtils.computeCosineSim(emb_a, emb_b)));
            }
        }

    }

    public static void computeSentenceSimSimple(String[] args) throws IOException {
        Map<String, String> ea = DataUtils.loadEntityAbstracts(args[0]);
        Map<String, TIntDoubleHashMap> tfidfscores = DataUtils.computeTFIDF(ea);

        Set<String> seeds = FileUtils.readIntoSet(args[1], "\n", false);
        Set<String> filter = FileUtils.readIntoSet(args[2], "\n", false);
        Map<String, Set<String>> gt = FileUtils.readMapSet(args[3], "\t");

        String out_file = args[4];
        for (String entity : seeds) {
            if (!ea.containsKey(entity)) {
                System.out.printf("There is no abstract for entity %s.\n", entity);
                continue;
            }
            List<String> feature_lines = new ArrayList<>();
            List<String> concurrent_fl = Collections.synchronizedList(feature_lines);

            int gt_total = gt.containsKey(entity) ? gt.get(entity).size() : 0;
            filter.parallelStream().forEach(entity_candidate -> {
                if (!ea.containsKey(entity_candidate)) {
                    System.out.printf("There is no abstract for entity %s.\n", entity_candidate);
                    return;
                }
                double score = DataUtils.computeCosine(tfidfscores.get(entity), tfidfscores.get(entity_candidate));
                //create for each of these pairs the features
                boolean label = gt.containsKey(entity) && gt.get(entity).contains(entity_candidate);

                StringBuffer sb = new StringBuffer();
                sb.append(entity).append("\t").append(gt_total).append("\t").append(entity_candidate).append("\t").append(score).append("\t").append(label).append("\n");
                concurrent_fl.add(sb.toString());
            });

            StringBuffer sb = new StringBuffer();
            concurrent_fl.forEach(s -> sb.append(s));
            FileUtils.saveText(sb.toString(), out_file, true);
        }
    }


    public static void computeSentenceSim(String[] args) throws IOException {
        Map<String, TDoubleArrayList> w2v = DataUtils.loadWord2Vec(args[0]);
        Map<String, String> ea = DataUtils.loadEntityAbstracts(args[1]);

        Set<String> seeds = FileUtils.readIntoSet(args[2], "\n", false);
        Set<String> filter = FileUtils.readIntoSet(args[3], "\n", false);
        Map<String, Set<String>> gt = FileUtils.readMapSet(args[4], "\t");

        Map<String, Map<Integer, TDoubleArrayList>> avg_wsv = new HashMap<>();
        filter.stream().filter(e -> ea.containsKey(e)).forEach(e -> avg_wsv.put(e, computeAvgSentenceW2V(ea.get(e), w2v)));

        String out_file = "entity_abs_sim.tsv";
        for (String entity : seeds) {
            if (!avg_wsv.containsKey(entity)) {
                System.out.printf("There is no abstract for entity %s.\n", entity);
                continue;
            }
            List<String> feature_lines = new ArrayList<>();
            List<String> concurrent_fl = Collections.synchronizedList(feature_lines);

            int gt_total = gt.containsKey(entity) ? gt.get(entity).size() : 0;
            filter.parallelStream().forEach(entity_candidate -> {
                if (!avg_wsv.containsKey(entity_candidate)) {
                    System.out.printf("There is no abstract for entity %s.\n", entity_candidate);
                    return;
                }
                Map<Integer, Map.Entry<Integer, Double>> sim = computeSentenceSim(avg_wsv.get(entity), avg_wsv.get(entity_candidate));
                //create for each of these pairs the features
                boolean label = gt.containsKey(entity) && gt.get(entity).contains(entity_candidate);

                StringBuffer sb = new StringBuffer();
                sb.append(entity).append("\t").append(gt_total).append("\t").append(entity_candidate);
                sim.keySet().stream().filter(k -> k <= 10).forEach(k -> sb.append("\t").append(sim.get(k).getKey()).append("\t").append(sim.get(k).getValue()));
                sb.append("\t").append(label).append("\n");
                concurrent_fl.add(sb.toString());
            });

            StringBuffer sb = new StringBuffer();
            concurrent_fl.forEach(s -> sb.append(s));
            FileUtils.saveText(sb.toString(), out_file, true);
        }


    }

    public static void computeCoverage(String file, String outfile, Map<String, Set<String>> gt, int field_index) throws IOException {
        Map<String, Map<Double, Set<String>>> entities = new HashMap<>();
        BufferedReader reader = FileUtils.getFileReader(file);

        DecimalFormat df = new DecimalFormat("#.0");
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\t");

            String seed_entity = tmp[0];
            String candidate = tmp[2];

            if (!entities.containsKey(seed_entity)) {
                entities.put(seed_entity, new TreeMap<>());
            }

            double score = Double.parseDouble(df.format(Double.parseDouble(tmp[field_index])));
            if (!entities.get(seed_entity).containsKey(score)) {
                entities.get(seed_entity).put(score, new HashSet<>());
            }
            entities.get(seed_entity).get(score).add(candidate);
        }

        FileUtils.saveText("entity\tgt_total\tsim\tall_candidates\toverlap\tunaligned_entities\taligned_ratio\tunaligned_ratio\n", outfile);

        for (String entity : entities.keySet()) {
            Map<Double, Map.Entry<Integer, Integer>> cumm_entities = new TreeMap<>();
            int gt_total = gt.containsKey(entity) ? gt.get(entity).size() : 0;
            Set<String> gt_entities = gt.containsKey(entity) ? gt.get(entity) : new HashSet<>();
            for (double score : entities.get(entity).keySet()) {
                Set<String> cumm = new HashSet<>();
                entities.get(entity).keySet().stream().filter(score_cmp -> score_cmp >= score).forEach(score_cmp -> cumm.addAll(entities.get(entity).get(score_cmp)));

                int cumm_size = cumm.size();
                cumm.retainAll(gt_entities);
                int coverage = cumm.size();
                cumm_entities.put(score, new AbstractMap.SimpleEntry<>(cumm_size, coverage));
            }

            //output the data
            String coverage_stats = BaselineCandidatePairStrategies.writeCoverageStats(cumm_entities, gt_total, entity);
            FileUtils.saveText(coverage_stats, outfile, true);
        }
    }

    public static Map<Integer, TDoubleArrayList> computeAvgSentenceW2V(String text, Map<String, TDoubleArrayList> w2v) {
        String[] sentences = text.split("\\.\\s+");
        Map<Integer, TDoubleArrayList> avg_w2v_s = new TreeMap<>();

        for (int i = 0; i < sentences.length; i++) {
            avg_w2v_s.put(i, DataUtils.computeAverageWordVector(sentences[i], w2v));
        }

        return avg_w2v_s;
    }

    public static Map<Integer, Map.Entry<Integer, Double>> computeSentenceSim(Map<Integer, TDoubleArrayList> avg_w2vs_a, Map<Integer, TDoubleArrayList> avg_w2vs_b) {
        Map<Integer, Map.Entry<Integer, Double>> sent_sim = new TreeMap<>();
        for (int i : avg_w2vs_a.keySet()) {
            TDoubleArrayList avg_a = avg_w2vs_a.get(i);
            double max_sim = 0.0;
            int max_idx = -1;
            for (int j : avg_w2vs_b.keySet()) {
                TDoubleArrayList avg_b = avg_w2vs_b.get(j);
                double score = DataUtils.computeCosineSim(avg_a, avg_b);

                if (score > max_sim) {
                    max_sim = score;
                    max_idx = j;
                }
            }

            sent_sim.put(i, new AbstractMap.SimpleEntry<>(max_idx, max_sim));
        }
        return sent_sim;
    }

}
