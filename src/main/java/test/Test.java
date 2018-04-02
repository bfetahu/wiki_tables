package test;

import io.FileUtils;

import java.util.*;

/**
 * Created by besnik on 3/12/18.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Double[] scores = {0.1, 0.3, 0.2, 0.6, 0.7};
        Arrays.sort(scores);

        for (double s : scores) {
            System.out.println(s);
        }


    }

    private static void cummulativeCoverage() {
        String[] lines = FileUtils.readText("/Users/besnik/Documents/L3S/wiki_tables/data/evaluation/coverage/coverage_experiments/entity_embedding_coverage.tsv").split("\n");
        Map<String, Map<Double, Map.Entry<Double, Integer>>> entity_overlap = new HashMap<>();

        for (String line : lines) {
            String[] data = line.split("\t");
            if (data[1].equals("0")) {
                continue;
            }

            String entity = data[0];
            double score = Double.parseDouble(data[2]);
            double overlap = Double.parseDouble(data[6]);
            int candidates = Integer.parseInt(data[3]);

            if (!entity_overlap.containsKey(entity)) {
                entity_overlap.put(entity, new TreeMap<>());
            }
            entity_overlap.get(entity).put(score, new AbstractMap.SimpleEntry<>(overlap, candidates));
        }

        double[] cutoffs = new double[]{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

        for (String entity : entity_overlap.keySet()) {
            for (double cutoff : cutoffs) {
                double coverage = entity_overlap.get(entity).entrySet().stream().filter(s -> s.getKey() >= cutoff).mapToDouble(s -> s.getValue().getKey()).sum();
                int candidates = entity_overlap.get(entity).entrySet().stream().filter(s -> s.getKey() >= cutoff).mapToInt(s -> s.getValue().getValue()).sum();

                if (coverage != 0) {
                    System.out.printf("%s\t%.2f\t%.2f\t%d\n", entity, cutoff, coverage, candidates);
                }
            }
        }
    }
}
