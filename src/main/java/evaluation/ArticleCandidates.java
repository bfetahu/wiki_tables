package evaluation;

import datastruct.wikitable.WikiColumnHeader;
import datastruct.wikitable.WikiTable;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;
import representation.CategoryRepresentation;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Compute the relatedness between Wikipedia articles based on their category representation and other features we consider
 * in our approach for generating candidates.
 * Created by besnik on 12/6/17.
 */
public class ArticleCandidates {
    //the entity dictionaries which include the entities for which we have the ground-truth (seed_entities, gt_entities),
    // and the entities which contain a table (filter_entities)
    public static Set<String> seed_entities = new HashSet<>();
    public static Set<String> filter_entities = new HashSet<>();
    public static Map<String, Set<String>> gt_entities = new HashMap<>();
    public static Map<Integer, Map<Integer, Boolean>> gt_table_entities = new HashMap<>();

    //the entity categories
    public static Map<String, Set<String>> entity_cats = new HashMap<>();
    public static Map<String, Set<String>> cats_entities = new HashMap<>();

    //load the word  and graph embeddings.
    public static Map<String, TDoubleArrayList> word2vec;
    public static Map<String, TDoubleArrayList> rdf2vec;
    public static Map<String, TDoubleArrayList> verse;
    public static Map<String, TDoubleArrayList> doc2vec;

    public static Map<String, Map<String, Double>> tfidf;
    public static Map<String, Map<String, Double>> mw;
    public static Map<String, Set<String>> types;

    //load the wikipedia tables
    public Map<String, List<WikiTable>> tables;

    //keep the entity abstracts
    public static Map<String, String> entity_abstracts = new HashMap<>();

    //load the list of stop words
    public static Set<String> stop_words;

    //load the category representation object
    public static CategoryRepresentation cat;
    public static Map<String, CategoryRepresentation> cat_map;
    public static Map<String, TIntHashSet> cat_parents;
    public static Map<Integer, String> cat_index;
    public static Map<String, TIntHashSet> entity_cat_parents;

    public static void main(String[] args) throws IOException, InterruptedException {
        String out_dir = "", table_data = "", option = "", cat_sim = "", col_key = "", emb_key = "";
        boolean isAppend = true;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-article_categories")) {
                String article_cats = args[++i];
                entity_cats = DataUtils.readEntityCategoryMappingsWiki(article_cats, null);
                cats_entities = DataUtils.readCategoryMappingsWiki(article_cats, null);
            } else if (args[i].equals("-seed_entities")) {
                seed_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-gt_pairs")) {
                gt_entities = FileUtils.readMapSet(args[++i], "\t");
            } else if (args[i].equals("-gt_table_alignment")) {
                gt_table_entities = DataUtils.loadTableAlignmentLabels(args[++i]);
            } else if (args[i].equals("-table_data")) {
                table_data = args[++i];
            } else if (args[i].equals("-word2vec")) {
                word2vec = DataUtils.loadWord2Vec(args[++i]);
            } else if (args[i].equals("-rdf2vec")) {
                rdf2vec = DataUtils.loadWord2Vec(args[++i]);
            } else if (args[i].equals("-verse")) {
                verse = DataUtils.loadWord2Vec(args[++i]);
            } else if (args[i].equals("-doc2vec")) {
                doc2vec = DataUtils.loadWord2Vec(args[++i]);
            } else if (args[i].equals("-abstracts")) {
                entity_abstracts = DataUtils.loadEntityAbstracts(args[++i], filter_entities);
            } else if (args[i].equals("-stop_words")) {
                stop_words = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-option")) {
                option = args[++i];
            } else if (args[i].equals("-cat_sim")) {
                cat_sim = args[++i];
            } else if (args[i].equals("-col_key")) {
                col_key = args[++i];
            } else if (args[i].equals("-isAppend")) {
                isAppend = args[++i].equals("true");
            } else if (args[i].equals("-emb_key")) {
                emb_key = args[++i];
            } else if (args[i].equals("-tfidf")) {
                tfidf = FileUtils.readMatrix(args[++i], "\t");
            } else if (args[i].equals("-mw")) {
                mw = FileUtils.readMatrix(args[++i], "\t");
            } else if (args[i].equals("-types")) {
                types = DataUtils.loadEntityTypes(args[++i], filter_entities);
            } else if (args[i].equals("-isAppend")) {
                isAppend = Boolean.parseBoolean(args[++i]);
            } else if (args[i].equals("-cat_rep")) {
                cat = CategoryRepresentation.readCategoryGraph(args[++i]);
                cat_map = new HashMap<>();
                cat.loadIntoMapChildCats(cat_map);

                //load the parents of each category
                cat_parents = new HashMap<>();
                cat_map.keySet().forEach(cat -> cat_parents.put(cat, new TIntHashSet()));
                cat_map.keySet().parallelStream().forEach(cat -> DataUtils.gatherParents(cat_map.get(cat), cat_parents.get(cat)));

                cat_index = new HashMap<>();
                cat_map.keySet().forEach(c -> cat_index.put(c.hashCode(), c));

                System.out.println("Loaded the category representation object and constructed the parent paths for each category");
            }
        }
        ArticleCandidates ac = new ArticleCandidates();

        if (option.equals("tables")) {
            ac.tables = DataUtils.loadTables(table_data, filter_entities, true);
            System.out.printf("Finished loading the data tables for %d entities.\n", ac.tables.size());

            Map<String, List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>>> table_rep = ac.loadTableRepresentations();

            //compute the features.
            ac.scoreTableCandidatesApproachSimple(table_rep, out_dir);
        } else if (option.equals("entities")) {
            //we need to load for each entity the parent categories (and their full path to the root)
            System.out.println("Starting to process entity features.");
            entity_cat_parents = new HashMap<>();
            entity_cats.keySet().forEach(e -> entity_cat_parents.put(e, new TIntHashSet()));
            entity_cats.keySet().forEach(e -> entity_cats.get(e).stream().filter(c -> cat_parents.containsKey(c)).forEach(c -> entity_cat_parents.get(e).addAll(cat_parents.get(c))));

            ac.scoreEntityCandidatesApproach(out_dir, cat_sim);
        } else if (option.equals("entity_feature_add")) {
            appendEntityFeatures(out_dir, isAppend, col_key, emb_key);
        } else if (option.equals("entity_cat_feature_add")) {
            entity_cat_parents = new HashMap<>();
            entity_cats.keySet().forEach(e -> entity_cat_parents.put(e, new TIntHashSet()));
            entity_cats.keySet().forEach(e -> entity_cats.get(e).stream().filter(c -> cat_parents.containsKey(c)).forEach(c -> entity_cat_parents.get(e).addAll(cat_parents.get(c))));
            appendEntityCategoryFeatures(out_dir, isAppend, emb_key);
        }
    }

    /**
     * Get embeddings based on a specific key.
     *
     * @param key
     * @return
     */
    public static Map<String, TDoubleArrayList> getEmbedding(String key) {
        if (key.equals("verse")) {
            return verse;
        } else if (key.equals("rdf2vec")) {
            return rdf2vec;
        }
        return rdf2vec;
    }

    /**
     * Append entity specific features. We do this to improve the efficiency of the feature computation to avoid loading
     * all possible embeddings at once.
     *
     * @param feature_file
     * @param isAppend
     * @param emb_key
     * @throws IOException
     */
    public static void appendEntityCategoryFeatures(String feature_file, boolean isAppend, String emb_key) throws IOException {
        Map<String, TDoubleArrayList> emb = getEmbedding(emb_key);
        Map<String, Map<String, double[]>> scores = new HashMap<>();
        seed_entities.parallelStream().forEach(entity -> {
            System.out.printf("Processing entity %s\n", entity);

            Set<String> cats_a = entity_cats.get(entity);
            Set<String> parents_a = new HashSet<>();
            if (cats_a != null) {
                cats_a.stream().filter(cat -> cat_map.containsKey(cat)).forEach(cat -> parents_a.addAll(cat_map.get(cat).parents.keySet()));
            }

            Map<String, double[]> sub_scores = new HashMap<>();
            filter_entities.forEach(entity_candidate -> {
                Set<String> cats_b = entity_cats.get(entity_candidate);
                Set<String> parents_b = new HashSet<>();

                if (cats_b != null) {
                    cats_b.stream().filter(cat -> cat_map.containsKey(cat)).forEach(cat -> parents_b.addAll(cat_map.get(cat).parents.keySet()));
                }

                Set<String> lca_cats = DataUtils.findLCACategories(entity_cat_parents.get(entity), entity_cat_parents.get(entity_candidate), cat_index, cat_map);

                //Emb category similarity
                double avg_cat_rdf2vec_ab = DataUtils.computeAverageCategoryEmbSim(entity, entity_candidate, emb, entity_cats);
                double avg_cat_rdf2vec_lca_a = DataUtils.computeAverageCategoryEmbSim(entity_cats.get(entity), lca_cats, emb);
                double avg_cat_rdf2vec_lca_b = DataUtils.computeAverageCategoryEmbSim(entity_cats.get(entity_candidate), lca_cats, emb);
                double avg_cat_parent_rdf2vec_sim = DataUtils.computeAverageCategoryEmbSim(parents_a, parents_b, emb);

                sub_scores.put(entity_candidate, new double[]{avg_cat_rdf2vec_ab, avg_cat_rdf2vec_lca_a, avg_cat_rdf2vec_lca_b, avg_cat_parent_rdf2vec_sim});
            });

            scores.put(entity, sub_scores);
        });

        String feature_header = "avg_cat_[EMB]_sim_ab\tavg_cat_parent_[EMB]_sim_ab\tcat_a_lca_avg_[EMB]\tcat_b_lca_avg_[EMB]";
        feature_header = feature_header.replace("[EMB]", emb_key);

        if (!isAppend) {
            StringBuffer sb = new StringBuffer();
            sb.append("entity_a\tentity_b\t").append(feature_header).append("\n");
            for (String ea : scores.keySet()) {
                for (String eb : scores.get(ea).keySet()) {
                    sb.append(ea).append("\t").append(eb);
                    for (double score : scores.get(ea).get(eb)) {
                        sb.append("\t").append(score);
                    }
                    sb.append("\n");

                    if (sb.length() > 100000) {
                        FileUtils.saveText(sb.toString(), feature_file, true);
                        sb.delete(0, sb.length());
                    }
                }
            }
            FileUtils.saveText(sb.toString(), feature_file, true);
        } else {
            BufferedReader reader = FileUtils.getFileReader(feature_file);
            String line;
            int idx = 0;
            StringBuffer sb = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                if (idx == 0) {
                    sb.append(line).append("\t").append(feature_header).append("\n");
                    idx++;
                    continue;
                }
                String[] data = line.split("\t");
                String ea = data[0];
                String eb = data[1];

                sb.append(line);
                for (double score : scores.get(ea).get(eb)) {
                    sb.append("\t").append(score);
                }
                sb.append("\n");
                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), feature_file + "_out", true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), feature_file + "_out", true);
        }
    }

    /**
     * Append entity specific features. We do this to improve the efficiency of the feature computation to avoid loading
     * all possible embeddings at once.
     *
     * @param feature_file
     * @param isAppend
     * @param emb_key
     * @param col_key
     * @throws IOException
     */
    public static void appendEntityFeatures(String feature_file, boolean isAppend, String col_key, String emb_key) throws IOException {
        Map<String, TDoubleArrayList> emb = getEmbedding(emb_key);
        int emb_size = emb != null ? emb.size() : 0;
        System.out.printf("Loaded embeddings with %d items from the embedding %s.\n", emb_size, emb_key);
        Map<String, Map<String, Double>> scores = new HashMap<>();
        seed_entities.forEach(entity -> {
            System.out.printf("Processing entity %s\n", entity);

            Map<String, Double> sub_scores = new HashMap<>();
            filter_entities.forEach(entity_candidate -> {
                double sim = DataUtils.computeRDF2VecSim(entity, entity_candidate, emb);
                sub_scores.put(entity_candidate, sim);
            });

            scores.put(entity, sub_scores);
        });

        if (!isAppend) {
            StringBuffer sb = new StringBuffer();
            sb.append("entity_a\tentity_b\t").append(col_key).append("\n");
            for (String ea : scores.keySet()) {
                for (String eb : scores.get(ea).keySet()) {
                    sb.append(ea).append("\t").append(eb).append("\t").append(scores.get(ea).get(eb)).append("\n");

                    if (sb.length() > 100000) {
                        FileUtils.saveText(sb.toString(), feature_file, true);
                        sb.delete(0, sb.length());
                    }
                }
            }
            FileUtils.saveText(sb.toString(), feature_file, true);
        } else {
            BufferedReader reader = FileUtils.getFileReader(feature_file);
            String line;
            int idx = 0;
            StringBuffer sb = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                if (idx == 0) {
                    sb.append(line).append("\t").append(col_key).append("\n");
                    idx++;
                    continue;
                }
                String[] data = line.split("\t");
                String ea = data[0];
                String eb = data[1];

                double score = scores.containsKey(ea) && scores.get(ea).containsKey(eb) ? scores.get(ea).get(eb) : 0.0;
                sb.append(line).append("\t").append(score).append("\n");
                if (sb.length() > 100000) {
                    FileUtils.saveText(sb.toString(), feature_file + "_out", true);
                    sb.delete(0, sb.length());
                }
            }
            FileUtils.saveText(sb.toString(), feature_file + "_out", true);
        }
    }


    /**
     * Generates the feature representation for each of our ground-truth articles based on our approach which consists
     * of several features.
     *
     * @param out_dir
     */
    public void scoreTableCandidatesApproachSimple(Map<String, List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>>> table_rep, String out_dir) throws IOException {
        String header = "entity_a\tentity_b\tlabel\ttbl_a\ttbl_b\t" +
                "ct_sim_0\tcv_sim_0\tci_dist_0\tct_sim_1\tcv_sim_1\tci_dist_1\t" +
                "ct_sim_2\tcv_sim_2\tci_dist_2\tct_sim_3\tcv_sim_3\tci_dist_3\t" +
                "ct_sim_4\tcv_sim_4\tci_dist_4\n";


        seed_entities.parallelStream().forEach(entity -> {
            String out_file = out_dir + "/table_features_" + entity.replaceAll("/", "_") + ".tsv";
            FileUtils.saveText(header, out_file);
            System.out.printf("Processing entity %s\n", entity);

            List<String> feature_lines = new ArrayList<>();
            filter_entities.forEach(entity_candidate -> {
                if (!table_rep.containsKey(entity_candidate)) {
                    return;
                }
                //compute the table similarities
                Map<Integer, Map<Integer, List<List<Object>>>> tbl_sim = computeTableFeatures(table_rep.get(entity), table_rep.get(entity_candidate), gt_table_entities);

                for (int tbl_id_a : tbl_sim.keySet()) {
                    for (int tbl_id_b : tbl_sim.get(tbl_id_a).keySet()) {
                        List<List<Object>> scores = tbl_sim.get(tbl_id_a).get(tbl_id_b);

                        //create for each of these pairs the features
                        boolean label = false;
                        if (gt_table_entities != null && !gt_table_entities.isEmpty()) {
                            label = gt_table_entities.containsKey(tbl_id_a) && gt_table_entities.get(tbl_id_a).containsKey(tbl_id_b) ?
                                    gt_table_entities.get(tbl_id_a).get(tbl_id_b) : false;
                        } else {
                            label = gt_entities.containsKey(entity) && gt_entities.get(entity).contains(entity_candidate);
                        }

                        StringBuffer sb_features = new StringBuffer();
                        sb_features.append(entity).append("\t").append(entity_candidate).append("\t").append(label).append("\t");
                        sb_features.append(tbl_id_a).append("\t").append(tbl_id_b);

                        double[][] col_score = DataUtils.getMaxColumnSimilarities(scores);

                        int counter = 0;
                        while (counter <= 4 & counter < col_score.length) {
                            double title_sim = col_score[counter][0];
                            double col_val_sim = col_score[counter][1];
                            double dist = col_score[counter][2];
                            sb_features.append("\t").append(title_sim).append("\t").append(col_val_sim).append("\t").append(dist);
                            counter++;
                        }

                        //add per table pair a feature line.
                        feature_lines.add(sb_features.toString());
                    }
                }
            });
            StringBuffer sb_out = new StringBuffer();
            for (String line : feature_lines) {
                sb_out.append(line).append("\n");
                if (sb_out.length() > 10000) {
                    FileUtils.saveText(sb_out.toString(), out_file, true);
                    sb_out.delete(0, sb_out.length());
                }
            }
            FileUtils.saveText(sb_out.toString(), out_file, true);
            System.out.printf("Finished processing features for entity %s.\n", entity);
        });
    }

    /**
     * Generates the feature representation for each of our ground-truth articles based on our approach which consists
     * of several features.
     *
     * @param out_dir
     */
    public void scoreEntityCandidatesApproach(String out_dir, String cat_sim) throws IOException {
        Map<String, Map<String, Double>> cat_sim_data = DataUtils.readCategoryIDFSimData(cat_sim);

        //since we reuse the average word vectors we create them first and then reuse.
        Map<String, TDoubleArrayList> avg_w2v = new HashMap<>();
        filter_entities.stream().forEach(entity -> avg_w2v.put(entity, DataUtils.computeAverageWordVector(entity_abstracts.get(entity), stop_words, word2vec)));
        //filter the filter entities to only those that contain tables.

        seed_entities.parallelStream().forEach(entity -> {
            System.out.printf("Processing entity %s\n", entity);

            String out_file = out_dir + "/" + entity.replaceAll("/", "_");
            String header = "entity_a\tentity_b\ttitle_sim\tabs_sim\tdoc2vec_abs_sim\t" +
                    "lca_cat_str\tcat_overlap\tcat_parents_overlap\tmin_cat_rep_sim\t" +
                    "avg_cat_rep_sim\ttype_overlap\tmw_score\ttf_idf_score\tlabel\n";
            FileUtils.saveText(header, out_file);

            StringBuffer sb = new StringBuffer();
            filter_entities.forEach(entity_candidate -> {
                Set<String> cats_a = entity_cats.get(entity);
                Set<String> cats_b = entity_cats.get(entity_candidate);

                Set<String> parents_a = new HashSet<>();
                Set<String> parents_b = new HashSet<>();

                if (cats_a != null && cats_b != null) {
                    cats_a.stream().filter(cat -> cat_map.containsKey(cat)).forEach(cat -> parents_a.addAll(cat_map.get(cat).parents.keySet()));
                    cats_b.stream().filter(cat -> cat_map.containsKey(cat)).forEach(cat -> parents_b.addAll(cat_map.get(cat).parents.keySet()));
                }

                Set<String> types_a = types.get(entity);
                Set<String> types_b = types.get(entity_candidate);

                //create for each of these pairs the features
                boolean label = gt_entities.containsKey(entity) && gt_entities.get(entity).contains(entity_candidate);
                Set<String> lca_cats = DataUtils.findLCACategories(entity_cat_parents.get(entity), entity_cat_parents.get(entity_candidate), cat_index, cat_map);

                //compute the title similarities
                double entity_title_sim = DataUtils.computeTextSim(entity, entity_candidate, word2vec);

                //compute the w2v sim between the entity abstracts
                double abs_sim = DataUtils.computeCosineSim(avg_w2v.get(entity), avg_w2v.get(entity_candidate));
                double doc2vec_abs_sim = DataUtils.computeCosineSim(doc2vec.get(entity.replaceAll(" ", "_")), doc2vec.get(entity_candidate.replaceAll(" ", "_")));

                //compute the category overlap and their similarity.
                double cat_overlap = DataUtils.computeJaccardSimilarity(cats_a, cats_b);
                double cat_parents_overlap = DataUtils.computeJaccardSimilarity(parents_a, parents_b);

                List<Double> cat_idf_sim = DataUtils.computeCategoryIDFSimilarity(cats_a, cats_b, cat_sim_data);
                double min_cat_idf_sim = cat_idf_sim.stream().mapToDouble(x -> x).min().getAsDouble();
                double avg_cat_idf_sim = cat_idf_sim.stream().mapToDouble(x -> x).average().getAsDouble();

                double type_overlap = DataUtils.computeJaccardSimilarity(types_a, types_b);
                double mw_score = mw.containsKey(entity) && mw.get(entity).containsKey(entity_candidate) ? mw.get(entity).get(entity_candidate) : 0.0;
                double tfidf_score = tfidf.containsKey(entity) && tfidf.get(entity).containsKey(entity_candidate) ? tfidf.get(entity).get(entity_candidate) : 0.0;

                String lca_cats_str = "";
                if (lca_cats != null && !lca_cats.isEmpty())
                    for (String cat : lca_cats) {
                        if (!lca_cats_str.isEmpty()) lca_cats_str += ";";
                        lca_cats_str += cat.replaceAll(" ", "_");
                    }

                //add the features.
                sb.append(entity).append("\t").append(entity_candidate).append("\t").append(entity_title_sim).append("\t").
                        append(abs_sim).append("\t").append(doc2vec_abs_sim).append("\t").append(lca_cats_str).append("\t");

                sb.append(cat_overlap).append("\t").append(cat_parents_overlap).append("\t").append(min_cat_idf_sim).append("\t").
                        append(avg_cat_idf_sim).append("\t").append(type_overlap).append("\t");
                sb.append(mw_score).append("\t").append(tfidf_score).append("\t").append(label).append("\n");

                if (sb.length() > 1000000) {
                    FileUtils.saveText(sb.toString(), out_file, true);
                    sb.delete(0, sb.length());
                }
            });
            FileUtils.saveText(sb.toString(), out_file, true);
            System.out.printf("Finished processing features for entity %s.\n", entity);
        });
    }

    /**
     * Pre-process the tables.
     *
     * @return
     */
    public Map<String, List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>>> loadTableRepresentations() {
        Map<String, List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>>> data = new HashMap<>();

        for (String entity : tables.keySet()) {
            List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>> sub_data = new ArrayList<>();
            data.put(entity, sub_data);


            for (WikiTable tbl : tables.get(entity)) {
                List<Map.Entry<TDoubleArrayList, TDoubleArrayList>> col_reps = new ArrayList<>();
                WikiColumnHeader[] cols = tbl.columns[tbl.columns.length - 1];
                for (int i = 0; i < cols.length; i++) {
                    WikiColumnHeader col = cols[i];

                    TDoubleArrayList avg_title = DataUtils.computeAverageWordVector(col.column_name, stop_words, word2vec);
                    TDoubleArrayList avg_val = DataUtils.getAverageW2VFromColValueDist(col, word2vec);
                    col_reps.add(new AbstractMap.SimpleEntry<>(avg_title, avg_val));
                }
                sub_data.add(new AbstractMap.SimpleEntry<>(tbl.table_id, col_reps));
            }
        }

        return data;
    }

    /**
     * Compute the similarity features between two entities in terms of their tables, respectively the table schemas.
     *
     * @param tables_a
     * @param tables_b
     * @return
     */
    public Map<Integer, Map<Integer, List<List<Object>>>> computeTableFeatures(List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>> tables_a,
                                                                               List<Map.Entry<Integer, List<Map.Entry<TDoubleArrayList, TDoubleArrayList>>>> tables_b,
                                                                               Map<Integer, Map<Integer, Boolean>> gt_pairs) {
        Map<Integer, Map<Integer, List<List<Object>>>> results = new HashMap<>();

        for (int index_a = 0; index_a < tables_a.size(); index_a++) {
            List<Map.Entry<TDoubleArrayList, TDoubleArrayList>> columns_a = tables_a.get(index_a).getValue();

            int table_id_a = tables_a.get(index_a).getKey();

            if (gt_pairs != null && !gt_pairs.containsKey(table_id_a)) {
                continue;
            }
            results.put(table_id_a, new HashMap<>());

            for (int index_b = 0; index_b < tables_b.size(); index_b++) {
                List<Map.Entry<TDoubleArrayList, TDoubleArrayList>> columns_b = tables_b.get(index_b).getValue();

                int table_id_b = tables_b.get(index_b).getKey();

                if (gt_pairs != null && (!gt_pairs.containsKey(table_id_a) || !gt_pairs.get(table_id_a).containsKey(table_id_b))) {
                    continue;
                }

                results.get(table_id_a).put(table_id_b, new ArrayList<>());

                for (int i = 0; i < columns_a.size(); i++) {
                    Map.Entry<TDoubleArrayList, TDoubleArrayList> col_a = columns_a.get(i);
                    for (int j = 0; j < columns_b.size(); j++) {
                        Map.Entry<TDoubleArrayList, TDoubleArrayList> col_b = columns_b.get(j);

                        double title_sim = DataUtils.computeCosineSim(col_a.getKey(), col_b.getKey());
                        double value_sim = DataUtils.computeCosineSim(col_a.getValue(), col_b.getValue());

                        List<Object> scores = new ArrayList<>();
                        scores.add(i);
                        scores.add(j);
                        scores.add(title_sim);
                        scores.add(value_sim);

                        results.get(table_id_a).get(table_id_b).add(scores);
                    }
                }
            }
        }

        return results;
    }


}