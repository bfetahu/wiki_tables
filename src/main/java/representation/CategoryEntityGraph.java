package representation;

import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import io.FileUtils;
import utils.DataUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by besnik on 3/11/18.
 */
public class CategoryEntityGraph {
    //keep the names of the nodes in this graph
    public Map<String, Integer> node_index;
    public int current_node_id = 0;

    //keep the edges of this graph through the indices of the actual node names.
    public TIntObjectHashMap<TIntHashSet> edges;

    //keep the pre-computed graph-embedding.
    public TIntObjectHashMap<TDoubleArrayList> graph_embedding;

    public static Set<String> filter_entities = new HashSet<>();
    public static Set<String> table_entities = new HashSet<>();

    public static void main(String[] args) throws IOException {
        String out_dir = "", anchor_data = "", cat_entities = "", cat_path = "", option = "";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-option")) {
                option = args[++i];
            } else if (args[i].equals("-out_dir")) {
                out_dir = args[++i];
            } else if (args[i].equals("-anchor_data")) {
                anchor_data = args[++i];
            } else if (args[i].equals("-cat_entities")) {
                cat_entities = args[++i];
            } else if (args[i].equals("-cat_path")) {
                cat_path = args[++i];
            } else if (args[i].equals("-filter_entities")) {
                filter_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            } else if (args[i].equals("-table_entities")) {
                table_entities = FileUtils.readIntoSet(args[++i], "\n", false);
            }
        }

        CategoryEntityGraph ceg = new CategoryEntityGraph();
        if (option.equals("graph")) {
            boolean hasIndex = ceg.loadIndex(out_dir);
            boolean hasEdges = ceg.loadEdges(out_dir);

            if (!hasEdges || !hasIndex) {
                ceg.loadGraph(anchor_data, cat_entities, cat_path);
                ceg.dumpGraph(out_dir);
            }

            System.out.printf("Loaded the entity-category graph with %d nodes and %d edges.\n", ceg.getNumNodes(), ceg.getNumEdges());
        } else if (option.equals("embedding")) {
            boolean hasIndex = ceg.loadIndex(out_dir);
            if (hasIndex) {
                //load the embeddings
                ceg.loadEmbedding(out_dir);
            }
        }
    }

    /**
     * Load the pre-existing node index of the entity-category graph.
     *
     * @param in_dir
     * @return
     * @throws IOException
     */
    public boolean loadIndex(String in_dir) throws IOException {
        String index_file = in_dir + "/category_entity_node.index.gz";
        if (!FileUtils.fileExists(index_file, false)) {
            return false;
        }

        String line;
        BufferedReader reader = FileUtils.getFileReader(index_file);

        node_index = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            String node_label = data[0];
            int node_id = Integer.parseInt(data[1]);
            node_index.put(node_label, node_id);
        }
        current_node_id = node_index.values().stream().mapToInt(x -> x).max().getAsInt() + 1;
        return true;
    }

    /**
     * Load the edges from the pre-computed edge file.
     *
     * @param in_dir
     * @return
     * @throws IOException
     */
    public boolean loadEdges(String in_dir) throws IOException {
        String edge_file = in_dir + "/category_entity_node.edges.gz";
        if (!FileUtils.fileExists(edge_file, false)) {
            return false;
        }
        edges = new TIntObjectHashMap<>();

        int num_edges = 0;
        String line;
        BufferedReader reader = FileUtils.getFileReader(edge_file);
        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\t");

            int a = Integer.valueOf(data[0]);
            int b = Integer.valueOf(data[1]);

            if (!edges.containsKey(a)) {
                edges.put(a, new TIntHashSet());
            }
            edges.get(a).add(b);
            num_edges++;
        }
        System.out.printf("Loaded the edges for the entity-category graph with %d edges.\n", num_edges);
        return true;
    }

    /**
     * Load the pre-computed word embeddings.
     *
     * @param in_dir
     */
    public boolean loadEmbedding(String in_dir) throws IOException {
        String embedding = in_dir + "/category_entity.emb.gz";
        if (!FileUtils.fileExists(embedding, false)) {
            return false;
        }
        if (node_index == null || node_index.isEmpty()) {
            loadIndex(in_dir);
        }

        if (edges == null || edges.isEmpty()) {
            loadEdges(in_dir);
        }

        graph_embedding = new TIntObjectHashMap<>();
        String line;
        BufferedReader reader = FileUtils.getFileReader(embedding);

        while ((line = reader.readLine()) != null) {
            String[] data = line.split("\\s+");
            if (data.length == 2) {
                continue;
            }

            int node_id = Integer.parseInt(data[0]);
            TDoubleArrayList embedding_val = new TDoubleArrayList();

            for (int i = 1; i < data.length; i++) {
                double val = Double.parseDouble(data[i]);
                embedding_val.add(val);
            }
            graph_embedding.put(node_id, embedding_val);
        }
        System.out.printf("Finished loading the pre-computed graph embeddings with %d nodes.\n", graph_embedding.size());
        return true;
    }

    /**
     * Dump the nodes and the edges of the graph.
     *
     * @param out_dir
     */
    public void dumpGraph(String out_dir) {
        //dump first the node index.
        StringBuffer sb = new StringBuffer();
        node_index.keySet().forEach(n -> sb.append(n).append("\t").append(node_index.get(n)).append("\n"));
        FileUtils.saveText(sb.toString(), out_dir + "/category_entity_node.index");

        //output the edges.
        sb.delete(0, sb.length());
        for (int i : edges.keys()) {
            for (int j : edges.get(i).toArray()) {
                sb.append(i).append("\t").append(j).append("\n");
            }

            if (sb.length() > 10000) {
                FileUtils.saveText(sb.toString(), out_dir + "/category_entity_node.edges", true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_dir + "/category_entity_node.edges", true);
    }

    /**
     * Load the data into the graph.
     *
     * @param anchor_graph
     * @param entity_cats
     * @param cat_path
     * @throws IOException
     */
    public void loadGraph(String anchor_graph, String entity_cats, String cat_path) throws IOException {
        edges = new TIntObjectHashMap<>();
        node_index = new HashMap<>();

        CategoryRepresentation cat = (CategoryRepresentation) FileUtils.readObject(cat_path);
        Map<String, Set<String>> cat_entities = DataUtils.readCategoryMappingsWiki(entity_cats, null);

        //load the anchor graph data first.
        loadAnchorGraph(anchor_graph);
        System.out.printf("Finished adding anchor data into the graph with %d nodes and %d edges.\n", getNumNodes(), getNumEdges());

        //load the entity-category data
        loadEntityCategoryGraph(cat_entities);
        System.out.printf("Finished adding category-entity data into the graph with %d nodes and %d edges.\n", getNumNodes(), getNumEdges());

        //load the taxonomic data from the Wikipedia Category taxonomy
        loadCategoryTaxonomyGraph(cat);
        System.out.printf("Finished adding category-taxonomy data into the graph with %d nodes and %d edges.\n", getNumNodes(), getNumEdges());
    }

    /**
     * Load the entity anchor data into the graph.
     *
     * @param wiki_anchor_graph
     * @throws IOException
     */
    public void loadAnchorGraph(String wiki_anchor_graph) throws IOException {
        String line;
        BufferedReader reader = FileUtils.getFileReader(wiki_anchor_graph);
        while ((line = reader.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            String[] data = line.split("\t");

            String source_article = data[0];
            if (!table_entities.contains(source_article)) {
                continue;
            }

            if (!node_index.containsKey(source_article)) {
                node_index.put(source_article, current_node_id);
                current_node_id++;
            }

            //get the assigned node_id for the current source article.
            int source_article_id = node_index.get(source_article);

            for (int i = 1; i < data.length; i++) {
                String out_article = data[i];
                if (!filter_entities.contains(out_article)) {
                    continue;
                }
                if (!node_index.containsKey(out_article)) {
                    node_index.put(out_article, current_node_id);
                    current_node_id++;
                }

                int out_article_id = node_index.get(out_article);

                //add the corresponding edge.
                if (!edges.containsKey(source_article_id)) {
                    edges.put(source_article_id, new TIntHashSet());
                }
                edges.get(source_article_id).add(out_article_id);
            }
        }
    }

    /**
     * Load the categories into the graph, and add their corresponding edges with the entities.
     *
     * @param cat_entities
     */
    public void loadEntityCategoryGraph(Map<String, Set<String>> cat_entities) {
        for (String category : cat_entities.keySet()) {
            if (!node_index.containsKey(category)) {
                node_index.put(category, current_node_id);
                current_node_id++;
            }

            int category_id = node_index.get(category);

            //add the edges to its entities.
            for (String entity : cat_entities.get(category)) {
                if (!table_entities.contains(entity)) {
                    continue;
                }

                if (!node_index.containsKey(entity)) {
                    node_index.put(entity, current_node_id);
                    current_node_id++;
                }

                int entity_id = node_index.get(entity);
                if (!edges.containsKey(category_id)) {
                    edges.put(category_id, new TIntHashSet());
                }
                edges.get(category_id).add(entity_id);
            }
        }
    }

    /**
     * Add the remaining categories from the category taxonomy, and add their relations into the category entity graph.
     *
     * @param cat
     */
    public void loadCategoryTaxonomyGraph(CategoryRepresentation cat) {
        if (!node_index.containsKey(cat.label)) {
            node_index.put(cat.label, current_node_id);
            current_node_id++;
        }

        if (cat.children.isEmpty()) {
            return;
        }

        int cat_id = node_index.get(cat.label);
        if (!edges.containsKey(cat_id)) {
            edges.put(cat_id, new TIntHashSet());
        }

        for (String child_cat_label : cat.children.keySet()) {
            if (!node_index.containsKey(child_cat_label)) {
                node_index.put(child_cat_label, current_node_id);
                current_node_id++;
            }

            int child_cat_id = node_index.get(child_cat_label);
            edges.get(cat_id).add(child_cat_id);

            //do the same for all the children of this child category in a recursive manner.
            loadCategoryTaxonomyGraph(cat.children.get(child_cat_label));
        }
    }

    /**
     * Get the number of nodes that are added in the graph.
     *
     * @return
     */
    public int getNumNodes() {
        if (node_index == null) {
            return 0;
        }
        return node_index.size();
    }

    /**
     * Get the number of edges in the graph.
     *
     * @return
     */
    public int getNumEdges() {
        if (edges == null) {
            return 0;
        }
        int num_edges = 0;
        for (int a : edges.keys()) {
            num_edges += edges.get(a).size();
        }
        return num_edges;
    }

    /**
     * Return the embedding dimensions.
     *
     * @return
     */
    public int getEmbeddDimension() {
        if (graph_embedding.isEmpty()) {
            return 0;
        }

        return graph_embedding.get(0).size();
    }

    /**
     * Construct an undirected graph from the adjacency list.
     *
     * @return
     */
    public Graph constructUndirectedGraph() {
        Graph<Integer, Integer> g = new UndirectedSparseGraph();

        //add the nodes in the undirected graph.
        node_index.keySet().forEach(node -> g.addVertex(node_index.get(node)));

        //add the edges.
        int edge_id = 0;
        for (int i : edges.keys()) {
            for (int j : edges.get(i).toArray()) {
                g.addEdge(edge_id, i, j);
                edge_id++;
            }
        }
        return g;
    }


    /**
     * Return the embeddings of the given key if it exists in the index.
     *
     * @param key
     * @return
     */
    public TDoubleArrayList getEmbeddingByKey(String key) {
        if (!node_index.containsKey(key) || graph_embedding == null) {
            return null;
        }
        return graph_embedding.get(node_index.get(key));
    }

    /**
     * Get the embeddings for a given index in our graph.
     *
     * @param index
     * @return
     */
    public TDoubleArrayList getEmbeddingByIndex(int index) {
        if (graph_embedding == null) {
            return null;
        }
        return graph_embedding.get(index);
    }
}
