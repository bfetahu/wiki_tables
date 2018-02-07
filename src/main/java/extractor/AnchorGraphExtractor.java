package extractor;

import io.FileUtils;
import org.w3c.dom.Document;

import java.io.BufferedReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 2/7/18.
 */
public class AnchorGraphExtractor {
    //we use this pattern to extract all the anchor text from Wikipedia pages.
    public static final Pattern anchor_pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");

    public static void main(String[] args) throws Exception {
        String in_dir = args[0];
        String out_dir = args[1];
        int num_threads = args.length < 3 ? 100 : Integer.parseInt(args[2]);
        Set<String> seeds = args.length < 4 ? null : FileUtils.readIntoSet(args[3], "\n", false);

        //do a parallel faster parse
        List<String> entities = new ArrayList<>();
        BufferedReader reader = FileUtils.getFileReader(in_dir);
        StringBuffer sb = new StringBuffer();
        String line;
        boolean is_data_region = false;
        while ((line = reader.readLine()) != null) {
            line = line.trim();

            if (line.equals("<page>")) {
                is_data_region = true;
                if (sb.length() != 0) {
                    entities.add(sb.toString());
                    sb.delete(0, sb.length());

                    if (entities.size() % num_threads == 0) {
                        extractAnchorLinks(entities, seeds, out_dir, num_threads);
                    }
                }
                continue;
            } else if (line.equals("</page>")) {
                continue;
            }
            if (is_data_region) {
                sb.append(line).append("\n");
            }
        }

        if (sb.length() != 0) {
            entities.add(sb.toString());
            extractAnchorLinks(entities, seeds, out_dir, num_threads);
        }
    }


    /**
     * Extract the Wikipedia anchor graph. For each page we store its outgoing links based on the anchor text.
     *
     * @param entities
     * @param seeds
     * @param out_file
     * @param num_threads
     * @throws InterruptedException
     */
    public static void extractAnchorLinks(List<String> entities, Set<String> seeds, String out_file, int num_threads) throws InterruptedException {
        Map<String, String> categories = new HashMap<>();
        ExecutorService thread_pool = Executors.newFixedThreadPool(num_threads);

        entities.parallelStream().forEach(page_text -> {
            Runnable r = () -> {
                try {
                    String page_content = "<page>" + page_text;
                    if (!page_text.contains("</page>")) {
                        page_content += "</page>";
                    }

                    Document doc = FileUtils.readXMLDocumentFromString(page_content);
                    String entity_name = doc.getElementsByTagName("title").item(0).getTextContent();
                    String entity_text = doc.getElementsByTagName("text").item(0).getTextContent();

                    if (seeds != null && !seeds.isEmpty() && !seeds.contains(entity_name)) {
                        return;
                    }
                    StringBuffer sb = new StringBuffer();
                    Matcher m = anchor_pattern.matcher(entity_text);
                    sb.append(entity_name);
                    while (m.find()) {
                        String anchor = m.group().replaceAll("\\[|\\]", "").trim().intern();

                        if (anchor.contains("|")) {
                            anchor = anchor.substring(0, anchor.indexOf("|"));
                        }
                        sb.append("\t").append(anchor);
                    }
                    sb.append("\n");

                    System.out.println("Parsing entity " + entity_name);
                    categories.put(entity_name, sb.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(page_text);
                }
            };
            thread_pool.submit(r);
        });
        thread_pool.shutdown();
        thread_pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        entities.clear();

        StringBuffer sb = new StringBuffer();
        for (String entity_name : categories.keySet()) {
            sb.append(categories.get(entity_name)).append("\n");

            if (sb.length() > 100000) {
                FileUtils.saveText(sb.toString(), out_file, true);
                sb.delete(0, sb.length());
            }
        }
        FileUtils.saveText(sb.toString(), out_file, true);
    }

}
