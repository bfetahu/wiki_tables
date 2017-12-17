package extractor;

import org.w3c.dom.Document;
import io.FileUtils;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 12/10/17.
 */
public class CategoryExtractor {
    public static void main(String[] args) throws Exception {
        String in_dir = args[0];
        String out_dir = args[1];
        boolean is_categories = args[2].equals("true");
        int num_threads = Integer.parseInt(args[3]);

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
                        parseEntityCategories(entities, out_dir, is_categories, num_threads);
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
            parseEntityCategories(entities, out_dir, is_categories, num_threads);
        }
    }

    /**
     * Extract all the categories and their sub-category relations from Wikipedia.
     *
     * @param entities
     * @param out_file
     * @throws InterruptedException
     */
    public static void parseEntityCategories(List<String> entities, String out_file, boolean is_category, int num_threads) throws InterruptedException {
        Map<String, String> categories = new HashMap<>();
        ExecutorService thread_pool = Executors.newFixedThreadPool(num_threads);
        Pattern p_cat = Pattern.compile("\\[+Category:(.*?)\\]+");

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

                    if (is_category && !entity_name.startsWith("Category:")) {
                        return;
                    }

                    if (!is_category && entity_name.startsWith("Category:")) {
                        return;
                    }

                    StringBuffer sb = new StringBuffer();
                    Matcher m = p_cat.matcher(entity_text);
                    while (m.find()) {
                        String sub_category = m.group().replaceAll("\\[|\\]", "").trim().intern();
                        sb.append(entity_name).append("\t").append(sub_category).append("\n");
                    }

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
