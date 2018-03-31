package extractor;

import datastruct.table.EntityTables;
import datastruct.wikitable.WikiTable;
import edu.jhu.nlp.wikipedia.WikiXMLParser;
import edu.jhu.nlp.wikipedia.WikiXMLParserFactory;
import entities.WikiEntity;
import entities.WikiSection;
import io.FileUtils;

import java.util.*;

/**
 * Created by besnik on 5/22/17.
 */
public class TableExtractor {
    public List<Map.Entry<String, Integer>> getWikiTablesMarkup(String start_clause, String end_clause, String text) {
        String[] lines = text.split("\n");

        List<Map.Entry<String, Integer>> tables = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        boolean found_table = false;
        int current_length = 0;
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.contains(start_clause)) {
                found_table = true;
                sb.append(line.substring(line.indexOf(start_clause))).append("\n");
                continue;
            }

            if (line.contains(end_clause) && found_table) {
                sb.append(line.substring(0, line.indexOf(end_clause) + end_clause.length())).append("\n");
                tables.add(new AbstractMap.SimpleEntry<>(sb.toString(), current_length));
                sb.delete(0, sb.length());
                found_table = false;
            }

            if (found_table) {
                sb.append(lines[i]).append("\n");
            }
            current_length += line.length();
        }

        return tables;
    }

    /**
     * Extract the Wiki table markup for a given text from Wikipedia.
     *
     * @param wiki_text
     * @return
     */
    public List<WikiTable> extractTables(String wiki_text) {
        if (!wiki_text.contains("class=\"wikitable")) {
            return null;
        }
        List<Map.Entry<String, Integer>> tables_str = getWikiTablesMarkup("{| class=\"wikitable", "|}", wiki_text);
        List<WikiTable> wiki_tables = new ArrayList<>();

        try {
            for (Map.Entry<String, Integer> wiki_markup : tables_str) {
                //store the wiki table text for later processing.
                String wiki_table = wiki_markup.getKey();
                try {
                    WikiTable table = new WikiTable(wiki_table);
                    table.start = wiki_markup.getValue();
                    table.preceeding_text = wiki_table;
                    table.generateWikiTable();
                    wiki_tables.add(table);
                } catch (Exception e) {
                    FileUtils.saveText(wiki_table, "table_err.log", true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return wiki_tables;
    }

    public static void main(String[] args) throws Exception {
        TableExtractor tbl_ext = new TableExtractor();
        String wiki_snapshot = args[0];
        String out_dir = args[1];

        WikiXMLParser wxsp = WikiXMLParserFactory.getSAXParser(wiki_snapshot);
        wxsp.setPageCallback(page -> {
            String entity_name = page.getTitle().replaceAll("\n", "").trim();
            String entity_text = page.getWikiText();
            try {
                String file_name = out_dir + "/" + entity_name.replaceAll("[^\\w.-]", "_");
                System.out.printf("Processing entity %s\n", entity_name);
                if (FileUtils.fileExists(file_name, false)) {
                    return;
                }

                //parse the Wikipedia markup text.
                WikiEntity entity = new WikiEntity();
                entity.title = entity_name;
                entity.content = entity_text;

                //add first the categories to this entity
                page.getCategories().forEach(cat -> entity.addCategory(cat));

                //we do not want to extract here the references.
                entity.setExtractReferences(false);
                entity.setMainSectionsOnly(false);
                entity.setExtractStatements(false);
                entity.setSplitSections(true);
                entity.parseContent(false);

                //extract the tables from each section.
                EntityTables et = new EntityTables();
                et.entity = entity;
                Set<String> section_keys = entity.getSectionKeys();
                int total_tables = 0;
                for (String section_key : section_keys) {
                    WikiSection section = entity.getSection(section_key);
                    List<WikiTable> entity_section_tables = tbl_ext.extractTables(section.section_text);
                    if (entity_section_tables != null) {
                        et.tables.put(section_key, entity_section_tables);
                        total_tables += entity_section_tables.size();
                    }
                }

                if (total_tables != 0) {
                    FileUtils.saveText(et.toString(), out_dir, true);
                    //store the object, which contains the entity data and the extracted tables.
                    System.out.printf("Finished processing entity %finished_gt_seeds and extracted %d tables, stored the data in %finished_gt_seeds.\n", entity.title, total_tables, file_name);
                }
            } catch (Exception e) {
                System.out.println("Error processing entity" + entity_name + "\t" + e.getMessage());
            }
        });
        wxsp.parse();
    }
}
