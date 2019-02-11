package extractor;

import datastruct.table.WikiColumnHeader;
import datastruct.table.WikiTable;
import datastruct.table.WikiTableCell;
import io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import utils.WebUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by besnik on 7/13/18.
 */
public class HTMLTableExtractor {
    public static void main(String[] args) throws IOException {
        String option = args[0];
        String seed_path = args[1];
        String table_data = args[2];

        if (option.equals("crawl_table_articles")) {
            Set<String> tables = FileUtils.readIntoSet(seed_path, "\n", false);

            extractHTMLWikiPageContent(tables, table_data);
        } else if (option.equals("parse_tables")) {
            String out_dir = args[3];
            parseHTMLTables(table_data, out_dir);
        }
    }

    /**
     * Extract the HTML content of Wikipedia pages. We call the REST API by Wikimedia which returns the HTML content
     * of an article.
     *
     * @param entities
     * @param out_dir
     * @throws IOException
     */
    public static void extractHTMLWikiPageContent(Set<String> entities, String out_dir) throws IOException {
        FileUtils.checkDir(out_dir);
        String out_file = out_dir + "/tables_wiki_subset.txt";
        FileUtils.saveText("", out_file);
        for (String entity : entities) {
            String entity_label = URLEncoder.encode(entity.replaceAll(" ", "_"));
            try {
                //https://en.wikipedia.org/api/rest_v1/page/html/Itzehoe
                String url = "https://en.wikipedia.org/api/rest_v1/page/html/" + entity_label;
                String url_body = WebUtils.getURLContent(url).replaceAll("<!--(.*?)-->", "");

                String out_json = entity + "\t" + url_body.replace("\n", "\\n") + "\n";
                FileUtils.saveText(out_json, out_file, true);

                System.out.printf("Finished extracting HTML content for entity %s.\n", entity);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Extract the tables from the HTML content of the articles.
     *
     * @param file
     * @throws IOException
     */
    public static void parseHTMLTables(String file, String outfile) throws IOException {
        BufferedReader reader = FileUtils.getFileReader(file);
        String line;

        List<String> table_data_lines = new ArrayList<>();
        Queue<String> table_output_lines = new ConcurrentLinkedQueue<>();

        AtomicInteger atm = new AtomicInteger(5000000);
        AtomicInteger err_atm = new AtomicInteger();
        while ((line = reader.readLine()) != null) {
            String entity_text = line.contains("\t") ? line.substring(line.indexOf("\t")) : line;

            //add the lines
            table_data_lines.add(entity_text);

            if (table_data_lines.size() > 1000) {
                table_data_lines.parallelStream().forEach(table_html_text -> {
                    String tbl_out = parseTableHTML(table_html_text, atm, err_atm);
                    table_output_lines.add(tbl_out);
                });

                //output the data
                StringBuffer sb = new StringBuffer();
                for (String tbl_html_out : table_output_lines) {
                    sb.append(tbl_html_out);
                    if (sb.length() > 100000) {
                        FileUtils.saveText(sb.toString(), outfile, true);
                        sb.delete(0, sb.length());
                    }
                }

                FileUtils.saveText(sb.toString(), outfile, true);
                //once done, flush out all the data
                table_data_lines.clear();
                table_output_lines.clear();
            }
        }
        //parse the remainder
        table_data_lines.parallelStream().forEach(table_html_text -> {
            String tbl_out = parseTableHTML(table_html_text, atm, err_atm);
            table_output_lines.add(tbl_out);
        });

        StringBuffer sb = new StringBuffer();
        for (String tbl_html_out : table_output_lines) {
            sb.append(tbl_html_out);

            if (sb.length() > 100000) {
                FileUtils.saveText(sb.toString(), outfile, true);
                sb.delete(0, sb.length());
            }
        }
        System.out.printf("Finished processing %d tables, and there %d were erroneous.\n", atm.get(), err_atm.get());
    }


    /**
     * Parse a single article.
     *
     * @param entity_text
     * @param atm
     * @param atm_err
     * @return
     */
    public static String parseTableHTML(String entity_text, AtomicInteger atm, AtomicInteger atm_err) {
        StringBuffer sb = new StringBuffer();
        Document doc = Jsoup.parse(entity_text);

        Elements sections = doc.select("section");
        String title = doc.title().replaceAll(" ", "_");
        System.out.printf("Processing entity %s\n", title);

        sb.append("{\"entity\":\"").append(StringEscapeUtils.escapeJson(title)).append("\", \"sections\":[");

        int section_idx = 0;
        for (Element section : sections) {
            String section_id = section.attr("data-mw-section-id");
            String section_name = getSectionName(section, section_id);
            Elements tables = section.getElementsByClass("wikitable");

            if (section_name.isEmpty() || tables == null || tables.isEmpty()) {
                continue;
            }

            if (section_idx != 0) {
                sb.append(", ");
            }


            sb.append("{\"section\":\"").append(StringEscapeUtils.escapeJson(section_name)).append("\", \"tables\":[");

            int tbl_idx = 0;
            for (Element table : tables) {
                if (table.select("table").size() > 1) {
                    continue;
                }
                //since some tables may contain sub-tables, we first split those and then process them further.
                try {
                    Map<Integer, List<Element>> table_rows = getSubTables(table);
                    for (int sub_tbl_id : table_rows.keySet()) {
                        int table_id = atm.incrementAndGet();
                        WikiTable tbl = parseTable(table_rows.get(sub_tbl_id));
                        tbl.section = section_name;
                        tbl.markup = table.toString();
                        tbl.table_caption = table.select("caption").text();
                        tbl.table_id = table_id;
                        table_id++;

                        String table_json_output = TablePrinter.printTableToJSON(tbl);

                        if (tbl_idx != 0) {
                            sb.append(", ");
                        }

                        sb.append(table_json_output);
                        tbl_idx++;

                    }
                } catch (Exception e) {
                    FileUtils.saveText(table.toString().replaceAll("\n", "\\n") + "\n", "error.log", true);
                    atm_err.incrementAndGet();
                }
            }
            sb.append("]}");
            section_idx++;
        }
        sb.append("]}\n");
        return sb.toString();
    }

    /**
     * Parse the HTML representation of the table into the WikiTable object.
     *
     * @param table_rows
     * @return
     */

    public static WikiTable parseTable(List<Element> table_rows) {
        List<Element> rows_data = table_rows.stream().filter(tr -> !tr.select("td").isEmpty()).collect(Collectors.toList());

        WikiTable tbl = new WikiTable();
        tbl.setColumnHeaders(table_rows);

        int num_rows = rows_data.size();
        tbl.initializeCells(num_rows);

        for (int i = 0; i < rows_data.size(); i++) {
            Elements cell_values = rows_data.get(i).getAllElements();
            int k = 0;

            for (int j = 1; j < cell_values.size(); j++) {
                Element cell_value = cell_values.get(j);
                if (cell_value.select("td").isEmpty() && cell_value.select("th").isEmpty()) {
                    continue;
                }
                WikiColumnHeader col = tbl.columns[tbl.columns.length - 1][k];
                WikiTableCell cell = new WikiTableCell(cell_value, col);
                k = tbl.addCellValue(cell, i, k);
            }
        }

        return tbl;
    }


    /**
     * Extract the section title. Depending on the level of the section, we need to look for either <h2></h2>, <h3></h3> etc.
     *
     * @param section
     * @param section_id
     * @return
     */
    public static String getSectionName(Element section, String section_id) {
        String section_name = "";
        if (section_id.equals("0")) {
            return "MAIN_SECTION";
        }
        if (!section.select("h2").isEmpty()) {
            return section.select("h2").first().text();
        } else if (!section.select("h3").isEmpty()) {
            section_name = section.select("h3").first().text();
        } else if (!section.select("h4").isEmpty()) {
            section_name = section.select("h4").first().text();
        } else if (!section.select("h5").isEmpty()) {
            section_name = section.select("h5").first().text();
        }

        return section_name;
    }

    /**
     * Some tables contain labels which are columns spanning across all columns and they appear after data rows. In this
     * case we can split the table into multiple parts.
     *
     * @param table
     * @return
     */
    public static Map<Integer, List<Element>> getSubTables(Element table) {
        Map<Integer, List<Element>> table_data = new HashMap<>();
        Elements rows = table.select("tr");

        int table_idx = 0;
        boolean data_region = false;
        for (int i = 0; i < rows.size(); i++) {
            Element row = rows.get(i);
            if (!table_data.containsKey(table_idx)) {
                table_data.put(table_idx, new ArrayList<>());
            }

            boolean is_header = !row.select("th").isEmpty();
            boolean is_data = !row.select("td").isEmpty();

            if (!is_header && !is_data) {
                continue;
            }

            if (!is_header || is_data) {
                if (!data_region && is_data) {
                    data_region = true;
                }
            } else if (is_header) {
                if (data_region) {
                    table_idx++;
                    data_region = false;

                    //add the new sub-table
                    if (!table_data.containsKey(table_idx)) {
                        table_data.put(table_idx, new ArrayList<>());
                    }

                    //from the first entry get the column headers.
                    List<Element> top_rows = table_data.get(0).size() > 1 ? table_data.get(0).stream().filter(th -> !th.select("th").isEmpty()).collect(Collectors.toList()) : table_data.get(0);
                    int length = top_rows.size() - 1;
                    table_data.get(table_idx).addAll(top_rows.subList(0, length));
                }
            }
            table_data.get(table_idx).add(row);
        }
        return table_data;
    }

}
