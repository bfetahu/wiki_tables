package utils;

import org.apache.commons.lang3.StringUtils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by besnik on 5/25/17.
 */
public class TableCellUtils {
    //matches the text which is of the following pattern::: [[Confederation of African Athletics|Africa]]
    public static final Pattern sq_entity_pattern = Pattern.compile("\\[{2}(.*?)\\]{2}");
    //matches the text which is of the following pattern::: {{([[List of African records in athletics|records]])}}
    public static final Pattern cr_entity_pattern = Pattern.compile("\\{{2}(.*?)\\}{2}");

    //is used to validate if a line contains header data (column data) in the cases where an ! is used, in most of the cases such a character is used also in data lines but is used for formatting.
    public static final Pattern header_match = Pattern.compile("\t!\\s?[colspan|rowspan|style]");

    public static final List<Map.Entry<String, String>> table_formatting = new ArrayList<>();
    private static Pattern rowspan_pattern = Pattern.compile("rowspan=\"?[0-9]*\"?");
    private static Pattern colspan_pattern = Pattern.compile("colspan=\"?[0-9]*\"?");

    private static Pattern sortname_pattern = Pattern.compile("\\{\\{sortname(.*?)\\}\\}");

    public static void init() {
        table_formatting.add(new AbstractMap.SimpleEntry<>("style=", "style=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("width=", "width=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("height=", "height=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("valign=", "valign=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("halign=", "halign=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("style:", "style:[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("width:", "width:[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("height:", "height:[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("valign:", "valign:[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("halign:", "halign:[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("text-align:", "text-align:[\"|']?(.*?);[\"|']?"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("bgcolor", "-?bgcolor=[\"|'](.*?)[\"|']"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("align", "align\\s?=\\s?[\\\"|']?[a-zA-Z]*[\\\"|']?"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("flagicon", "flagicon\\|?"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("size=", "size=[\"|']?(.*?)[\"|']?[a-z%]+"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("flag", "(?i)Flag\\|?"));
        table_formatting.add(new AbstractMap.SimpleEntry<>("small", "small\\|"));
    }

    /**
     * Remove fields of the type {{sort| | }}
     *
     * @param value
     * @return
     */
    public static String removeSortWikiMarkup(String value) {
        if (value.contains("{{sort") && !value.contains("{{sortname")) {
            String remainder = value.indexOf("{{sort") == 0 ? "" : value.substring(0, value.indexOf("{{sort"));
            String sort_value = "";
            if (value.contains("}}")) {
                sort_value = value.substring(value.indexOf("{{sort"), value.indexOf("}}", value.indexOf("{{sort") + "{{sort".length()) + "}}".length());
            } else if (value.contains("}")) {
                sort_value = value.substring(value.indexOf("{{sort"), value.indexOf("}", value.indexOf("{{sort") + "{{sort".length()) + "}".length());
            } else {
                return value;
            }

            sort_value = sort_value.replace("{{sort", "").replace("}}", "").trim();
            sort_value = sort_value.substring(sort_value.indexOf("|") + "|".length());

            if (sort_value.contains("|")) {
                sort_value = sort_value.substring(sort_value.indexOf("|") + "|".length());
            }
            return remainder + " " + sort_value;
        }
        return value;
    }

    /**
     * This removes the sortname markup from Wiki tables. It handles all the known and documented cases from Wiki markup.
     *
     * @param value
     * @return
     */
    public static String removeSortnameWikiMarkup(String value) {
        if (!value.contains("{{sortname")) {
            return value;
        }
        //in case the column contains other value apart from the sorted field, here handle only the sorted field
        String remainder = value.indexOf("{{sortname") == 0 ? "" : value.substring(0, value.indexOf("{{sortname"));
        String sort_value = value.substring(value.indexOf("{{sortname"));

        if (sort_value.contains("}}")) {
            sort_value = value.substring(value.indexOf("{{sortname"), value.indexOf("}}", value.indexOf("{{sortname") + "{{sortname".length()) + "}}".length());
        } else if (sort_value.contains("}")) {
            //sometimes the input is malformed and it doesnt contain a double bracket.
            sort_value = value.substring(value.indexOf("{{sortname"), value.indexOf("}", value.indexOf("{{sortname") + "{{sortname".length()) + "}".length());
        } else {
            return value;
        }

        //to determine which parts to remove by counting the number of |.
        //Depending on their number we can determine which template has been used from the {{sortname}} clause
        int pipe_no = StringUtils.countMatches(sort_value, "|");
        switch (pipe_no) {
            case 2:
                //this is the simplest case, it consists of only {{sortname|first|last}}
                sort_value = "[[" + sort_value.replace("{{sortname|", "").replace("\\|", "\t").replace("}}", "").trim() + "]]";
                break;
            case 3:
                //this is another case where no link/ a link or disambiguation is provided
                // {{sortname|first|last|nolink=1}} {{sortname|first|last|dab=disambiguator}} {{sortname|first|last|target}}
                sort_value = sort_value.replace("{{sortname|", "").replace("}}", "").replaceAll("\\|", "\t");
                if (sort_value.contains("nolink")) {
                    sort_value = sort_value.substring(0, sort_value.indexOf("nolink")).trim();
                } else if (sort_value.contains("dab=")) {
                    String[] tmp = sort_value.split("\t");
                    String dab_key = tmp[tmp.length - 1].replace("dab=", "");

                    StringBuffer sb_new_val = new StringBuffer();
                    sb_new_val.append("[[");
                    for (int i = 0; i < tmp.length - 1; i++) {
                        sb_new_val.append(tmp[i]).append(" ");
                    }
                    sb_new_val.append("(").append(dab_key).append(")]]");
                    sort_value = sb_new_val.toString();
                } else {
                    String[] tmp = sort_value.split("\t");
                    StringBuffer sb_new_val = new StringBuffer();
                    sb_new_val.append("[[");

                    for (int i = 0; i < tmp.length - 1; i++) {
                        sb_new_val.append(tmp[i]).append(" ");
                    }
                    sb_new_val.append("|").append(tmp[tmp.length - 1]).append("]]");
                    sort_value = sb_new_val.toString();
                }
                break;
            case 4:
                //here we need to distinguish if its a sort or a sort with a link
                if (sort_value.contains("||")) {
                    if (sort_value.contains("sortname||")) {
                        //this is the case where the data has a sort and a target {{sortname||first|last|target|sort}}
                        sort_value = sort_value.replace("{{sortname||", "").replace("}}", "");
                        sort_value = sort_value.replaceAll("sort=(.*?)|", "").replaceAll("nolink=(.*?)|", "");
                        sort_value = sort_value.substring(0, sort_value.lastIndexOf("|")).replace("|", " ").trim();
                    } else {
                        //this is the case where the data has a sort and a target {{sortname|first|last|target|sort}}
                        sort_value = sort_value.replace("{{sortname|", "").replace("}}", "");
                        sort_value = sort_value.substring(0, sort_value.indexOf("||")).replace("|", " ").trim();
                    }
                } else {
                    //this is the case where the data has a sort and a target {{sortname|first|last|target|sort}}
                    sort_value = sort_value.replace("{{sortname|", "").replace("}}", "").replaceAll("\\|", "\t");
                    String[] tmp = sort_value.split("\t");

                    StringBuffer sb_new_val = new StringBuffer();
                    for (int i = 0; i < tmp.length - 2; i++) {
                        sb_new_val.append(tmp[i]).append(" ");
                    }

                    String new_val = sb_new_val.toString().trim();
                    String target = tmp[tmp.length - 2];
                    if (target.contains("dab=")) {
                        target = new_val + "(" + target.replace("dab=", "") + ")";
                    }

                    sort_value = "[[" + new_val + "|" + target + "]]";
                }
                break;
            case 5:
                //this is the case where we have sorting an eventually a link or no-link {{sortname|first|last||sort|nolink=1}}
                if (sort_value.contains("||")) {
                    sort_value = sort_value.replace("{{sortname|", "").replace("}}", "");

                    String new_value = sort_value.substring(0, sort_value.indexOf("||"));
                    String[] tmp = sort_value.substring(sort_value.indexOf("||") + "||".length()).split("|");
                    String target_value = tmp[tmp.length - 1];

                    if (target_value.contains("dab=")) {
                        target_value = new_value + "(" + target_value.replace("dab=", "") + ")";
                    }
                    sort_value = "[[" + new_value + "|" + target_value + "]]";
                }
                break;
        }
        return remainder + " " + sort_value;
    }

    /**
     * Remove formatting for the table.
     *
     * @param value
     * @return
     */
    public static String removeFormattingClauses(String value) {
        if (table_formatting.isEmpty()) {
            init();
        }
        //remove sortname markup
        value = removeSortWikiMarkup(value);
        value = removeSortnameWikiMarkup(value);

        for (Map.Entry<String, String> format_entry : table_formatting) {
            if (value.contains(format_entry.getKey())) {
                value = value.replaceAll(format_entry.getValue(), "");
            }
        }

        if (value.contains("rowspan")) {
            value = value.replaceAll("rowspan=\"?[0-9]*\"?", "");
        }
        if (value.contains("colspan")) {
            value = value.replaceAll("colspan=\"?[0-9]*\"?", "");
        }
        return value.trim();
    }

    public static int[] getRowColSpan(String value) {
        int[] span = {1, 1};
        if (value.contains("rowspan")) {
            Matcher m = rowspan_pattern.matcher(value);
            if (m.find()) {
                String rsp = m.group().replace("rowspan=", "").replace("\"", "");

                if (!rsp.isEmpty()) {
                    span[0] = Integer.parseInt(rsp);
                }
            }
        }
        if (value.contains("colspan")) {
            Matcher m = colspan_pattern.matcher(value);
            if (m.find()) {
                String csp = m.group().replace("colspan=", "").replace("\"", "");
                if (!csp.isEmpty()) {
                    span[1] = Integer.parseInt(csp);
                }
            }
        }
        return span;
    }

    /**
     * Extract the values from snippets which link to other Wikipedia articles. Alongside we extract also the anchor label.
     *
     * @param value
     * @param values
     */
    public static int extractAnchorData(String value, List<Map.Entry<String, String>> values, int pos) {
        Matcher m = sq_entity_pattern.matcher(value);
        while (m.find()) {
            int start = m.start();
            if (start < pos) {
                continue;
            }
            String[] anchor_text = m.group().split("\\|");
            //extract the anchor text
            String article = anchor_text[0].replaceAll("\\[+|\\]+", "");
            String label = anchor_text.length == 2 ? anchor_text[1].replaceAll("\\[|\\]", "") : article;

            values.add(new AbstractMap.SimpleEntry<>(article, label));

            pos = m.end();
        }
        return pos;
    }

    /**
     * Extract the values from markup with curly brackets. These are usually not linked to other Wikipedia articles.
     *
     * @param value
     * @param values
     */
    public static int extractBracketedValue(String value, List<Map.Entry<String, String>> values, int pos) {
        Matcher m = cr_entity_pattern.matcher(value);
        while (m.find()) {
            int start = m.start();
            if (start < pos) {
                continue;
            }
            values.add(new AbstractMap.SimpleEntry<>(m.group().replaceAll("\\{+|\\}+", ""), ""));
            pos = m.end();
        }
        return pos;
    }

    /**
     * Assigns the TAB delimiters to a string line by taking into account cases where the line
     * might contain a wiki table markup field {{sortname}} which can contain || a string
     * used as a column splitter in wiki tables.
     *
     * @param value
     * @return
     */
    public static String assignTabDelimLine(String value) {
        Matcher m = sortname_pattern.matcher(value);

        //we generate all string  parts which do not contain a {{sortname}} field and then remerge
        StringBuffer sb = new StringBuffer();
        int prev_pos = 0;
        while (m.find()) {
            String sort_group = m.group();
            int start = m.start();
            int end = m.end();

            String sub_string = value.substring(prev_pos, start).replaceAll("\\|\\|", "\t");
            sb.append(sub_string).append(" ").append(sort_group);
            prev_pos = end;
        }
        String sub_string = value.substring(prev_pos).replaceAll("\\|\\|", "\t");
        sb.append(" ").append(sub_string);
        return sb.toString();
    }


}
