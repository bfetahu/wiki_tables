package utils;

import java.util.*;
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
            String label = anchor_text.length == 2 ? anchor_text[1] : "";

            values.add(new AbstractMap.SimpleEntry<>(article, label));

            pos  = m.end();
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
            values.add(new AbstractMap.SimpleEntry<>(m.group().replaceAll("\\{+|\\}+",""), ""));
            pos = m.end();
        }
        return pos;
    }

}
