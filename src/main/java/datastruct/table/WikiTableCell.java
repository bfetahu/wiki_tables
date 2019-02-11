package datastruct.table;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 5/22/17.
 */
public class WikiTableCell implements Serializable {
    //check if the value spans across rows and/or columns
    public int col_span = 1;
    public int row_span = 1;

    //the column to which this cell is assigned
    public WikiColumnHeader col_header;

    //the value assigned to this cell.
    public String value;

    //hold the parsed and interlinked values
    public List<Map.Entry<String, String>> values;

    public WikiTableCell(WikiColumnHeader col_header) {
        this.col_header = col_header;
    }

    public WikiTableCell(Element cell, WikiColumnHeader col_header) {
        this.col_header = col_header;
        this.value = cell.text();

        if (cell.hasAttr("colspan")) {
            col_span = Integer.parseInt(cell.attr("colspan"));
        }
        if (cell.hasAttr("rowspan")) {
            row_span = Integer.parseInt(cell.attr("rowspan"));
        }

        //check if it contains any hyperlink
        this.linkValues(cell);
    }


    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(value.toString());
        if (values != null && !values.isEmpty()) {
            sb.append(" == ");
            values.forEach(s -> sb.append(s).append(";"));
        }
        return sb.toString();
    }

    /**
     * Extract the structured information from the HTML cell value.
     *
     * @param element
     */
    public void linkValues(Element element) {
        if (values == null) {
            values = new ArrayList<>();
        }
        Elements structured_href = element.select("a");
        if (structured_href != null) {
            for (Element struct_href : structured_href) {
                String value = struct_href.text();
                String ref = struct_href.attr("title");
                values.add(new AbstractMap.SimpleEntry<>(ref, value));
            }
        } else {
            values.add(new AbstractMap.SimpleEntry<>(element.text(), ""));
        }
    }
}
