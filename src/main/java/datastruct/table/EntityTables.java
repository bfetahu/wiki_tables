package datastruct.table;

import entities.WikiEntity;
import entities.WikiSection;
import datastruct.wikitable.WikiTable;
import org.apache.commons.lang3.StringEscapeUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by besnik on 5/25/17.
 */
public class EntityTables implements Serializable {
    public WikiEntity entity;
    public Map<String, List<WikiTable>> tables;

    public EntityTables() {
        tables = new HashMap<>();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();

        //print in JSON format the tables.
        sb.append("{\"entity\":\"").append(StringEscapeUtils.escapeJson(entity.title)).append("\", \"sections\":[");
        int section_counter = 0;
        for (String section_label : tables.keySet()) {
            if (tables.get(section_label).isEmpty()) {
                continue;
            }

            WikiSection section = entity.getSection(section_label);
            if (section_counter != 0) {
                sb.append(",");
            }
            sb.append("{\"section\":\"").append(StringEscapeUtils.escapeJson(section.section_label)).
                    append("\",\"text\":\"").append(StringEscapeUtils.escapeJson(section.section_text)).
                    append("\", \"level\":").append(section.section_level).
                    append(",\"tables\":[");

            int table_counter = 0;
            for (WikiTable table : tables.get(section_label)) {
                if (table_counter != 0) {
                    sb.append(",");
                }
                sb.append("{\"caption\":\"").append(StringEscapeUtils.escapeJson(table.table_caption)).
                        append("\", \"table_data\":\"").append(StringEscapeUtils.escapeJson(table.markup)).
                        append("\"").append("}");
                table_counter++;
            }
            sb.append("]}");
            section_counter++;
        }
        sb.append("]}\n");

        return sb.toString();
    }
}
