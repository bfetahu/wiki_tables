# TableNET: Table data and Ground-Truth


In TableNet we have extracted all the table data from a set of more than 530k Wikipedia articles. Here you can find for download all the table data, and the evaluation datasets we used for building our models in TableNet. 

### Table Data

The table data consists of more than 3M tables. The data format is in JSON, and we extract the schema, the rows, the columns of a table. The JSON snippet below provides an example of the data format.

```javascript
{"caption":"TABLE_CAPTION", "markup":"TABLE_HTML", "id": TBL_ID, 
"header":[{"level":HEADER_LEVEL, "columns":[ 
  {"name":"COL_NAME", "col_span":COL_SPAN, "row_span":ROW_SPAN, "value_dist":[{"value":"VAL","count":COUNT}]}]}
], 
"rows":[ 
  {"row_index":ROW_INDEX, "values":[
    {"column":"COL_NAME", "col_index":COL_IDX, "value":"CELL_VALUE",  
    "structured_values":[{"structured":"VAL", "anchor":"ANCHOR_VAL"}]
    }]
  }
]
}
```
