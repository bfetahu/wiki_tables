# TableNET: Table data and Ground-Truth


In TableNet we have extracted all the table data from a set of more than 530k Wikipedia articles. Here you can find for download all the table data, and the evaluation datasets we used for building our models in TableNet. 

### Table Data

The table data consists of more than 3M tables. The data format is in JSON, and we extract the schema, the rows, the columns of a table. The JSON snippet below provides an example of the data format.

```javascript
{"caption":"TABLE_CAPTION", "markup": "TABLE_HTML", "id": TBL_ID, 
"header":[
  {"level":HEADER_LEVEL, "columns":[ 
    {"name": "COL_NAME", "col_span": COL_SPAN, "row_span": ROW_SPAN, 
    "value_dist":[{"value":"VAL","count":COUNT}]}
 ]}
], 
"rows":[ 
  {"row_index":ROW_INDEX, "values":[
    {"column":"COL_NAME", "col_index":COL_IDX, "value":"CELL_VALUE",  
    "structured_values":[{"structured":"VAL", "anchor":"ANCHOR_VAL"}]
    }]
  }]
}
```

`All the extracted tables can be downloaded from the following` [url](http://l3s.de/~fetahu/wiki_tables/data/table_data/html_data/structured_html_table_data.json.gz).


### Evaluation Ground-Truth for Table Relations

The evaluation files consists of table pairs with their corresponding relations. The relations are for a set of 50 seed Wikipedia articles, correspondingly for all their tables. 

The pairs correspond to a manually filtered set of article pairs, that is seed articles and all other Wikipedia article that contain a table, however, filtered iteratively and manually based on carefully designed matching filters that consider the semantics of the table relations **subPartOf** and **equivalent**.

| Source Article | Matching Article | Source Section  | Matching Section | Source Table ID | Matching Table ID | Label |
|--- | --- | --- | --- | --- | --- | --- |
|1979–80 Liga Alef | 2013 UEFA European Under-21 Championship | North Division | Seeding | 8765 | 2904 | noalignment|
|2012 Catalunya GP2 and GP3 Series rounds | 1926 Italian Grand Prix | Qualifying | Classification | 5503 | 4417| equivalent|
|Air Force Falcons football statistical leaders | Air Force Falcons football statistical leaders | Passing yards | Passing yards | 5018953| 5018952 |subpartof| 

`The evaluation file consist of 17k pairs and it can be downloaded from the following` [url](http://l3s.de/~fetahu/wiki_tables/data/gt_data/table_pair_evaluation_eq_sub_irrel_labels.tsv).


