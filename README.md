# TableNet: An Approach for Determining Fine-grained Relations for Wikipedia Tables

This repository contains the code for extracting tables from Wikipedia, and additionally contains the models for determining if for a pair of tables one of the relations *subPartOf* or *equivalent* holds. 

## HTML Article Extraction
To extract the HTML content of the Wikipedia articles, please use the following command:
```
java -cp *COMPILED_PROJECT*.jar HTMLTableExtractor -option crawl_table_articles -seed SEED_FILE -out OUT_FILE
```

- `SEED_FILE: should consist of a list of articles that you are interested in extracting the HTML content. Each line contains an article name.`
- `OUT_FILE: the output file where the extracted HTML content is stored.`

## Table Extraction

To extract the tables, please use the following command: 
```
java -cp *COMPILED_PROJECT*.jar HTMLTableExtractor -option parse_tables -in INPUT_FILE -out OUT_FILE
```
- `INPUT_FILE: should point to the HTML content of the Wikipedia articles. The HTML content is JSON escaped, such that each line consists of an article. You can use the previous operation (*HTML Article Extraction*) to extract the HTML content.`
- `OUTPUT_FILE: the file where the extracted tables will be outputed in JSON format.`

## Table Alignment

We have uploaded a sample of the [ground-truth data](https://github.com/bfetahu/wiki_tables/blob/master/table_pair_labels_100_sample.tsv) and extracted [table data](https://github.com/bfetahu/wiki_tables/data/). We will release the full dataset upon acceptance of our paper. 




## Citation
Please cite the following work, when using this dataset or approach.
```
@article{fetahu2019tablenet,
  title={TableNet: An Approach for Determining Fine-grained Relations for Wikipedia Tables},
  author={Fetahu, Besnik and Anand, Avishek and Koutraki, Maria},
  booktitle={Proceedings of the 2019 World Wide Web Conference on World Wide Web, {WWW} 2019, San Francisco, USA, May 13-17},
  year={2019}
}
```
