import json
import re
import numpy as np
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class WikiTable:
    def __init__(self):
        self.table_rows = []
        self.columns = []
        self.table_caption = ''
        self.entity = ''
        self.section = ''
        self.label = 'NA'
        self.table_html = ''
        self.table_json = ''
        self.table_markup = ''
        self.label_confidence = 0.0
        self.table_id = -1
        self.column_meta_data = dict()

    def load_data(self, line):
        data_cells = line.split('\t')
        self.entity = data_cells[0]
        self.section = data_cells[1]
        self.table_id = int(data_cells[2])
        self.label = data_cells[3]
        self.label_confidence = float(data_cells[4])
        self.table_html = data_cells[5].strip()
        self.table_json = data_cells[6].strip()
        self.table_markup = data_cells[7].strip()

        self.parse_table_data()

    def load_json(self, json_line, entity, section, table_id, col_meta_parse=False):
        self.table_id = table_id
        self.entity = entity
        self.table_json = json_line
        self.section = section

        self.parse_table_data(col_meta_parse)

    '''
        Process the table HTML into the different sub-parts: (1) table caption, (2) table header (columns), (3) table cells
    '''

    def parse_table_data(self, col_meta_parse=False):
        # first check if the table has a caption
        tbljson = json.loads(self.table_json)
        self.table_caption = tbljson['caption']
        # get the table header and process it into the different rows and columns
        rows = tbljson['rows']
        for row in rows:
            row_values = row['values']

            row_cell_dict = dict()
            for cell in row_values:
                colname = cell['column']
                if colname not in self.columns:
                    self.columns.append(colname)
                row_cell_dict[colname] = cell['value']
            self.table_rows.append(row_cell_dict)

        # load the column meta data
        if col_meta_parse:
            header = tbljson['header']
            header = header[len(header) - 1]['columns']

            for col in header:
                colname = col['name']
                values = col['value_dist']

                self.column_meta_data[colname] = []
                for value in values:
                    self.column_meta_data[colname].append(value['value'])

    '''
        Here we compute simple features w.r.t the extracted table data from the Wikipedia markup.
        
        We will look into several features here. We look if certain values from the markup are 
        represent as columns or values in our parsed table data. In this way we may find missing 
        columns or values.
    '''

    def compute_features(self, bin_buckets=10):
        lines = [line.strip() for line in self.table_markup.decode('string_escape').split('\n')]

        # keep a dictionary of features here
        features = dict()

        # instead of keeping the line indexes we bucket them into 10 bins
        # so that all tables have the same representation space
        bins = np.linspace(1, len(lines), bin_buckets)

        for idx, line in enumerate(lines):
            idx = np.digitize(idx, bins).item(0) - 1
            # check first if the line is table definition or if it contains the table caption
            if re.match('{\|\s?class=', line) or line.startswith('|+') or len(line) == 0:
                continue

            '''
             otherwise we assume that here we are dealing either with the table header data or the cell values
             therefore, we will replace all characters which are ! or | which are used to delimiter the columns
             or cells in Wiki tables
            '''
            tokens = re.sub(r'!+|\|+|-', '\t', line).strip().split('\t')
            for token in tokens:
                if len(token) == 0:
                    continue

                has_token = False
                # check first if this token might be a column name
                if token in self.table_caption or token.startswith(
                        ('colspan', 'rowspan', 'bgcolor', 'style', 'class')):
                    continue
                # check here if the token is any of the values in our table cell data
                elif any((True for x in self.table_rows if token in x)):
                    has_token = True

                # add these missing tokens and their frequency for the respective lines
                if idx not in features.keys():
                    features[idx] = {}

                if token not in features[idx]:
                    features[idx][token] = [0, 0]

                if has_token:
                    features[idx][token][0] += 1
                else:
                    features[idx][token][1] += 1

        # we will aggregate for each row or feature ID the amount of tokens which are covered or uncovered
        aggr_features = {}
        bins = np.linspace(0, 1, bin_buckets)

        for idx in features:
            for token in features[idx]:
                total = sum(features[idx][token])
                covered = features[idx][token][0] / float(total)

                bin_val = np.digitize(covered, bins).item(0) - 1

                key_val = str(idx) + '-' + str(bin_val)
                if key_val not in aggr_features:
                    aggr_features[key_val] = 0
                aggr_features[key_val] += 1

        aggr_features['jacc'] = self.compute_html_markup_sim()
        aggr_features.update(self.column_value_dist(bins=bin_buckets))
        aggr_features['kl'] = self.compute_html_markup_kl()
        aggr_features['num_cols'] = len(self.columns)
        aggr_features['markup_double_exlamanation'] = self.table_markup.count('!!')
        aggr_features['markup_single_exlamanation'] = self.table_markup.count('!')
        return aggr_features

    '''
        Return the word distribution from the columns in this table
    '''

    def column_word_dist(self):
        column_features = {}
        columns = json.loads(self.table_json)['header'][len(json.loads(self.table_json)['header']) - 1]['columns']
        for idx, col in enumerate(columns):
            col_values = col['value_dist']

            for val in col_values:
                value = val['value'].encode('ascii', 'ignore').decode('ascii').decode('unicode-escape')
                wordlist = value.lower().split(' ')
                d = {v: wordlist.count(v) for v in wordlist}
                column_features.update({k: d.get(k, 0) + column_features.get(k, 0) for k in set(d.keys())})
        column_features = {k: v for k, v in column_features.iteritems() if v < 3}

        return column_features

    '''
        Compute features that are related w.r.t the distribution of column values.
    '''

    def column_value_dist(self, bins=10):
        column_features = {}
        # check the distribution of the column values
        columns = json.loads(self.table_json)['header'][len(json.loads(self.table_json)['header']) - 1]['columns']
        bin_buckets = np.linspace(1, len(columns), bins)
        for idx, col in enumerate(columns):
            col_values = col['value_dist']

            numbers, letters, other = 0, 0, 0
            for val in col_values:
                value = val['value']
                value = value.replace(' ', '').replace('"', '').replace('&', '')
                count = val['count']
                if value.isalpha():
                    letters += count
                elif value.isdigit():
                    numbers += count
                else:
                    other += count
            idx_key = str(np.digitize(idx, bin_buckets))
            total = float(numbers + letters + other)
            total = total == 0 and 1 or total

            if ('col-num-' + idx_key) not in column_features:
                column_features['col-num-' + idx_key] = []
                column_features['col-lt-' + idx_key] = []
                column_features['col-ot-' + idx_key] = []

            column_features['col-num-' + idx_key].append(numbers / total)
            column_features['col-lt-' + idx_key].append(letters / total)
            column_features['col-ot-' + idx_key].append(other / total)

        features = {}
        for key in column_features:
            features[key] = sum(column_features[key]) / len(column_features[key])
        return features

    '''
        Compute the similarity between the table as it appears in Wikipedia and its extracted version
    '''

    def compute_html_markup_sim(self):
        # compute the Jaccard sim
        clean_html = re.sub(r'<[^>]+>', ' ', self.table_html)
        set_tbl_markup = set(self.clean_wiki_markup().split(' '))
        set_tbl_html = set(clean_html.split(' '))
        score = float(len(set_tbl_markup & set_tbl_html)) / len(set_tbl_markup | set_tbl_html)
        return score

    '''
        Compute the KL divergence between the unigram language models 
        of the markup and the html representations of the table.
    '''

    def compute_html_markup_kl(self):
        clean_html = re.sub(r'<[^>]+>', ' ', self.table_html)
        html_wd = clean_html.lower().split(' ')
        d_html = {v: html_wd.count(v) for v in html_wd}

        clean_markup = self.clean_wiki_markup()
        html_mp = clean_markup.lower().split(' ')
        d_mp = {v: html_mp.count(v) for v in html_mp}

        keys = set(d_mp.keys()) | set(d_html.keys())
        a, b = np.zeros(len(keys)), np.zeros(len(keys))

        a_total = float(sum(d_html.values()))
        b_total = float(sum(d_mp.values()))

        epsilon = 0.001
        for idx, key in enumerate(keys):
            a[idx] = (key in d_html and d_html[key] or epsilon) / a_total
            b[idx] = (key in d_mp and d_mp[key] or epsilon) / b_total

        return np.sum(np.where(a != 0, a * np.log(a / b), 0))

    '''
        Clean the Wiki markup from the extracted table.
    '''

    def clean_wiki_markup(self):
        clean_markup = re.sub(r'\\\"', '"', self.table_markup.decode('unicode-escape'))
        clean_markup = re.sub(r'style=\"?(.*?)\"', ' ', clean_markup)
        clean_markup = re.sub(r'(\\n)|(class=\"?wikitable\"?)|(colspan=(.*?))|(rowspan=(.*?))|\|+', ' ',
                              clean_markup)
        clean_markup = re.sub('<ref(\s?name=(.*?))?>(.*?)</ref>', ' ', clean_markup)
        clean_markup = re.sub(r'</?span\s*>', '', clean_markup)
        clean_markup = re.sub(r'(bgcolor=\"(.*?)")|(align=\"(.*?)\")', ' ', clean_markup)
        clean_markup = re.sub(r'\]+|\[+|\"+|\'+|!+|\}+|\{+|\n+|\++', ' ', clean_markup).strip()
        return clean_markup
