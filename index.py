import math
import re
import os
import shutil
from numpy import dot
from tqdm import tqdm
import pandas as pd
import numpy as np
import constant
from array import array
from collections import defaultdict


INDEX = {}
TF = {}
IDF = {}
INDEX_LOADED = False


def load_data():
    return pd.read_json(constant.FILENAME)


def optimize_df(dataframe):
    # dataframe = dataframe.drop('review_detail', axis=1)
    dataframe.to_json("json_reviews.jl", orient="records", lines=True)


def prepare_data():
    df_raw = load_data()
    optimize_df(df_raw)


def build_reviewer_index():
    prepare_data()
    # Reset the directory
    shutil.rmtree(constant.DIRECTORY + '/reviewer_index', ignore_errors=False, onerror=None)
    os.mkdir(constant.DIRECTORY + '/reviewer_index')

    path_to_index = constant.DIRECTORY + '/reviewer_index/'

    with open("json_reviews.jl") as file:
        print('Building Reviewer Index')
        # Go through the lines and sort reviewers with all their reviews alphabetically
        for line in tqdm(file, total=100_000):
            reg = '\"reviewer\":\"\\w+\"'
            try:
                found = re.search(reg, line)
                if found:
                    username = found.group().split('"')[3]
                    filename = '{path}{username}.txt'.format(path=path_to_index, username=username)
                    with open(filename, 'a') as f:
                        f.write(line)
            except AttributeError:
                pass


def get_terms(doc):
    doc = doc.lower()
    doc = re.sub(r'\\W', ' ', doc)  # put spaces instead of non-alphanumeric characters
    terms = doc.split()

    terms = [term for term in terms if term not in constant.STOPWORDS]
    terms = [constant.LEMMATIZER.lemmatize(term) for term in terms]
    terms = [constant.STEMMER.stem(term) for term in terms]
    return terms


def extract_review_id(line):
    reg = '\"review_id\":\"\\w+\"'
    try:
        found = re.search(reg, line)
        if found:
            return found.group().split(':')[1].replace('\"', '')
    except AttributeError:
        return -1


def extract_review_detail(line):
    reg = '\"review_detail\":\"(\\w|\\W)+\"'
    try:
        found = re.search(reg, line)
        if found:
            return found.group().removeprefix('"review_detail":"').split('","helpful":')[0]
    except AttributeError:
        return -1


def write_tf_idf_index_to_file(index, docs_count, tf, df):
    with open(constant.INDEX_FILE, 'w') as file:
        docs_count = int(docs_count)
        print('docs_count:{}'.format(docs_count), file=file)  # Print to file

        for term in index.keys():
            term_positions_list = []
            for term_position in index[term]:
                doc_id = term_position[0]  # review_id aka doc_id
                positions = term_position[1]  # positions of the term in the document
                term_positions_list.append(':'.join([str(doc_id), ','.join(map(str, positions))]))
            term_positions_data = ';'.join(term_positions_list)
            tf_data = ','.join(map(str, tf[term]))
            idf_data = '%4f' % (1 + np.log(docs_count / df[term]))
            print('|'.join((term, term_positions_data, tf_data, idf_data)), end="\n", file=file)


def build_tf_idf_index():
    print('Building TF-IDF Index')

    prepare_data()

    docs_count = 0
    index = defaultdict(list)  # the inverted index
    tf = defaultdict(list)  # term frequencies of terms in documents
    df = defaultdict(int)  # document frequencies of terms in the corpus

    with open('json_reviews.jl', 'r', encoding='latin-1') as file:

        for idx, line in tqdm(enumerate(file), total=100_000):
            doc_id = extract_review_id(line)  # Here take review ID as document ID
            review_detail = extract_review_detail(line)
            terms = get_terms(review_detail)

            if terms is None:
                continue

            docs_count += 1

            term_dict = {}
            for i, term in enumerate(terms):
                if term in term_dict:
                    term_dict[term][1] = np.append(term_dict[term][1], [i])  # Append another index of term to the array
                else:
                    term_dict[term] = [doc_id, np.array([i])]  # create dictionary key-value pair with doc_id and
                    # indexes of positions, where the term is present

            # Copy values to index
            for term, term_positions in term_dict.items():
                index[term].append(term_positions)

            # Normalize the doc
            norm = 0
            for _, term_positions in term_dict.items():
                norm += pow(len(term_positions), 2)
            norm = math.sqrt(norm)

            # calculate tf and df weights
            for term, term_positions in term_dict.items():
                tf_value = term_positions[1].size / norm
                tf[term].append('%.4f' % tf_value)
                df[term] += 1

        write_tf_idf_index_to_file(index, docs_count, tf, df)


def read_tf_idf_index():
    index = {}
    tf = {}
    idf = {}

    with open(constant.INDEX_FILE) as file:
        docs_count = int(file.readline().rstrip().removeprefix('docs_count:'))

        for line in tqdm(file, total=100_000):
            line = line.rstrip()
            term, term_positions, tfl, idft = line.split(
                '|')
            term_positions = term_positions.split(';')  # term_positions = ['doc_id1:pos1,pos2','doc_id2:pos1,pos2']
            term_positions_parsed = []
            for t in term_positions:
                t = t.split(":")  # term_positions = [['doc_id1', 'pos1,pos2'], ['doc_id2', 'pos1,pos2']]
                term_positions_parsed.append([t[0], map(int, t[1].split(','))])  # term_positions = [['doc_id1',
                # [pos1,pos2], ['doc_id2', [pos1,pos2]]]

            index[term] = term_positions_parsed
            # read tf
            tfl = tfl.split(',')
            tf[term] = tfl  # map(float, tfl)
            # read idf
            idf[term] = float(idft)

    print('Index loaded\n')
    global INDEX_LOADED
    INDEX_LOADED = True
    global INDEX
    INDEX = index
    global TF
    TF = tf
    global IDF
    IDF = idf
    return index, tf, idf


def dot_product(vec1, vec2):
    if len(vec1) != len(vec2):
        return 0
    return dot(vec1, vec2)


def parse_review_ids(result_docs):
    review_ids = []
    for doc in result_docs:
        review_ids.append(doc[0])
    return review_ids


def rank_documents(query_terms, docs, index, tf, idf):
    doc_vectors = defaultdict(lambda: [0] * len(query_terms))
    query_vector = [0] * len(query_terms)

    for term_index, term in enumerate(query_terms):
        query_vector[term_index] = idf[term]
        for docIndex, (doc, postings) in enumerate(index[term]):
            if doc in docs:
                tfScores = list(tf[term])
                doc_vectors[doc][term_index] = float(tfScores[docIndex])

    print(query_vector)
    doc_scores = [[doc, dot_product(doc_vector, query_vector), doc_vector, query_vector] for doc, doc_vector in
                  doc_vectors.items()]

    doc_scores.sort(key=lambda x: x[1], reverse=True)  # sort by dot_product
    result_docs = [d for d in doc_scores][:10]  # Max number of results
    print(result_docs)
    result_review_ids = parse_review_ids(result_docs)

    return result_review_ids


def free_text_query(query, index, tf, idf):
    query_terms = get_terms(query)
    if len(query_terms) == 0:
        print('Empty query!')
        return []

    doc_list_intersection = set()
    for term in query_terms:
        print('Looking for ' + term)
        if term in index:
            term_positions = index[term]
            docs = [t[0] for t in term_positions]
            # If doc_list_intersection is empty initialize it, so that there's intersection
            if not doc_list_intersection:
                doc_list_intersection = set(docs)
            # Logical AND
            doc_list_intersection = doc_list_intersection & set(docs)

    doc_list_intersection = list(doc_list_intersection)

    if len(doc_list_intersection) == 0:
        print('No results for the specified query!')
        return

    return rank_documents(query_terms, doc_list_intersection, index, tf, idf)


def search_tf_idf_index(query):
    if not INDEX_LOADED:
        index, tf, idf = read_tf_idf_index()
    index = INDEX
    tf = TF
    idf = IDF
    return free_text_query(query, index, tf, idf)
