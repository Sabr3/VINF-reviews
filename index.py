import re
import os
import shutil
import constant
import lucene
from org.apache.lucene import analysis, document, index, queryparser, search, util
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
import sys
import subprocess
from tqdm import tqdm

lucene.initVM()

# from numpy import dot
# from tqdm import tqdm
# import numpy as np
# import json
# from collections import defaultdict
#
#
# INDEX = {}
# TF = {}
# IDF = {}
# INDEX_LOADED = False


# def build_reviewer_index():
#     # Reset the directory
#     shutil.rmtree(constant.DIRECTORY + '/reviewer_index', ignore_errors=False, onerror=None)
#     os.mkdir(constant.DIRECTORY + '/reviewer_index')
#
#     path_to_index = constant.DIRECTORY + '/reviewer_index/'
#
#     with open(constant.JSON_LINES_FILE) as file:
#         print('Building Reviewer Index')
#         # Go through the lines and sort reviewers with all their reviews alphabetically
#         for line in file:
#             reg = '\"reviewer\":\"\\w+\"'
#             try:
#                 found = re.search(reg, line)
#                 if found:
#                     username = found.group().split('"')[3]
#                     filename = '{path}{username}.txt'.format(path=path_to_index, username=username)
#                     with open(filename, 'a') as f:
#                         f.write(line)
#             except AttributeError:
#                 pass

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def build_pylucene_index():
    install('termcolor')
    analyzer = analysis.standard.StandardAnalyzer()
    # Store an index on disk:
    path = Paths.get('index')
    directory = FSDirectory.open(path)
    config = index.IndexWriterConfig(analyzer)
    iwriter = index.IndexWriter(directory, config)
    doc = document.Document()
    with open(constant.JSON_LINES_FILE) as file:
        for line in tqdm(file, total=100000): 
            doc.add(document.Field("review", line, document.TextField.TYPE_STORED))
            iwriter.addDocument(doc)
    iwriter.close()

def search_pylucene_index(query):
    path = Paths.get('index')
    directory = FSDirectory.open(path)
    analyzer = analysis.standard.StandardAnalyzer()
    ireader = index.DirectoryReader.open(directory)
    isearcher = search.IndexSearcher(ireader)
    # Parse a simple query that searches for "text":
    parser = queryparser.classic.QueryParser("review", analyzer)
    query = parser.parse(query)
    print(query)
    hits = isearcher.search(query, 10).scoreDocs

    # Iterate through the results:
    for hit in hits:
        hitDoc = isearcher.doc(hit.doc)
        print('HitDoc:',hitDoc['review'])
    ireader.close()
    directory.close()

# def get_terms(doc):
#     doc = doc.lower()
#     doc = re.sub(r'\W', ' ', doc)  # put spaces instead of non-alphanumeric characters
#     terms = doc.split()
#
#     terms = [term for term in terms if term not in constant.STOPWORDS]
#     terms = [constant.LEMMATIZER.lemmatize(term) for term in terms]
#     # terms = [constant.STEMMER.stem(term) for term in terms]
#     return terms
#
#
# def extract_review_id(line):
#     reg = '\"review_id\":\"\\w+\"'
#     try:
#         found = re.search(reg, line)
#         if found:
#             return found.group().split(':')[1].replace('\"', '')
#     except AttributeError:
#         return -1
#
#
# def extract_review_detail(line):
#     reg = '\"review_detail\":\"(\\w|\\W)+\"'
#     try:
#         found = re.search(reg, line)
#         if found:
#             return found.group().removeprefix('"review_detail":"').split('","helpful":')[0]
#     except AttributeError:
#         return -1
#
#
# def write_tf_idf_index_to_file(index, docs_count, tf, df):
#     index_dict = {}
#     for term in index.keys():
#         term_positions_list = {}
#         for term_position in index[term]:
#             doc_id = term_position[0]  # review_id aka doc_id
#             positions = term_position[1].tolist()  # positions of the term in the document
#             term_positions_list[doc_id] = positions
#
#         idf = '%4f' % (1 + np.log(docs_count / df[term]))
#         index_dict[term] = {'term_positions_list': term_positions_list, 'tf': tf[term], 'idf': idf}
#
#     with open(constant.INDEX_FILE, 'w') as file:
#         # docs_count = int(docs_count)
#         # print('docs_count:{}'.format(docs_count), file=file)  # Print to file
#         file.write(json.dumps(index_dict))
#
#
# def build_tf_idf_index():
#     print('Building TF-IDF Index')
#
#     docs_count = 0
#     index = defaultdict(list)  # the inverted index
#     tf = defaultdict(list)  # term frequencies of terms in documents
#     df = defaultdict(int)  # document frequencies of terms in the corpus
#
#     with open(constant.JSON_LINES_FILE, 'r', encoding='latin-1') as file:
#
#         for idx, line in tqdm(enumerate(file), total=5_571_000):
#             doc_id = extract_review_id(line)  # Here take review ID as document ID
#
#             # review_detail = extract_review_detail(line)
#             # terms = get_terms(review_detail)
#             review_details = line.replace('"', '').replace("'", "")
#             terms = get_terms(review_details)
#
#             if terms is None:
#                 continue
#
#             docs_count += 1
#
#             term_dict = {}
#             for i, term in enumerate(terms):
#                 if term in term_dict:
#                     term_dict[term][1] = np.append(term_dict[term][1], [i])  # Append another index of term to the array
#                 else:
#                     term_dict[term] = [doc_id, np.array([i])]  # create dictionary key-value pair with doc_id and
#                     # indexes of positions, where the term is present
#
#             # Copy values to index
#             for term, term_positions in term_dict.items():
#                 index[term].append(term_positions)
#
#             # Normalize the doc
#             norm = 0
#             for _, term_positions in term_dict.items():
#                 norm += pow(len(term_positions), 2)
#             norm = math.sqrt(norm)
#
#             # calculate tf and df weights
#             for term, term_positions in term_dict.items():
#                 tf_value = term_positions[1].size / norm
#                 tf[term].append('%.4f' % tf_value)
#                 df[term] += 1
#
#         write_tf_idf_index_to_file(index, docs_count, tf, df)
#
#
# def read_tf_idf_index():
#     with open(constant.INDEX_FILE) as file:
#         # docs_count = int(file.readline().rstrip().removeprefix('docs_count:'))
#         data = file.read()
#         index_dict = json.loads(data)
#
#     print(colored('Index loaded\n', 'green'))
#     global INDEX_LOADED
#     INDEX_LOADED = True
#     global INDEX
#     INDEX = index_dict
#
#
# # Euclidean normalization
# def dot_product(vec1, vec2):
#     if len(vec1) != len(vec2):
#         return 0
#     return dot(vec1, vec2)
#
#
# def parse_review_ids(result_docs):
#     review_ids = []
#     for doc in result_docs:
#         review_ids.append(doc[0])
#     return review_ids
#
#
# def rank_documents(query_terms, docs, index_dict):
#     # Preparation of variables
#     doc_vectors = defaultdict(lambda: [0] * len(query_terms))
#     query_vector = [0] * len(query_terms)  # Query will have [0, 0, 0, 0] (if query length is 4)
#
#     for term_index, term in enumerate(query_terms):
#         idf = index_dict[term]['idf']
#         tf = index_dict[term]['tf']
#         query_vector[term_index] = float(idf)  # Assign to positions of query_vector the idf value of the term
#         for docIndex, doc_id in enumerate(index_dict[term]['term_positions_list'].keys()):
#             if doc_id in docs:
#                 tfScores = tf
#                 doc_vectors[doc_id][term_index] = float(tfScores[docIndex])  # Assign to doc_vectors the termIndexes
#                 # tfScores
#
#     doc_scores = [[doc, dot_product(doc_vector, query_vector), doc_vector, query_vector] for doc, doc_vector in
#                   doc_vectors.items()]  # Doc_scores
#
#     doc_scores.sort(key=lambda x: x[1], reverse=True)  # sort by dot_product
#     result_docs = [d for d in doc_scores][:10]  # Max number of results is 10
#     result_review_ids = parse_review_ids(result_docs)
#
#     return result_review_ids
#
#
# def phrase_query(query, index_dict):
#     print(colored('Looking for phrase: {}'.format(query), 'yellow'))
#     query_terms = get_terms(query)
#
#     doc_list_intersection = intersect_query_with_index(query_terms, index_dict)
#
#     doc_phrase_list = {}
#
#     for rw_id in doc_list_intersection:
#         doc_phrase_list[rw_id] = {}
#         for term in query_terms:
#             review_phrase = index_dict[term]['term_positions_list'][rw_id]
#             doc_phrase_list[rw_id][term] = review_phrase
#     phrase_match = {}
#
#     for idx, dpl in enumerate(doc_phrase_list):
#         phrase_match[dpl] = []
#         for i, term in enumerate(query_terms):
#             possible_matches = []
#             if i == 0:
#                 continue
#             for j in range(len(doc_phrase_list[dpl][term])):
#                 try:
#                     term_index_minus_one = doc_phrase_list[dpl][term][j] - 1
#                     list_index_minus_one = doc_phrase_list[dpl][query_terms[i-1]]
#                     if term_index_minus_one in list_index_minus_one:
#                         possible_matches.append(doc_phrase_list[dpl][term][j])
#                     doc_phrase_list[dpl][term] = possible_matches
#                 except:
#                     pass
#             if possible_matches:
#                 phrase_match[dpl] = phrase_match[dpl] + possible_matches
#
#     matched_review_ids = []
#
#     for dpl in phrase_match:
#         if len(phrase_match[dpl]) >= len(query_terms) - 1:
#             matched_review_ids.append(dpl)
#
#     return rank_documents(query_terms, matched_review_ids, index_dict)
#
#
# def intersect_query_with_index(query_terms, index_dict):
#     doc_list_intersection = set()
#     for term in query_terms:
#         if term in index_dict:
#             term_positions = index_dict[term]['term_positions_list']
#             docs = list(term_positions.keys())
#             # If doc_list_intersection is empty initialize it, so that there's intersection
#             if not doc_list_intersection:
#                 doc_list_intersection = set(docs)
#             # Logical AND
#             doc_list_intersection = doc_list_intersection & set(docs)
#
#     return list(doc_list_intersection)
#
#
# def free_text_query(query, index_dict):
#     query_terms = get_terms(query)
#     if len(query_terms) == 0:
#         print('Empty query!')
#         return []
#
#     doc_list_intersection = intersect_query_with_index(query_terms, index_dict)
#
#     if len(doc_list_intersection) == 0:
#         return
#
#     return rank_documents(query_terms, doc_list_intersection, index_dict)
#
#
# def search_tf_idf_index(query, is_phrase_query):
#     if not INDEX_LOADED:
#         read_tf_idf_index()
#     index = INDEX
#     if is_phrase_query:
#         return phrase_query(query, index)
#     return free_text_query(query, index)
