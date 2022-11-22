import re
import os
import shutil
import constant
import lucene
from org.apache.lucene import analysis
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
from tqdm import tqdm
from lupyne import engine
from termcolor import colored

lucene.initVM()
analyzer = analysis.standard.StandardAnalyzer()
path = Paths.get('index')
directory = FSDirectory.open(path)


def build_reviewer_index():
    # Reset the directory
    reviewer_index_path = constant.DIRECTORY + '/reviewer_index'
    if os.path.exists(reviewer_index_path):
        shutil.rmtree(reviewer_index_path, ignore_errors=False, onerror=None)
    os.mkdir(constant.DIRECTORY + '/reviewer_index')

    path_to_index = constant.DIRECTORY + '/reviewer_index/'

    with open(constant.JSON_LINES_FILE) as file:
        print('Building Reviewer Index')
        # Go through the lines and sort reviewers with all their reviews alphabetically
        for line in tqdm(file, total=5_500_000):
            reg = r'\"reviewer\":\"[\w|\W ]+\"'
            try:
                found = re.search(reg, line)
                if found:
                    username = found.group().split('"')[3]
                    filename = '{path}{username}.txt'.format(path=path_to_index, username=username)
                    with open(filename, 'a') as f:
                        f.write(line)
            except AttributeError:
                pass


def build_pylucene_index():
    lucene_index_path = constant.DIRECTORY + '/index'
    if os.path.exists(lucene_index_path):
        shutil.rmtree(lucene_index_path, ignore_errors=False, onerror=None)

    indexer = engine.IndexWriter(directory=directory, analyzer=analyzer)
    indexer.set('review', engine.Field.Text, stored=True)  # default indexed text settings for documents

    with open(constant.JSON_LINES_FILE) as file:
        for line in tqdm(file, total=5_700_000):
            indexer.add(review=line)  # add document
        indexer.commit()  # commit changes and refresh searcher


def search_pylucene_index(query):
    indexer = engine.IndexSearcher(directory=directory, analyzer=analyzer)
    reviews_count = 10

    print(colored('Enter how many reviews do you want to show (default=10):', 'yellow'))
    res = input()
    try:
        reviews_count = int(res)
    except: 
        pass

    hits = indexer.search(query, field='review', count=reviews_count)  # parsing handled if necessary
    reviews = []
    for hit in hits:
        review = hit['review']
        reviews.append(review)
    return reviews
