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

lucene.initVM()
analyzer = analysis.standard.StandardAnalyzer()
path = Paths.get('index')
directory = FSDirectory.open(path)


def build_reviewer_index():
    # Reset the directory
    shutil.rmtree(constant.DIRECTORY + '/reviewer_index', ignore_errors=False, onerror=None)
    os.mkdir(constant.DIRECTORY + '/reviewer_index')

    path_to_index = constant.DIRECTORY + '/reviewer_index/'

    with open(constant.JSON_LINES_FILE) as file:
        print('Building Reviewer Index')
        # Go through the lines and sort reviewers with all their reviews alphabetically
        for line in file:
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


def build_pylucene_index():
    indexer = engine.IndexWriter(directory=directory, analyzer=analyzer)
    indexer.set('review', engine.Field.Text, stored=True)  # default indexed text settings for documents

    with open(constant.JSON_LINES_FILE) as file:
        for line in tqdm(file, total=5_700_000):
            indexer.add(review=line)  # add document
        indexer.commit()  # commit changes and refresh searcher


def search_pylucene_index(query):
    indexer = engine.IndexSearcher(directory=directory, analyzer=analyzer)

    hits = indexer.search(query, field='review', count=10)  # parsing handled if necessary
    reviews = []
    for hit in hits:
        review = hit['review']
        reviews.append(review)
    return reviews
