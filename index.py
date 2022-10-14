import re
import os
import shutil

from tqdm import tqdm
import pandas as pd
import constant


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


def build_all_index():
    print('Building All Index')
