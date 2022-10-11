import pandas as pd
import time
import re
import constant
import datetime
from tqdm import tqdm


def load_data():
    df_raw = pd.read_json(constant.FILENAME)
    return df_raw


def optimize_df(dataframe):
    dataframe = dataframe.drop('review_detail', axis=1)
    dataframe.to_json("json_reviews.jl", orient="records", lines=True)


def prepare_data():
    df_raw = load_data()
    optimize_df(df_raw)


def let_user_pick(options):
    for idx, element in enumerate(options):
        print("{}) {}".format(idx + 1, element))

    i = input("Enter number: ")
    while True:
        if 0 < int(i) <= len(options):
            return int(i) - 1
        i = input("Wrong number! Please try again: ")


def get_single_reviewer(reviewers):
    if not reviewers:
        return None
    elif len(reviewers) > 1:
        # Make a list of reviewers for user to choose from
        print('I found {count} reviewers for your query. Please choose one from the list!'.format(count=len(reviewers)))
        res = let_user_pick(reviewers)
        reviewer = reviewers[res]
    else:
        reviewer = reviewers[0]
    return reviewer


def parse_usernames(file, username):
    reviewers = set()

    for line in tqdm(file, total=100_000):
        reg = r'\"reviewer\":\"' + username + '\\w*\"'
        try:
            found = re.search(reg, line, re.IGNORECASE)
            if found:
                reviewers.add(found.group().split(':')[1].replace('"', ''))
        except AttributeError:
            pass

    return reviewers


def parse_reliability(line):
    reg = '\"helpful\":\\[\"\\d+\",\"\\d+\"\\]'
    try:
        found = re.search(reg, line)
        if found:
            # Extract numbers from helpful tag
            numbers = re.findall(r'\d+', found.group())
            return int(numbers[0]) - int(numbers[1])
    except AttributeError:
        pass


def parse_spoiler_count(line):
    reg = '\"spoiler_tag\":1'
    try:
        spoiler_tag = re.search(reg, line)
        return 1 if bool(spoiler_tag) else 0
    except AttributeError:
        pass


def get_earlier_date(date1, date2):
    # date in yyyy/mm/dd format
    d1 = date1.split('/')
    d1 = datetime.datetime(int(d1[0]), int(d1[1]), int(d1[2]))
    d2 = date2.split('/')
    d2 = datetime.datetime(int(d2[0]), int(d2[1]), int(d2[2]))
    return date1 if d1 < d2 else date2


# Parse date to machine-readable form
def parse_date(date):
    date = date.split(' ')
    year = date[2]
    month = {i for i in constant.MONTH_DICT if constant.MONTH_DICT[i] == date[1]}.pop()
    day = date[0]
    date = '{0}/{1}/{2}'.format(year, month, day)
    return date


# Parse date to human-readable form
def reverse_parse_date(date):
    date = date.split('/')
    year = date[0]
    month = constant.MONTH_DICT[int(date[1])]
    day = date[2]
    date = '{0} {1} {2}'.format(day, month, year)
    return date


def parse_first_review_date(line, review_date):
    found_review_date = review_date
    reg = '\"review_date\":\"\\d+\\s\\w+\\s\\d+\"'
    try:
        found = re.search(reg, line)
        if found:
            # date in dd MM yyyy format
            raw_date = found.group().split(':')[1].replace('"', '')
            found_review_date = parse_date(raw_date)
    except AttributeError:
        pass
    if not review_date:
        return found_review_date
    else:
        return get_earlier_date(found_review_date, review_date)


def parse_avg_rating(line):
    reg = '\"rating\":\\d+\\.*\\d*'
    try:
        found = re.search(reg, line)
        if found:
            return float(found.group().split(':')[1].replace('\'', ''))
    except AttributeError:
        return -1


def parse_single_reviewer_data(file, username):
    reviewers_reliability = 0
    spoilers_count = 0
    all_reviews_count = 0
    first_review_date = False
    total_rating = 0
    no_rating_reviews = 0

    for line in tqdm(file, total=100_000):
        reg = '\"reviewer\":\"' + username + '\"'
        try:
            found = re.search(reg, line)
            if found:
                all_reviews_count += 1
                spoilers_count += parse_spoiler_count(line)
                reviewers_reliability += parse_reliability(line)
                first_review_date = parse_first_review_date(line, first_review_date)
                rating = parse_avg_rating(line)
                if rating != -1:
                    total_rating += rating
                else:
                    no_rating_reviews += 1
        except AttributeError:
            pass

    if all_reviews_count == 0:
        print('Reviewer has not posted any review yet!')
    else:
        print('Reviewer\'s total reviews', all_reviews_count)
        print('Reviewer\'s first review date:', reverse_parse_date(first_review_date))
        print('Reviewer\'s spoiler rate: {:0.2f}%'.format(spoilers_count/all_reviews_count * 100))
        print('Reviewer\'s average rating: {:0.1f}/10'.format(total_rating/(all_reviews_count-no_rating_reviews)))
        print('Reviewer\'s reliability:', reviewers_reliability)


def parse_data(username):
    reviewer = None
    r_username = username
    with open("json_reviews.jl") as file:
        if reviewer is None:
            reviewers = list(parse_usernames(file, r_username))
            reviewer = get_single_reviewer(reviewers)
        while reviewer is None:
            print('No results found! Try again!')
            file.seek(0)  # Return to beginning
            r_username = input()
            reviewers = list(parse_usernames(file, r_username))
            reviewer = get_single_reviewer(reviewers)

        file.seek(0)
        print("Showing results for reviewer", reviewer)
        parse_single_reviewer_data(file, reviewer)


def main():
    print('Enter reviewer\'s username:')
    r_username = input()

    start = time.time()
    prepare_data()
    parse_data(r_username)
    end = time.time()
    print("Execution time in seconds: ", (end - start))


if __name__ == '__main__':
    main()
