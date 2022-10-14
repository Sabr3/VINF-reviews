import os
import time
import re
import constant
import index
import datetime
from tqdm import tqdm


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

    for line in file:
        reg = '\"reviewer\":\"' + username + '\"'
        try:
            found = re.search(reg, line)
            if found:
                all_reviews_count += 1
                spoilers_count += parse_spoiler_count(line)
                reviewers_reliability += parse_reliability(line)
                first_review_date = parse_first_review_date(line, first_review_date)
                rating = parse_avg_rating(line)
                if rating != -1 and rating is not None:
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


def parse_data_no_index(username):
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


def parse_data_reviewer_index(files):
    class File:
        def __init__(self, name, length, username):
            self.name = name
            self.length = length
            self.username = username

    files_with_length = []
    reviewers = []
    # Let user pick which reviewer he wants, sorted by number of reviews
    for file in files:
        with open(file) as f:
            for count, line in enumerate(f):
                pass
        file_with_stats = File(file, count + 1, file.replace('reviewer_index/', '').replace('.txt', ''))
        files_with_length.append(file_with_stats)
    files_with_length_sorted = sorted(files_with_length, key=lambda x: x.length, reverse=True)
    for fwls in files_with_length_sorted:
        reviewers.append(fwls.username)

    reviewer = get_single_reviewer(reviewers)
    reviewer_file = 'reviewer_index/{}.txt'.format(reviewer)
    with open(reviewer_file) as file:
        parse_single_reviewer_data(file, reviewer)


def use_reviewer_index(username):
    files = []
    for file in os.listdir("reviewer_index"):
        if bool(re.match(username, file, re.IGNORECASE)):
            files.append(os.path.join("reviewer_index", file))
    return files


def get_query_type(query):
    reg = r'reviewer:(\"|\')\w+(\"|\')'
    try:
        found = re.search(reg, query, re.IGNORECASE)
        if found:
            return 'REVIEWER'
    except AttributeError:
        pass
    return 'ALL'


def main():
    print('Welcome to FoxSearch (Collection of IMDb reviews from many years). Please choose what you want to do!')
    options = ['Search for the query', 'Build index']
    res = let_user_pick(options)

    # If we're building index
    if res == 1:
        print('Which index do you want to build/rebuild?')
        index_options = ['Reviewers index', 'All index']
        index_res = let_user_pick(index_options)
        if index_res == 0:
            index.build_reviewer_index()
        elif index_res == 1:
            index.build_all_index()

    elif res == 0:
        print('Enter your search query:')
        query = input()

        query_type = get_query_type(query)
        data = ''

        # If QueryType is REVIEWER use ReviewerIndex
        if query_type == 'REVIEWER':
            # index = reviewer_index
            query = query.replace('\'', '"')
            username = query.split('"')[1]
            start = time.time()
            files = use_reviewer_index(username)
            parse_data_reviewer_index(files)
            end = time.time()
            print("Execution time in seconds: ", (end - start))
            return 0

        # If QueryType is ALL use AllSearchIndex
        elif query_type == 'ALL':
            # index = all_index
            print('All index')

        start = time.time()
        parse_data_no_index(query)
        end = time.time()
        print("Execution time in seconds: ", (end - start))


if __name__ == '__main__':
    main()
