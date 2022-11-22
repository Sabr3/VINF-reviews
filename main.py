import os
import re
import constant
import index
import datetime
# from tqdm import tqdm
from termcolor import colored


def let_user_pick(options):
    for idx, element in enumerate(options):
        print("{}) {}".format(idx + 1, element))

    i = input(colored("Pick a number from the list above: ", "yellow"))
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

    for line in file:
        reg = r'\"reviewer\":\"' + username + '\\w*\"'
        try:
            found = re.search(reg, line, re.IGNORECASE)
            if found:
                reviewers.add(found.group().split(':')[1].replace('"', ''))
        except AttributeError:
            pass

    return reviewers


def parse_reliability(line):
    reg = '\"helpful\":-?\\d+'
    try:
        found = re.search(reg, line)
        if found:
            # Extract numbers from helpful tag
            numbers = re.findall(r'-?\d+', found.group())
            return int(numbers[0])
    except AttributeError:
        pass


def parse_spoiler_count(line):
    reg = '\"spoiler\":1'
    try:
        is_spoiler = bool(re.search(reg, line))
        return 1 if is_spoiler else 0
    except AttributeError:
        pass


def get_earlier_date(date1, date2):
    # date in yyyy/mm/dd format
    d1 = date1.split('-')
    d1 = datetime.datetime(day=int(d1[0]), month=int(d1[1]), year=int(d1[2]))
    d2 = date2.split('-')
    d2 = datetime.datetime(day=int(d2[0]), month=int(d2[1]), year=int(d2[2]))
    return date1 if d1 < d2 else date2

#
# # Parse date to machine-readable form
# def parse_date(date):
#     date = date.split(' ')
#     year = date[2]
#     month = {i for i in constant.MONTH_DICT if constant.MONTH_DICT[i] == date[1]}.pop()
#     day = date[0]
#     date = '{0}/{1}/{2}'.format(year, month, day)
#     return date


# Parse date to human-readable form
def reverse_parse_date(date):
    date = date.split('-')
    day = date[0]
    month = constant.MONTH_DICT[int(date[1])]
    year = date[2]
    date = '{0} {1} {2}'.format(day, month, year)
    return date


def parse_first_review_date(line, review_date):
    found_review_date = review_date
    reg = '\"review_date\":\"\\d+-\\d+-\\d+\"'
    try:
        found = re.search(reg, line)
        if found:
            # date in dd MM yyyy format
            found_review_date = found.group().split(':')[1].replace('"', '')
    except AttributeError:
        pass
    if not review_date:
        return found_review_date
    else:
        return get_earlier_date(found_review_date, review_date)


def parse_avg_rating(line):
    reg = '\"rating\":\"\\d+\\.*\\d*\"'
    try:
        found = re.search(reg, line)
        if found:
            return float(found.group().split(':')[1].replace('\"', ''))
    except AttributeError:
        return -1


def parse_single_reviewer_data(file, username):
    reviewers_reliability = 0
    spoilers_count = 0
    all_reviews_count = 0
    first_review_date = False
    total_rating = 0

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

        except AttributeError:
            pass

    if all_reviews_count == 0:
        print(colored('Reviewer has not posted any review yet!', 'red'))
    else:
        print(colored('Showing data about: {}', 'green').format(username))
        print('Reviewer\'s total reviews:', all_reviews_count)
        print('Reviewer\'s first review date:', reverse_parse_date(first_review_date))
        print('Reviewer\'s spoiler rate: {:0.2f}%'.format(spoilers_count / all_reviews_count * 100))
        print('Reviewer\'s average rating: {:0.1f}/10'.format(total_rating / all_reviews_count))
        print('Reviewer\'s reliability:', reviewers_reliability)


def parse_data_reviewer_index(files):
    if not files:
        print(colored('No results found for that query! Please try again!', 'red'))
        return

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
    if not username:
        return
    files = []
    for file in os.listdir("reviewer_index"):
        if bool(re.match(username, file, re.IGNORECASE)):
            files.append(os.path.join("reviewer_index", file))
    return files


def get_query_type(query):
    reg = "reviewer:(\"|\')\\w+(\"|\')"
    # Just so that we find out that user wanted to search for the reviewer
    query = query.replace(' ', '')
    try:
        found = re.search(reg, query, re.IGNORECASE)
        if found:
            return 'REVIEWER'
    except AttributeError:
        pass
    return 'FULL-TEXT'

#
# def is_phrase_query(query):
#     if (query.startswith('"') and query.endswith('"')) or (query.startswith("'") and query.endswith("'")):
#         return True
#     return False


def parse_reviewer(review):
    reg = r'\"reviewer\":\"(\w|[^\w"])+\"'
    try:
        found = re.search(reg, review, re.IGNORECASE)
        if found:
            return found.group().split(':')[1].replace('"', '')
    except AttributeError:
        pass


def parse_movie(review):
    reg = r'\"movie\":\"(\w|\W)+\"'
    try:
        found = re.search(reg, review, re.IGNORECASE)
        if found:
            return found.group().split('","rating":')[0].replace('"movie":"', '')
    except AttributeError:
        pass


def extract_review_detail(line):
    reg = '\"review\":\"(\\w|\\W)+\"'
    try:
        found = re.search(reg, line)
        if found:
            return found.group().split('","helpful":')[0].replace('"review":"', '')
    except AttributeError:
        return -1


def parse_reviewers_from_reviews(result_reviews):
    if not result_reviews:
        return
    reviews_details_list = []
    for review in result_reviews:
        reviewer = parse_reviewer(review)
        movie = parse_movie(review)
        review_detail = extract_review_detail(review)
        reviews_details_list.append({'reviewer': reviewer, 'movie': movie, 'review_detail': review_detail})

    return reviews_details_list


def show_result_reviewers_and_extract_one(result_reviewers):
    if not result_reviewers:
        return
    print(colored('I found these reviews for your query. Please choose one from the list and I will show you '
                  'data about the reviewer of that movie!', 'green'))
    res = let_user_pick(result_reviewers)
    reviewer = result_reviewers[res]['reviewer']
    return reviewer


def main():
    print(colored('Welcome to FoxSearch (Collection of IMDb reviews from many years). Please choose what you want to '
                  'do!', 'blue'))
    options = ['Search for the query', 'Build index', 'Exit the program']
    try:
        res = let_user_pick(options)
    except:
        print(colored('Wrong number. Please try again!', 'red'))
        res = -1
        pass

    while res != 2:
        # If we're building index
        if res == 1:
            print('Which index do you want to build/rebuild?')
            # index_options = ['Reviewers index', 'TF-IDF index']
            index_options = ['Reviewers index', 'PyLucene Index']
            index_res = let_user_pick(index_options)
            if index_res == 0:
                index.build_reviewer_index()

            elif index_res == 1:
                # index.build_tf_idf_index()
                index.build_pylucene_index()

        elif res == 0:
            print('Enter your search query:')
            query = input()

            query_type = get_query_type(query)

            # If QueryType is REVIEWER use ReviewerIndex
            if query_type == 'REVIEWER':
                # index = reviewer_index
                query = query.replace('\'', '"')
                username = query.split('"')[1]
                files = use_reviewer_index(username)
                parse_data_reviewer_index(files)

            # If QueryType is ALL use AllSearchIndex
            elif query_type == 'FULL-TEXT':
                result_reviews = index.search_pylucene_index(query)
                result_reviewers = parse_reviewers_from_reviews(result_reviews)
                reviewer = show_result_reviewers_and_extract_one(result_reviewers)
                # Put the result to reviewer index
                files = use_reviewer_index(reviewer)
                parse_data_reviewer_index(files)

        elif res == 2:
            print('Bye Bye!')
            return 0

        print(colored('-------------------------------------------------------------------------------------------------------', 'green'))
        print(colored('-------------------------------------------------------------------------------------------------------', 'green'))
        print('Please choose what you want to do next!')
        options = ['Search for the query', 'Build index', 'Exit the program']
        try:
            res = let_user_pick(options)
        except:
            print(colored('Wrong number. Please try again!', 'red'))
            res = -1
            continue

    print(colored('Bye Bye!', 'blue'))


if __name__ == '__main__':
    main()
