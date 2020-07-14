# import our libraries
import requests
import urllib
from bs4 import BeautifulSoup
import os
import pandas as pd
import re

# Set project diectory and assign data directory to path
os.chdir(r'C:\Users\joshu\projects\SecScrape')
path = '/data'

"""Defining a URL Builder"""
# let's first make a function that will make the process of building a url easy.


def make_url(base_url, comp):
    url = base_url
    # add each component to the base url
    for r in comp:
        url = '{}/{}'.format(url, r)
    return url


"""Pulling the daily index filings"""
# define the urls needed to make the request, let's start with all the daily filings


def get_DailyIndex(year):
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
    # The daily-index filings, require a year and content type (html, json, or xml).
    year_url = make_url(base_url, [str(year), 'index.json'])

    # Display the new Year URL
    print('-'*100)
    print('Building the URL for Year: {}'.format(str(year)))
    print("URL Link: " + year_url)
    # request the content for each year, remember that a JSON strucutre will be sent back so we need to decode it.
    content = requests.get(year_url)
    decoded_content = content.json()
    decoded_content
    # the structure is almost identical to other json requests we've made. Go to the item list.
    # AGAIN ONLY GRABBING A SUBSET OF THE FULL DATASET
    for item in decoded_content['directory']['item']:
        # get the name of the folder
        print('-'*100)
        print('Pulling url for Quarter: {}'.format(item['name']))
        # The daily-index filings, require a year, a quarter and a content type (html, json, or xml).
        qtr_url = make_url(base_url, [str(year), item['name'], 'index.json'])
        # print out the url.
        print("URL Link: " + qtr_url)
        # Request, the new url and again it will be a JSON structure.
        file_content = requests.get(qtr_url)
        decoded_content = file_content.json()
        print('-'*100)
        print('Pulling file urls')
        # for each file in the directory items list, print the file type and file href.
        # AGAIN DOING A SUBSET
        for file in decoded_content['directory']['item']:
            file_url = make_url(base_url, [str(year), item['name'], file['name']])
            if file_url.find('master') != -1:
                print("File URL Link: " + file_url)
                start = file_url.find('master.')
                end = file_url.find('.idx')
                file_number = file_url[start+7:end]
                file_name = r'master_'+file_number+r'.txt'
                DailyIndexMaster_list['file_name'].append(file_name)
                DailyIndexMaster_list['file_url'].append(file_url)


DailyIndexMaster_list = {
    'file_name': [],
    'file_url': []
}
[get_DailyIndex(i) for i in range(2000, 2021)]
DailyIndexMaster_list['file_name'][0:10]

"""Parsing the master IDX file"""
# define a url, in this case I'll just take one of the urls up above.


def parseIDX(file_name):
    if file_name.find('https') != -1:

        # request that new content, this will not be a JSON STRUCTURE!
        content = requests.get(file_name).content
        content
        # we can always write the content to a file, so we don't need to request it again.
        with open('data/master_20190102.txt', 'wb') as f:
            f.write(content)
    # let's open it and we will now have a byte stream to play with.
    with open('data/master_20190102.txt', 'rb') as f:
        byte_data = f.read()

    # Now that we loaded the data, we have a byte stream that needs to be decoded and then split by double spaces.
    data = byte_data.decode("utf-8").split('  ')
    data
    # We need to remove the headers, so look for the end of the header and grab it's index
    for index, item in enumerate(data):
        if "ftp://ftp.sec.gov/edgar/" in item:
            start_ind = index

    # define a new dataset with out the header info.
    data_format = data[start_ind + 1:]
    master_data = []
    # now we need to break the data into sections, this way we can move to the final step of getting each row value.
    for index, item in enumerate(data_format):

        # if it's the first index, it won't be even so treat it differently
        if index == 0:
            clean_item_data = item.replace('\n', '|').split('|')
            clean_item_data = clean_item_data[4:]
        else:
            clean_item_data = item.replace('\n', '|').split('|')
        # Now, we iterate over the entire list of items to organize the contents into the appropriate rows
        for index, row in enumerate(clean_item_data):
            # When you find a text file, use it to index the row information
            if '.txt' in row:
                mini_list = clean_item_data[index-4:index+1]
                if len(mini_list) != 0:
                    mini_list[4] = "https://www.sec.gov/Archives/" + mini_list[4]
                    master_data.append(mini_list)


master_data
"""Creating our Document Dictionary"""
# loop through each document in the master list.
for index, document in enumerate(master_data):

    # create a dictionary for each document in the master list
    document_dict = {}
    document_dict['cik_number'] = document[0]
    document_dict['company_name'] = document[1]
    document_dict['form_id'] = document[2]
    document_dict['date'] = document[3]
    document_dict['file_url'] = document[4]
    # Replace the master list of lists with a list of dictionaries
    master_data[index] = document_dict


"""Filtering by File Type"""
# by being in a dictionary format, it'll be easier to get the items we need.
for document_dict in master_data:

    # if it's a 10-K document pull the url and the name.
    if document_dict['form_id'] == '10-K':

        # get the components
        comp_name = document_dict['company_name']
        docu_url = document_dict['file_url']

        print('-'*100)
        print(comp_name)
        print(docu_url)

# Create a url that takes us to the Detail filing landing page
file_url_adj = docu_url.split('.txt')
file_url_archive = file_url_adj[0] + '-index.htm'

print('-'*100)
print('The Filing Detail can be found here: {}'.format(file_url_archive))

# Create a url that will take us to the archive folder
archive_url = docu_url.replace('.txt', '').replace('-', '')

print('-'*100)
print('The Archive Folder can be found here: {}'.format(archive_url))

# Create a url that will take us the Company filing Archive
company_url = docu_url.rpartition('/')
company_url
print('-'*100)
print('The Company Archive Folder can be found here: {}'.format(company_url[0]))


"""Parsing Company 10-Ks from the SEC"""
# define the base url needed to create the file url.


def get10kIndex(url):
    base_url = r"https://www.sec.gov"

    # convert a normal url to a document url
    normal_url = url

    def urlConvert(url):
        url = url.replace('-', '').replace('.txt', '/index.json')
        return url

    # define a url that leads to a 10k document landing page
    documents_url = urlConvert(normal_url)

    # request the url and decode it.
    content = requests.get(documents_url).json()
    content
    for file in content['directory']['item']:

        # Grab the filing summary and create a new url leading to the file so we can download it.
        if file['name'] == 'FilingSummary.xml':

            xml_summary = base_url + content['directory']['name'] + "/" + file['name']

            print('-' * 100)
            print(base_url)
            print(content['directory']['name'])
            print('File Name: ' + file['name'])
            print('File Path: ' + xml_summary)
            return xml_summary


# define a new base url that represents the filing folder. This will come in handy when we need to download the reports.
base_url = xml_summary.replace('FilingSummary.xml', '')

# request and parse the content
content = requests.get(xml_summary).content
soup = BeautifulSoup(content, 'lxml')
soup
# find the 'myreports' tag because this contains all the individual reports submitted.
reports = soup.find('myreports')
reports
# I want a list to store all the individual components of the report, so create the master list.
master_reports = []

# loop through each report in the 'myreports' tag but avoid the last one as this will cause an error.
for report in reports.find_all('report')[:-1]:

    # let's create a dictionary to store all the different parts we need.
    report_dict = {}
    report_dict['name_short'] = report.shortname.text
    report_dict['name_long'] = report.longname.text
    report_dict['position'] = report.position.text
    report_dict['category'] = report.menucategory.text
    report_dict['url'] = base_url + report.htmlfilename.text

    # append the dictionary to the master list.
    master_reports.append(report_dict)

    # print the info to the user.
    print('-'*100)
    print(base_url + report.htmlfilename.text)
    print(report.longname.text)
    print(report.shortname.text)
    print(report.menucategory.text)
    print(report.position.text)

"""Grabbing the Financial Statements"""
# Create a blank list to hold the financial statement urls
statements_url = []
master_reports
# Loop through the master reports to pull desired financial table urls from each
for report_dict in master_reports:
    # Define the items that you want to pull
    item1 = r"Consolidated Balance Sheets"
    item2 = r"Consolidated Statements of Operations and Comprehensive Income (Loss)"
    item3 = r"Consolidated Statements of Cash Flows"
    item4 = r"Consolidated Statements of Stockholder's (Deficit) Equity"

    # store them in a list.
    report_list = [item1, item2, item3, item4]

    # if the short name can be found in the report list.
    if report_dict['name_short'] in report_list:

        # print some info and store it in the statements url.
        print('-'*100)
        print(report_dict['name_short'])
        print(report_dict['url'])

        statements_url.append(report_dict['url'])
statements_url
"""Scraping the Financial Statements"""
# This section is constructed to pool all the information from each table into a single dataframe
statements_data = []
content = requests.get(statement_url).content
report_soup = BeautifulSoup(content, 'html')
# Loop over each dataframe in statements_url
for statement in statements_url:
    # define a dictionary that will store the different parts of the statement.
    statement_data = {}
    statement_data['headers'] = []
    statement_data['sections'] = []
    statement_data['data'] = []

    # Request the statement file content
    content = requests.get(statement).content
    report_soup = BeautifulSoup(content, 'html')

    # find all the rows, figure out what type of row it is, parse the elements, and store in the statement file list.
    for index, row in enumerate(report_soup.table.find_all('tr')):

        # first let's get all the elements.
        cols = row.find_all('td')

        # if it's a regular row and not a section or a table header
        if (len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0):
            reg_row = [ele.text.strip() for ele in cols]
            statement_data['data'].append(reg_row)

        # if it's a regular row and a section but not a table header
        elif (len(row.find_all('th')) == 0 and len(row.find_all('strong')) != 0):
            sec_row = cols[0].text.strip()
            statement_data['sections'].append(sec_row)

        # finally if it's not any of those it must be a header
        elif (len(row.find_all('th')) != 0):
            hed_row = [ele.text.strip() for ele in row.find_all('th')]
            statement_data['headers'].append(hed_row)

        else:
            print('We encountered an error.')

    # append it to the master list.
    statements_data.append(statement_data)


# Format the data into a pandas DataFrame
statements_data[0]
pd.DataFrame(statements_data[0]['data'])
