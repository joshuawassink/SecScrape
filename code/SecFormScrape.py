# import our libraries
import requests
import urllib
from bs4 import BeautifulSoup
import os
import re
import concurrent.futures
import pandas as pd
import string
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import recordlinkage
# Set project folder & define path variable
os.chdir('C:/Users/joshu/projects/SecScrape')
path = 'data'

# Define dictionaries to store final data and track problem files and forms
finalData_dict = {}
failure_dict = {}
# Create lists within finalData_dict to store the extracted data and to record tables that couldn't be scraped
finalData_dict['dataSet_list'] = []
finalData_dict['failedScrapes_list'] = []

"""Defining a URL Builder"""


def make_url(base_url, comp):
    url = base_url
    # add each component to the base url
    for r in comp:
        url = '{}/{}'.format(url, r)
    return url


"""Define a heading generator"""


def printText(text):
    print('-'*100)
    print(text)
    print('-'*100)


"""Defining a title builder to title downloaded files"""


def make_title(string, path=path, ext='txt'):
    filename = string.rpartition('/')[2]
    filename = filename.rpartition('.')
    filename = filename[0].replace('.', '_')+filename[1]+ext
    return filename


"""Pulling the daily index filings"""
# This code creats a dictionary of key value pairs in which the key is the url for an SEC daily index and the value is the corresponding file name that can be used to save the daily index locally
# define the urls needed to make the request, let's start with all the daily filings


def getDailyIndex(start_year, end_year, output_dict, filetype='sec'):
    """start: the first year of interest
        end: the last year of interest (inclusive)
        filetype: the type of file to filter for (default is all)
        output_dict: the repository for urls and filenames (this must be created before execution)"""
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
    # The daily-index filings, require a year and content type (html, json, or xml).
    start_year = start_year
    end_year = end_year
    period = range(start_year, end_year+1)
    for year in period:
        year = str(year)
        year_url = make_url(base_url, [year, 'index.json'])
        # request the content as a JSON strucutre and then decode it
        content = requests.get(year_url)
        decoded_content = content.json()
        decoded_content
        # the structure is almost identical to other json requests we've made. Go to the item list.
        # AGAIN ONLY GRABBING A SUBSET OF THE FULL DATASET
        for item in decoded_content['directory']['item']:
            # get the name of the folder
            # The daily-index filings, require a year, a quarter and a content type (html, json, or xml).
            qtr_url = make_url(base_url, [year, item['name'], 'index.json'])
            # Request, the new url and again it will be a JSON structure.
            file_content = requests.get(qtr_url)
            decoded_content = file_content.json()
            # for each file in the directory items list, print the file type and file href.
            # AGAIN DOING A SUBSET
            for file in decoded_content['directory']['item']:
                if file['name'].find(filetype) != -1:
                    file_url = make_url(base_url, [year, item['name'], file['name']])
                    filename = make_title(file_url)
                    output_dict[file_url] = filename


"""Parsing the master IDX file"""


def parseIndex(url, filename, output_list, header_list, failure_list, downloaded_data, download=True):
    # This function parses the master index file to extract form-specific URLs
    # url refers to the form url obtained by the index
    # file name is the name of the file
    # output_list defines the list to store the urls contained in the file
    # header_list refers to a list that will store header info from each file
    # failure_list refers to files that could not be successfully decoded
    downloaded_data = downloaded_data
    file_url = url
    filename = filename
    # Determine If the file should be downloaded
    if download:
        # assign full file name
        full_filename = path+'/'+filename
        # Check if the file has already been downloaded
        if filename not in downloaded_data:
            # request the url and extract the content!
            content = requests.get(file_url).content
            # Write the content to the disc
            with open(full_filename, 'wb') as f:
                f.write(content)
        # Open the file and decode the byte stream.
        f = open(full_filename, 'rb')
        content = f.read()
    else:
        # request the url and extract the content
        content = requests.get(file_url).content
        # decode the content
    # Attempt to decode the files
    try:
        data = content.decode("utf-8").split('\n')
        if download:
            f.close()
        """Now to process the data"""
        # Filter rows containign no alphanumeric characters to remove formatting lines
        data = [s for s in data if re.search(r'\w+', s)]
        # Remove everything prior to the headers by looking for the end of the header and grabbing it's index
        for index, item in enumerate(data):
            if "ftp://ftp.sec.gov/edgar/" in item:
                start_ind = index+1
        # define a new dataset.
        data_format = data[start_ind:]
        # now we need to break the data into sections, this way we can move to the final step of getting each row value.
        data_list = []
        for index, row in enumerate(data_format):
            # First, Split into seperate values
            row = row.split('|')
            # Second, append the urls with the SEC route url
            row = [item.replace(item, "https://www.sec.gov/Archives/"+item).strip()
                   if '.txt' in item else item.strip() for item in row]
            # Third, append to the master_data list
            # If header, add to header
            if index == 0:
                header_list.append(row)
            else:
                data_list.append(row)
        output_list.append(data_list)
    except:
        failure_list.append(filename)


"""Creating a Document Dictionary"""


def indexExtract(master_data, master_headers, master_reports):
    """Then, loop through each document in the master list."""
    # First enumerating the list of document lists
    for index1, document in enumerate(master_data):
        # Next, enumerate each document list
        for index2, row in enumerate(document[1:]):
            # Then, initialize a new dictionary to hold the data from each document
            document_dict = {}
            # Then interate over the document info and read it into the dictionary
            for index3, value in enumerate(row):
                # Note that the layered indexing facilitates matching by item label
                document_dict[master_headers[index1][index3]] = value
            # Finally, append each document dictionary to the master_reports list
            master_reports.append(document_dict)
    # Then, check the success rate
    length = 0
    for row in master_data:
        length += len(row)
    printText('Parsed {} ({:.2f}%) out of {} indexes'.format(
        len(master_reports), (len(master_reports)/length)*100, length))


"""Define a function to filter document_dicts by form Type and return urls and company names"""


def getDocByType(document_dict, formType, filtered_dict):
    # First, filter the document dictionaries by form type
    if document_dict['Form Type'] == formType:
        document_dict['documents_url'] = document_dict['File Name'].replace(
            '-', '').replace('.txt', '/index.json')
        filtered_dict.append(document_dict)


"""Function to process the 10-K form"""
# This function will request the index in .json format and then extract the filing summary if one is available


def processForm(document_dict, fileSummary_list, noSummary_list):
    # If the form has a filing summary, this function will append the url to call the filing summary to the output list. Otherwise, it will append the form url to the failure list
    url = document_dict['documents_url']
    content = requests.get(url).json()
    length = len(fileSummary_list)
    for file in content['directory']['item']:
        # Grab the filing summary and create a new url leading to the file so we can download it.
        if file['name'] == 'FilingSummary.xml':
            xml_summary = r"https://www.sec.gov" + content['directory']['name'] + "/" + file['name']
            fileSummary_list.append(
                [xml_summary, document_dict['Company Name'], document_dict['CIK'], document_dict['Date Filed'][:4]])
    if length == len(fileSummary_list):
        noSummary_list.append([url, document_dict['Company Name'],
                               document_dict['CIK'], document_dict['Date Filed'][:4]])


"""Define a function to parse the file summaries and extract the specific file names"""


def parseFileSummary(url, output_list):
    # First, define a base url to use when downloading the reports
    base_url = url[0].replace('FilingSummary.xml', '')
    # Next, request and parse the content
    content = requests.get(url[0]).content
    soup = BeautifulSoup(content, 'lxml')
    # find the 'myreports' tag because this contains all the individual reports submitted.
    reports = soup.find('myreports')
    # Extract list of reports
    reports = reports.find_all('report')
    # Create a list to store all the individual components of the report
    master_reports = []
    # append company name and CIK
    master_reports.append(url[1:4])
    # let's create a dictionary to store all the different parts we need.
    for report in reports:
        report_dict = {}
        # loop through each report in the 'myreports' tag
        if report.shortname != None:
            report_dict['name_short'] = report.shortname.text
        if report.longname != None:
            report_dict['name_long'] = report.longname.text
        if report.position != None:
            report_dict['position'] = report.position.text
        if report.reporttype != None:
            report_dict['report_type'] = report.reporttype.text
        if report.menucategory != None:
            report_dict['category'] = report.menucategory.text
        if report.htmlfilename != None:
            report_dict['url'] = base_url + report.htmlfilename.text
        if report_dict != {}:
            master_reports.append(report_dict)
    if len(master_reports) > 0:
        output_list.append(master_reports)


"""The next three functions work together to extract the data from each table and organize it into nested data dictionaries. The first function, statementUrls, extracts the urls that correspond to the desired tables. Then, it loops through the next two functions for each url. First, statementData extracts each table's headers, section labels and row data. Then, tableScrape organizes the data into nested dictionaries in which each column header is a dictionary key containing a list of key:value pairs for each variable. This ensurs that each value is associated with the proper row name within the proper column time period."""

"""Define a function to scrape the financial statements"""


def statementUrls(fileSummary, report_list, data_dict):
    data_dict = data_dict
    # define the statements we want to look for.
    # Create a list to hold the table urls
    statementUrls_list = []
    # For each report dict
    companyInfo_list = fileSummary[0]
    # For each report dict
    for report_dict in fileSummary[1:]:
        # For each item in the list of desired reports
        for item in report_list:
            # if the report dict name can be found in the list of desired reports
            if len(item.findall(report_dict['name_short'])) > 0:
                # append the url to the statement urls list
                statementUrls_list.append(report_dict['url'])
    statementUrls_list = statementUrls_list[:-1]
    # Finally, for each statement url
    for statement in statementUrls_list:
        # Get the data from the table url
        statement_data = statementData(statement)
        # scrape the data and add it in long format to the data dictionary
        tableScrape(statement_data, companyInfo_list, data_dict)


"""Extract the statement data from an xml table"""


def statementData(statement):
    # define a dictionary that will store the different parts of the statement.
    statement_data = {}
    statement_data['headers'] = []
    statement_data['sections'] = []
    statement_data['data'] = []
    # request the statement file content
    content = requests.get(statement).content
    report_soup = BeautifulSoup(content, 'html')
    # find all the rows, figure out what type of row it is, parse the elements, and store in the statement file list.
    for index, row in enumerate(report_soup.table.find_all('tr')):
        # first let's get all the elements.
        cols = row.find_all('td', {'class': ['tl', 'th', 'td', 'pl', 'nump', 'text']})
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
    return statement_data


"""Store the extracted data in nested dictionaries of columns:lists and then rowNames:values"""


def tableScrape(url_table, companyInfo_list, data_dict):
    companyInfo_list = companyInfo_list
    data_dict = data_dict
    # Define a regular expression to extract non-decimal values
    non_decimal = re.compile(r'[^\d.]+')
    # Assign the headers to a list
    header = url_table['headers']
    # In the headers span multiple columns
    if len(header) > 1:
        # Create new headers that combine the values from the two columns
        new_header = [[header[0][0]]]
        # For each column, concatenate the subheader with the macro header
        for index, value in enumerate(header[1]):
            new_header[0].append(header[1][index])
        header = new_header
    CIK = companyInfo_list[1]
    year = companyInfo_list[2]
    header[0] = [head+'_' + year + '_' + CIK for head in header[0]]
    #header = [head+'_' +companyInfo_list[1] for head in header]
    # Assign the data to a list
    data = url_table['data']
    try:
        for col in range(1, len(header[0])):
            # Create a new dictionary for each column in the data table
            if header[0][col] not in list(data_dict.keys()):
                # Use the key 'column header'+'CIK'
                data_dict[header[0][col]] = {}
        for index, row in enumerate(data):
            for i in range(1, len(row)):
                # Once, i.e., the first time through
                if index == 0:
                    # Assign the header to a value
                    data_dict[header[0][i]]['header'] = header[0][i]
                # For non-missing celss
                if row[i] != '':
                    # If row is not a footnote
                    data_dict[header[0][i]][row[0].lower()] = non_decimal.sub('',
                                                                              row[i]).replace('(', '-')
                else:
                    data_dict[header[0][i]][row[0].lower()] = 'NaN'
                # Append company information
                data_dict[header[0][i]]['CIK'] = companyInfo_list[1]
                data_dict[header[0][i]]['Company Name'] = companyInfo_list[0]
                data_dict[header[0][i]]['Year'] = companyInfo_list[2]
    except IndexError as error:
        print('{} could not be scraped'.format(companyInfo_list[0]))
        finalData_dict['failedScrapes_list'].append(companyInfo_list[0])


"""
Define master funcction to extract data from SEC forms
start_year: The first year (YYYY) from which you would like to  extract data
end_year:   The last inclusive year (YYYY) from which you would like to  extract data
formType: The type of form you would like to extract (e.g., 10-K)
finalData_dict:a dictionary to store the extracted data
failure_dict: a dictionary to record failed extractions
reports: the types of tables within the form that you would like to scrape
"""


def scrapeSec(start_year, end_year, formType, finalData_dict, failure_dict, reports):
    # Create a dictionary to hold the index file urls
    DailyIndex_dict = {}
    # Execute getDailyIndex for desired period
    printText('Building daily index from {} to {}'.format(start_year, end_year))
    getDailyIndex(start_year, end_year, DailyIndex_dict, 'master')
    # Store the filing urls
    finalData_dict['index_urls'] = DailyIndex_dict
    """Parse all index files to extract data storage info"""
    # Empty list to store index information
    master_data = []
    # Empty list to hold .txt files that were unsuccessfully decodes
    failed_decodes = []
    # Empty list to store file headers
    master_headers = []
    # Create a list of already downloaded files to prevent duplication
    downloaded_data = os.listdir(path)
    # Iterate parseIndex over all of the urls in finalData_dict['index_urls']
    printText('Parsing index files')
    [parseIndex(url, filename, master_data, master_headers, failed_decodes, downloaded_data)
     for url, filename in finalData_dict['index_urls'].items()]
    # Store failed decodes in the failure dictionary
    failure_dict['failed_decodes'] = failed_decodes
    # Report performance
    printText('{} ({:.2f}%) out of {} index files successfully parsed'.format(len(master_data), len(
        master_data)/(len(master_data)+len(failed_decodes))*100, len(master_data)+len(failed_decodes)))
    """Create a Document Dictionary to itemize each individual report"""
    printText('Creating document dictionary')
    # First, initialize a master list to hold the document inventories
    master_reports = []
    """Then, loop through each index to extract a document dictionary for every report filed."""
    indexExtract(master_data, master_headers, master_reports)
    """Create a filtered list containing the desired form type"""
    # Generate a new list for the desired form type
    filtered_list = []
    # Use a list comprehension to filter master reports and select info for 10-Ks
    printText('Filtering document dictionary to extract {} forms'.format(formType))
    [getDocByType(document_dict, '10-K', filtered_list) for document_dict in master_reports]
    finalData_dict['filtered_list'] = filtered_list
    """Parse the 10K file summaries"""
    # Create empty lists to store summary output & failed extractions
    fileSummary_list = []
    noSummary_list = []
    # Iterate over the files to extract the filing sumarry urls
    printText('Getting file summary urls from SEC.gov. This may take a while')
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        [executor.submit(processForm(document_dict, fileSummary_list, noSummary_list))
         for document_dict in finalData_dict['filtered_list'][:20]]
    printText('{} ({:.2f}%) out of {} index files successfully parsed'.format(len(fileSummary_list), len(
        fileSummary_list)/len(finalData_dict['filtered_list'])*100, len(finalData_dict['filtered_list'])))
    # Store the file summaries
    finalData_dict['noSummary_list'] = noSummary_list
    finalData_dict['fileSummary_list'] = fileSummary_list
    # Create an empty list to store the file summaries
    fileSummaryInfo = []
    # Parse file summaries
    printText('Parsing file summaries. This may take a while')
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        [executor.submit(parseFileSummary(url, fileSummaryInfo)) for url in fileSummary_list]
    finalData_dict['fileSummary_list'] = fileSummary_list
    """Scrape data from the desired files"""
    # Generate an empty dictionary to store the key:value pairs from the xml tables
    data_dict = {}
    # Generate regular expressions to identify desired tables
    report_list = reports
    for i, item in enumerate(report_list):
        report_list[i] = re.compile(report_list[i], re.IGNORECASE)
    # Execute statementUrls to extract the financial data and store it in nested dictionaries
    printText('Extracting data from tables. This may take a while')
    [statementUrls(summary, report_list, data_dict) for summary in fileSummaryInfo]
    finalData_dict['tableData'] = data_dict


scrapeSec(
    start_year=2020,
    end_year=2020,
    formType='10-K',
    finalData_dict=finalData_dict,
    failure_dict=failure_dict,
    reports=['consolidated balance sheets']
)
