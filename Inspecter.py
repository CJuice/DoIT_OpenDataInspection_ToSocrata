"""
Inspect all datasets in MD DoIT Open Data Portal on Socrata, inventory null data, and output statistics in .csv files.

Use the Data Freshness Report dataset to identify all datasets to be inspected by this process.
For each dataset, request records from Socrata, inventory nulls in records, and repeat until all records have been
 processed.
Identify datasets without nulls, with nulls, and problem datasets that couldn't be processed.
Output a csv file providing an overview of the entire process findings, a csv file providing information on each
 dataset with nulls including insight by column, a csv file capturing all problematic datasets, and a csv file
 reporting on the performance of the script.
Author: CJuice
Date: 20180501
Revisions:

PENDING FUNCTIONALITY:
Only processes datasets that have been processed more recently than the last run. Reads the last date processed
 information from the api metadata for the dataset.
Compare previous results against current to see change in the datasets.
Multiprocessing use for speed. Note: No null records are "seen" and no csv files for datasets are written when
 multiprocessor approach is used. I think multiprocessing makes copies of this script for use in each processor
 so they are not sharing the same variables. I think this may be the issue.
"""

# IMPORTS
from collections import namedtuple
from datetime import date
import json
import os
import re
import time
import urllib2
from functools import partial
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

process_start_time = time.time()

# VARIABLES (alphabetic)
Variable = namedtuple("Variable", ["value"])
CORRECTIONAL_ENTERPRISES_EMPLOYEES_API_ID = Variable("mux9-y6mb")
CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE = Variable("MarylandCorrectionalEnterprises_JSON.json")
DATA_FRESHNESS_REPORT_API_ID = Variable("t8k3-edvn")
LIMIT_MAX_AND_OFFSET = Variable(20000)
MD_STATEWIDE_VEHICLE_CRASH_STARTSWITH = Variable("Maryland Statewide Vehicle Crashes")
OVERVIEW_STATS_FILE_NAME = Variable("_OVERVIEW_STATS")
PERFORMANCE_SUMMARY_FILE_NAME = Variable("__script_performance_summary")
PROBLEM_DATASETS_FILE_NAME = Variable("_PROBLEM_DATASETS")
REAL_PROPERTY_HIDDEN_NAMES_API_ID = Variable("ed4q-f8tm")
REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE = Variable("RealPropertyHiddenOwner_JSON.json")
ROOT_PATH_FOR_CSV_OUTPUT = Variable(r"E:\DoIT_OpenDataInspection_Project\OUTPUT_CSVs")
ROOT_URL_FOR_DATASET_ACCESS = Variable(r"https://data.maryland.gov/resource/")
THREAD_COUNT = Variable(8)

assert os.path.exists(REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE.value)
assert os.path.exists(CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE.value)


# FUNCTIONS (alphabetic)
def build_csv_file_name_with_date(today_date_string, filename):
    """
    Build a string, ending in .csv, that contains todays date and the input file name

    :param today_date_string: Intended to be the date the file will be created
    :param filename: Name of the file
    :return: string that is 'date_filename.csv'
    """
    return "{}_{}.csv".format(today_date_string, filename)

def build_dataset_url(url_root, api_id, limit_amount, offset, total_count):
    """
    Build the url used for each request for data from socrata

    :param url_root: Root socrata url common to all datasets
    :param api_id: ID specific to dataset of interest
    :param limit_amount: Upper limit on number of records to be returned in response to request
    :param offset: If more than one request, offset the range of records requested by this amount
    :param total_count: Current total number of records evaluated during processing of each individual dataset
    :return: String url
    """
    # if the record count exceeds the initial limit then the url must include offset parameter
    if total_count >= LIMIT_MAX_AND_OFFSET.value:
        return "{}{}.json?$limit={}&$offset={}".format(url_root, api_id, limit_amount, offset)
    else:
        return "{}{}.json?$limit={}".format(url_root, api_id, limit_amount)

def build_datasets_inventory(freshness_report_json_objects):
    """
    Process json response code for dataset names and api id's and build a dictionary for use

    :param freshness_report_json_objects: json returned by socrata per our request
    :return: Dictionary in format of ['dataset name' : 'dataset api id']
    """
    datasets_dictionary = {}
    for record_obj in freshness_report_json_objects:
        dataset_name = record_obj["dataset_name"]
        api_id = record_obj["link"]
        datasets_dictionary[dataset_name] = os.path.basename(api_id)
    return datasets_dictionary

def build_today_date_string():
    """
    Build a string representing todays date

    :return: string representing date formatted as Year Month Day
    """
    return "{:%Y%m%d}".format(date.today())

def calculate_percent_null_for_dataset(null_count_total, total_records_processed, number_of_fields_in_dataset):
    """
    Calculate the percent of all possible data values, not rows or columns, that are null

    :param null_count_total: Total number of null values
    :param total_records_processed: Total number or records processed
    :param number_of_fields_in_dataset: Total number of columns in the dataset
    :return: Percent value as a float
    """
    if number_of_fields_in_dataset is None:
        return 0
    else:
        total_number_of_values_in_dataset = float(total_records_processed*number_of_fields_in_dataset)
        if total_number_of_values_in_dataset == 0:
            return 0
        else:
            return (float(null_count_total/total_number_of_values_in_dataset)*100)

def calculate_time_taken(start_time):
    """
    Calculat the time difference between now and the value passed as the start time

    :param start_time: Time value representing start of processing
    :return: Difference value between start time and current time
    """
    return (time.time() - start_time)

def calculate_total_number_of_empty_values_per_dataset(null_counts_list):
    """
    Calculate the total number of null values in a dataset

    :param null_counts_list: List of numeric values representing null counts per column in dataset
    :return: Integer value representing total
    """
    return sum(null_counts_list)

def generate_freshness_report_json_objects(dataset_url):
    """
    Makes request to socrata url for dataset and processes response into json objects

    :param dataset_url: url to which the request is made
    :return: json objects in dictionary form
    """
    json_objects = None
    url = dataset_url
    req = urllib2.Request(url)
    try:
        response = urllib2.urlopen(req)
    except urllib2.URLError as e:
        if hasattr(e, "reason"):
            print("build_datasets_inventory(): Failed to reach a server. Reason: {}".format(e.reason))
        elif hasattr(e, "code"):
            print("build_datasets_inventory(): The server couldn't fulfill the request. Error Code: {}".format(e.code))
        exit()
    else:
        html = response.read()
        json_objects = json.loads(html)
    return json_objects

def grab_field_names_for_mega_columned_datasets(socrata_json_object):
    """
    Generate a dictionary of column names. Specific to very large datasets where field names are suppressed by socrata.

    :param socrata_json_object: response.info() json content from socrata
    :return: dictionary of hidden and visible field names in dataset
    """
    column_list = None
    field_names_list_visible = []
    field_names_list_hidden = []
    try:
        meta = socrata_json_object['meta']
        view = meta['view']
        column_list = view['columns']
    except KeyError as ke:
        print("Problem accessing json dictionary in Mega Column Dataset File. Key not found = {}".format(ke))
        exit()
    for dictionary in column_list:
        temp_field_list = dictionary.keys()
        if 'flags' in temp_field_list:
            field_names_list_hidden.append(dictionary['fieldName'])
        else:
            field_names_list_visible.append(dictionary['fieldName'])
    fields_dict = {"visible":field_names_list_visible, "hidden":field_names_list_hidden}
    return fields_dict

def handle_illegal_characters_in_string(string_with_illegals, spaces_allowed=False):
    """
    Process string, only allowing alpha and numeric. Spaces can be allowed.

    :param string_with_illegals: Input string to be processed
    :param spaces_allowed: If True, spaces will be allowed
    :return: String with illegal characters removed.
    """
    if spaces_allowed:
        re_string = "[a-zA-Z0-9 ]"
    else:
        re_string = "[a-zA-Z0-9]"
    strings_list = re.findall(re_string,string_with_illegals)
    return "".join(strings_list)

def inspect_record_for_null_values(field_null_count_dict, record_dictionary):
    """
    Inspect the socrata record for the number of null values

    :param field_null_count_dict: dictionary that counts the nulls for each field in the dataset
    :param record_dictionary: the data record to be evaluated
    :return: nothing
    """
    # In the response from a request to Socrata, only the fields with non-null/empty values appear to be included
    record_dictionary_fields = record_dictionary.keys()
    for field_name in field_null_count_dict.keys():
        # It appears Socrata does not send empty fields so absence will be presumed to indicate empty/null values
        if field_name not in record_dictionary_fields:
            field_null_count_dict[field_name] += 1
    return

def load_json(json_file_contents):
    """
    Load .json file contents

    :param json_file_contents: contents of a json file
    :return: the json file contents as a python dictionary
    """
    return json.loads(json_file_contents)

def read_json_file(file_path):
    """
    Read a .json file and grab all contents.

    :param file_path: Path to the .json file
    :return: the contents of the .json file
    """
    with open(file_path, 'r') as file_handler:
        filecontents = file_handler.read()
    return filecontents

def write_dataset_results_to_csv(dataset_name, root_file_destination_location, filename, dataset_inspection_results, total_records, processing_time):
    """
    Write a csv file containing the analysis results specific to a single dataset

    :param dataset_name: Name of the dataset of interest
    :param root_file_destination_location: Path to the location where the file directory where the file will be created
    :param filename: Name of the file, specific to each dataset
    :param dataset_inspection_results: Results of the data set inspection for null values
    :param total_records: Total number of records in the dataset
    :param processing_time: Time it took to process the dataset
    :return: None
    """
    file_path = os.path.join(root_file_destination_location, filename)
    try:
        with open(file_path, 'w') as file_handler:
            file_handler.write("{}\n".format(dataset_name))
            file_handler.write("RECORD COUNT TOTAL,{}\n".format(total_records))
            file_handler.write("PROCESSING TIME,{}\n".format(processing_time))
            file_handler.write("FIELD NAME,NULL COUNT,PERCENT\n")
            for key, value in dataset_inspection_results.items():
                percent = 0
                if total_records > 0:
                    percent = (value / float(total_records))*100
                file_handler.write("{},{},{:6.2f}\n".format(key, value, percent))
    except IOError as io_err:
        print(io_err)
        exit()
    return

def write_overview_stats_to_csv(root_file_destination_location, filename, dataset_name, dataset_csv_file_name, total_number_of_dataset_columns, total_number_of_dataset_records, data_provider, total_number_of_null_fields=0, percent_null=0):
    """
    Write analysis results for entire process, as an overview of all datasets, to .csv
    :param root_file_destination_location: Path to the location where the file directory where the file will be created
    :param filename: Name of the overview analysis file
    :param dataset_name: Name of the dataset of interest
    :param dataset_csv_file_name: Filename of the dataset of interest
    :param total_number_of_dataset_columns: Total number of columns in dataset of interest
    :param total_number_of_dataset_records: Total number of records in dataset of interest
    :param data_provider: Data provider for dataset of interest
    :param total_number_of_null_fields: Total number of null fields for dataset of interest
    :param percent_null: The percent null for the dataset of interest
    :return: None
    """
    file_path = os.path.join(root_file_destination_location, filename)
    try:
        if not os.path.exists(file_path):
            with open(file_path, "w") as file_handler:
                file_handler.write("DATASET NAME,FILE NAME,TOTAL COLUMN COUNT,TOTAL RECORD COUNT,TOTAL NULL VALUE COUNT,PERCENT NULL,DATA PROVIDER\n")
        if os.path.exists(file_path):
            with open(file_path, 'a') as file_handler:
                file_handler.write("{},{},{},{},{},{:6.2f},{}\n".format(dataset_name,
                                                                     dataset_csv_file_name,
                                                                     total_number_of_dataset_columns,
                                                                     total_number_of_dataset_records,
                                                                     total_number_of_null_fields,
                                                                     percent_null,
                                                                     data_provider)
                                   )
    except IOError as io_err:
        print(io_err)
        exit()
    return

def write_problematic_datasets_to_csv(root_file_destination_location, filename, dataset_name, message, resource=None):
    """
    Write to .csv any datasets that encountered problems during processing.
    :param root_file_destination_location:  Path to the location where the file directory where the file will be created
    :param filename: Name of the problem datasets file
    :param dataset_name: Name of the dataset of interest
    :param message: Message related to reason was problematic
    :param resource: The url resource that was being processed when problem occurred
    :return: None
    """
    file_path = os.path.join(root_file_destination_location, filename)
    try:
        if not os.path.exists(file_path):
            with open(file_path, "w") as file_handler:
                file_handler.write("DATASET NAME,PROBLEM MESSAGE,RESOURCE\n")
        if os.path.exists(file_path):
            with open(file_path, 'a') as file_handler:
                file_handler.write("{},{},{}\n".format(dataset_name, message, resource))
    except IOError as io_err:
        print(io_err)
        exit()
    return

def write_script_performance_summary(root_file_destination_location, filename, start_time, number_of_datasets_in_data_freshness_report, dataset_counter, valid_nulls_dataset_counter, valid_no_null_dataset_counter, problem_dataset_counter):
    """
    Write a summary file that details the performance of this script during processing

    :param root_file_destination_location: Path to the location where the file directory where the file will be created
    :param filename: Name of the performance summary file
    :param start_time: Time the process started
    :param number_of_datasets_in_data_freshness_report: Number of datasets in the freshness report
    :param dataset_counter: The count incremented during processing to count the number of datasets processed
    :param valid_nulls_dataset_counter: Number of datasets with at least on valid null
    :param valid_no_null_dataset_counter: Number of datasets with zero detected null values
    :param problem_dataset_counter: Number of datasets with problems
    :return: None
    """
    file_path = os.path.join(root_file_destination_location, filename)
    try:
        with open(file_path, 'w') as scriptperformancesummaryhandler:
            scriptperformancesummaryhandler.write("Date,{}\n".format(build_today_date_string()))
            scriptperformancesummaryhandler.write("Number of datasets in freshness report,{}\n".format(number_of_datasets_in_data_freshness_report))
            scriptperformancesummaryhandler.write("Total datasets processed,{}\n".format(dataset_counter))
            scriptperformancesummaryhandler.write("Valid datasets with nulls count (csv generated),{}\n".format(valid_nulls_dataset_counter))
            scriptperformancesummaryhandler.write("Valid datasets without nulls count (no csv),{}\n".format(valid_no_null_dataset_counter))
            scriptperformancesummaryhandler.write("Problematic datasets count,{}\n".format(problem_dataset_counter))
            time_took = time.time() - start_time
            scriptperformancesummaryhandler.write("Process time (minutes),{:6.2f}\n".format(time_took/60.0))
    except IOError as io_err:
        print(io_err)
        exit()
    return

# FUNCTIONALITY
def main():

    # Initiate csv report files
    problem_datasets_csv_filename = build_csv_file_name_with_date(
        today_date_string=build_today_date_string(),
        filename=PROBLEM_DATASETS_FILE_NAME.value)
    overview_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                          filename=OVERVIEW_STATS_FILE_NAME.value)

    # Need an inventory of all Maryland Socrata datasets; will gather from the data freshness report.
    data_freshness_url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS.value,
                                           api_id=DATA_FRESHNESS_REPORT_API_ID.value,
                                           limit_amount=LIMIT_MAX_AND_OFFSET.value,
                                           offset=0,
                                           total_count=0)
    freshness_report_json_objects = generate_freshness_report_json_objects(dataset_url=data_freshness_url)
    dict_of_socrata_dataset_IDs = build_datasets_inventory(freshness_report_json_objects=freshness_report_json_objects)
    number_of_datasets_in_data_freshness_report = len(dict_of_socrata_dataset_IDs)
    dict_of_socrata_dataset_providers = {}
    for record_obj in freshness_report_json_objects:
        data_freshness_dataset_name = (record_obj["dataset_name"]).encode("utf8")
        data_freshness_report_dataset_name_noillegal = handle_illegal_characters_in_string(string_with_illegals=data_freshness_dataset_name,
                                                           spaces_allowed=True)
        data_freshness_data_provider = (record_obj["data_provided_by"]).encode("utf8")
        provider_name_noillegal = handle_illegal_characters_in_string(string_with_illegals=data_freshness_data_provider,
                                                            spaces_allowed=True)
        dict_of_socrata_dataset_providers[data_freshness_report_dataset_name_noillegal] = os.path.basename(provider_name_noillegal)

    # Variables for next lower scope (alphabetic)
    dataset_counter = 0
    problem_dataset_counter = 0
    valid_no_null_dataset_counter = 0
    valid_nulls_dataset_counter = 0

    # Need to inventory field names of every dataset and tally null/empty values
    for dataset_name, dataset_api_id in dict_of_socrata_dataset_IDs.items():
        dataset_start_time = time.time()
        # Handle occasional error when writing unicode to string using format. sometimes "-" was problematic
        dataset_name_with_spaces_but_no_illegal = handle_illegal_characters_in_string(
            string_with_illegals=dataset_name.encode("utf8"),
            spaces_allowed=True)
        dataset_api_id = dataset_api_id.encode("utf8")
#____________________________________________________________________________________________________________
        # FOR TESTING - avoid huge datasets on test runs
        huge_datasets_api_s = (REAL_PROPERTY_HIDDEN_NAMES_API_ID.value,)
        if dataset_api_id in huge_datasets_api_s:
            print("Dataset Skipped Intentionally (TESTING): {}".format(dataset_name_with_spaces_but_no_illegal))
            continue
#____________________________________________________________________________________________________________

        dataset_counter += 1
        print("{}: {} ............. {}".format(dataset_counter, dataset_name_with_spaces_but_no_illegal.upper(), dataset_api_id))

        # Variables for next lower scope (alphabetic)
        dataset_fields_string = None
        field_headers = None
        is_problematic = False
        is_special_too_many_headers_dataset = False
        json_file_contents = None
        more_records_exist_than_response_limit_allows = True
        null_count_for_each_field_dict = {}
        number_of_columns_in_dataset = None
        problem_message = None
        problem_resource = None
        socrata_record_offset_value = 0
        socrata_response_info_key_list = None
        socrata_url_response = None
        total_record_count = 0

        # Some datasets will have more records than are returned in a single response; varies with the limit_max value
        while more_records_exist_than_response_limit_allows:

            # Maryland Statewide Vehicle Crashes are excel files, not Socrata records,
            #   but they will return empty json objects endlessly
            if dataset_name.startswith(MD_STATEWIDE_VEHICLE_CRASH_STARTSWITH.value):
                problem_message = "Intentionally skipped. Dataset was an excel file as of 20180409. Call to Socrata endlessly returns empty json objects."
                is_problematic = True
                break

            cycle_record_count = 0
            url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS.value,
                                    api_id=dataset_api_id,
                                    limit_amount=LIMIT_MAX_AND_OFFSET.value,
                                    offset=socrata_record_offset_value,
                                    total_count=total_record_count)
            print(url)

            request = urllib2.Request(url)

            try:
                socrata_url_response = urllib2.urlopen(request)
            except urllib2.URLError as e:
                problem_resource = url
                is_problematic = True
                if hasattr(e, "reason"):
                    problem_message = "Failed to reach a server. Reason: {}".format(e.reason)
                    break
                elif hasattr(e, "code"):
                    problem_message = "The server couldn't fulfill the request. Error Code: {}".format(e.code)
                    break

            # For datasets with a lot of fields it looks like Socrata doesn't return the
            #   field headers in the response.info() so the X-SODA2-Fields key DNE.
            # Only need to get the list of socrata response keys the first time through
            if socrata_response_info_key_list == None:
                socrata_response_info_key_list = []
                for key in socrata_url_response.info().keys():
                    socrata_response_info_key_list.append(key.lower())
            else:
                pass

            # Only need to get the field headers the first time through
            if dataset_fields_string == None and "x-soda2-fields" in socrata_response_info_key_list:
                dataset_fields_string = socrata_url_response.info()["X-SODA2-Fields"]
            elif dataset_fields_string == None and "x-soda2-fields" not in socrata_response_info_key_list:
                is_special_too_many_headers_dataset = True
            else:
                pass

            # If Socrata didn't send the headers see if the dataset is one of the two known to be too big
            if field_headers == None and is_special_too_many_headers_dataset and dataset_api_id == REAL_PROPERTY_HIDDEN_NAMES_API_ID.value:
                json_file_contents = read_json_file(file_path=REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE.value)
            elif field_headers == None and is_special_too_many_headers_dataset and dataset_api_id == CORRECTIONAL_ENTERPRISES_EMPLOYEES_API_ID.value:
                json_file_contents = read_json_file(file_path=CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE.value)
            elif field_headers == None and is_special_too_many_headers_dataset:
                # In case a new previously unknown dataset comes along with too many fields for transfer
                problem_message = "Too many fields. Socrata suppressed X-SODA2-FIELDS value in response."
                problem_resource = url
                is_problematic = True
                break
            elif field_headers == None:
                field_headers = re.findall("[a-zA-Z0-9_]+", dataset_fields_string)
            else:
                pass

            # If special, first time through load the field names from their pre-made json files.
            if json_file_contents != None:
                json_loaded = load_json(json_file_contents=json_file_contents)
                field_names_dictionary = grab_field_names_for_mega_columned_datasets(socrata_json_object=json_loaded)
                field_headers = field_names_dictionary["visible"]
            else:
                pass

            # Need a dictionary of headers to store null count
            for header in field_headers:
                null_count_for_each_field_dict[header] = 0

            if number_of_columns_in_dataset == None:
                number_of_columns_in_dataset = len(field_headers)

            response_string = socrata_url_response.read()
            json_objects_pythondict = json.loads(response_string)

            # Some datasets are html or other type but socrata returns an empty object rather than a json object with
            #   reason or code. These datasets are then not recognized as problematic and throw off the tracking counts.
            if len(json_objects_pythondict) == 0:
                problem_message = "Response json object was empty"
                problem_resource = url
                is_problematic = True
                break

            partial_function_for_multithreading = partial(inspect_record_for_null_values,
                                                          null_count_for_each_field_dict)
            # pool = Pool()
            pool = ThreadPool(THREAD_COUNT.value)
            pool.map(partial_function_for_multithreading, json_objects_pythondict)
            pool.close()
            pool.join()
            record_count_increase = len(json_objects_pythondict)
            cycle_record_count += record_count_increase
            total_record_count += record_count_increase

            # Any cycle_record_count that equals the max limit indicates another request is needed
            if cycle_record_count == LIMIT_MAX_AND_OFFSET.value:
                # Give Socrata servers small interval before requesting more
                time.sleep(0.2)
                socrata_record_offset_value = cycle_record_count + socrata_record_offset_value
            else:
                more_records_exist_than_response_limit_allows = False

        # Output the results, to a stand alone csv for each dataset containing null values,
        #   to a csv of problematic datasets, and to the overview for all datasets.
        total_number_of_null_values = calculate_total_number_of_empty_values_per_dataset(
            null_counts_list=null_count_for_each_field_dict.values())
        percent_of_dataset_are_null_values = calculate_percent_null_for_dataset(
            null_count_total=total_number_of_null_values,
            total_records_processed=total_record_count,
            number_of_fields_in_dataset=number_of_columns_in_dataset)

        if is_problematic:
            problem_dataset_counter += 1
            write_problematic_datasets_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT.value,
                                              filename=problem_datasets_csv_filename,
                                              dataset_name=dataset_name_with_spaces_but_no_illegal,
                                              message=problem_message,
                                              resource=problem_resource)
        elif total_number_of_null_values > 0:
            valid_nulls_dataset_counter += 1

            # Write each datasets stats to its own csv
            dataset_name_no_spaces_no_illegal = handle_illegal_characters_in_string(string_with_illegals=dataset_name)
            dataset_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                 filename=dataset_name_no_spaces_no_illegal)
            # dataset_csv_file_path = os.path.join(ROOT_PATH_FOR_CSV_OUTPUT.value, dataset_csv_filename)
            write_dataset_results_to_csv(dataset_name=dataset_name_with_spaces_but_no_illegal,
                                         root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT.value,
                                         filename=dataset_csv_filename,
                                         dataset_inspection_results=null_count_for_each_field_dict,
                                         total_records=total_record_count,
                                         processing_time=calculate_time_taken(dataset_start_time))

            # Append the overview stats for each dataset to the overview stats csv
            write_overview_stats_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT.value,
                                        filename=overview_csv_filename,
                                        dataset_name=dataset_name_with_spaces_but_no_illegal,
                                        dataset_csv_file_name=dataset_csv_filename,
                                        total_number_of_dataset_columns=number_of_columns_in_dataset,
                                        total_number_of_dataset_records=total_record_count,
                                        data_provider=dict_of_socrata_dataset_providers[dataset_name_with_spaces_but_no_illegal],
                                        total_number_of_null_fields=total_number_of_null_values,
                                        percent_null=percent_of_dataset_are_null_values)
        else:
            valid_no_null_dataset_counter += 1

            # Append the overview stats for each dataset to the overview stats csv
            write_overview_stats_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT.value,
                                        filename=overview_csv_filename,
                                        dataset_name=dataset_name_with_spaces_but_no_illegal,
                                        dataset_csv_file_name=None,
                                        total_number_of_dataset_columns=number_of_columns_in_dataset,
                                        total_number_of_dataset_records=total_record_count,
                                        data_provider=dict_of_socrata_dataset_providers[dataset_name_with_spaces_but_no_illegal],
                                        total_number_of_null_fields=total_number_of_null_values,
                                        percent_null=percent_of_dataset_are_null_values
                                        )

    performance_summary_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                 filename=PERFORMANCE_SUMMARY_FILE_NAME.value)
    write_script_performance_summary(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT.value,
                                     filename=performance_summary_filename,
                                     start_time=process_start_time,
                                     number_of_datasets_in_data_freshness_report=number_of_datasets_in_data_freshness_report,
                                     dataset_counter=dataset_counter,
                                     valid_nulls_dataset_counter=valid_nulls_dataset_counter,
                                     valid_no_null_dataset_counter=valid_no_null_dataset_counter,
                                     problem_dataset_counter=problem_dataset_counter)

    print("Process time (minutes) = {:4.2f}\n".format((time.time()-process_start_time)/60.0))

if __name__ == "__main__":
    main()
