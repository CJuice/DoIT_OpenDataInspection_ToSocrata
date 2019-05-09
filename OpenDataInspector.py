"""
Inspect all datasets in MD DoIT Open Data Portal on Socrata, inventory null data, and output statistics.

Use the Data Freshness Report dataset to identify all datasets to be inspected by this process.
For each dataset, request records from Socrata, inventory nulls in records, and repeat until all records have been
 processed.
Identify datasets without nulls, with nulls, and problem datasets that couldn't be processed.
Upsert output statistics to Socrata dataset providing an overview at the dataset level, a Socrata dataset
 providing information at the field level, a csv file capturing all problematic datasets, and a csv file
 reporting on the performance of the script. Optionally, also write dataset level and field level statistics to csv files.
Author: CJuice
Date: 20180601
Revisions: 20190311, CJuice, Revised root url for data.maryland.gov to opendata.maryland.gov due to domain change
"""

# TODO: requests module has built in json decoder. if r is the response then r.json() is the call. Evaluate use.
# TODO: evaluate use of requests params keyword and pass limit and offset in a dictionary
# TODO: add logging

# FIXME: DO NOT COMMIT CHANGES TO GITHUB. NEED TO ADDRESS THE CHANGES I WIPED BUT WANT TO IMPLEMENT.


def main():
    # IMPORTS
    from collections import namedtuple
    from datetime import date
    import json
    import os
    import re
    import time
    import requests
    # from functools import partial
    # Note: below import is same as 'from multiprocessing.dummy import Pool as ThreadPool'
    # from multiprocessing.pool import ThreadPool
    from sodapy import Socrata

    process_start_time = time.time()

    CONSTANT = namedtuple("CONSTANT", ["value"])

    # VARIABLES (alphabetic)
    _ROOT_URL_FOR_PROJECT = os.path.dirname(__file__)
    CORRECTIONAL_ENTERPRISES_EMPLOYEES_API_ID = "mux9-y6mb"
    CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE = os.path.join(_ROOT_URL_FOR_PROJECT, r"EssentialExtraFilesForOpenDataInspectorSuccess\MarylandCorrectionalEnterprises_JSON.json")
    DATA_FRESHNESS_REPORT_API_ID = "t8k3-edvn"
    FIELD_LEVEL_STATS_FILE_NAME = "_FIELD_LEVEL_STATS"
    FIELD_LEVEL_STATS_SOCRATA_HEADERS = ['DATASET NAME', 'FIELD NAME', 'TOTAL NULL VALUE COUNT', 'TOTAL RECORD COUNT', 'PERCENT NULL', 'HYPERLINK', 'DATASET ID', 'FIELD ID', 'DATE', 'ROW ID']
    LIMIT_MAX_AND_OFFSET = 10000
    MD_STATEWIDE_VEHICLE_CRASH_STARTSWITH = "Maryland Statewide Vehicle Crashes"
    OVERVIEW_LEVEL_STATS_FILE_NAME = "_OVERVIEW_STATS"
    OVERVIEW_LEVEL_STATS_SOCRATA_HEADERS = ['DATASET NAME', 'HYPERLINK', 'TOTAL COLUMN COUNT', 'TOTAL RECORD COUNT', 'TOTAL VALUE COUNT', 'TOTAL NULL VALUE COUNT', 'PERCENT NULL', 'DATASET ID', 'DATA PROVIDER', 'DATE', 'ROW ID']
    PERFORMANCE_SUMMARY_FILE_NAME = "__script_performance_summary"
    PROBLEM_DATASETS_FILE_NAME = "_PROBLEM_DATASETS"
    REAL_PROPERTY_HIDDEN_NAMES_API_ID = "ed4q-f8tm"
    REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE = os.path.join(_ROOT_URL_FOR_PROJECT, r"EssentialExtraFilesForOpenDataInspectorSuccess\RealPropertyHiddenOwner_JSON.json")
    ROOT_PATH_FOR_CSV_OUTPUT = os.path.join(_ROOT_URL_FOR_PROJECT, "OUTPUT_CSVs")
    ROOT_URL_FOR_DATASET_ACCESS = r"https://opendata.maryland.gov/resource/"
    SOCRATA_CREDENTIALS_JSON_FILE = os.path.join(_ROOT_URL_FOR_PROJECT.value, r"EssentialExtraFilesForOpenDataInspectorSuccess\Credentials_OpenDataInspector_ToSocrata_TESTING.json")   # TESTING
    # SOCRATA_CREDENTIALS_JSON_FILE = os.path.join(_ROOT_URL_FOR_PROJECT.value, r"EssentialExtraFilesForOpenDataInspectorSuccess\Credentials_OpenDataInspector_ToSocrata.json") # PRODUCTION
    TURN_ON_WRITE_OUTPUT_TO_CSV = False         # OPTION
    TURN_ON_UPSERT_OUTPUT_TO_SOCRATA = True    # OPTION

    assert os.path.exists(CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE)
    assert os.path.exists(REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE)
    assert os.path.exists(ROOT_PATH_FOR_CSV_OUTPUT)
    assert os.path.exists(SOCRATA_CREDENTIALS_JSON_FILE)

    # FUNCTIONS (alphabetic)
    def build_csv_file_name_with_date(today_date_string, filename):
        """
        Build a string, ending in .csv, that contains todays date and the provided file name

        :param today_date_string: Intended to be the date the file will be created
        :param filename: Name of the file
        :return: string that is 'date_filename.csv'
        """
        return "{}_{}.csv".format(today_date_string, filename)

    def build_dataset_url(url_root, api_id, limit_amount=0, offset=0, total_count=None):
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
        if total_count == None and limit_amount == 0 and offset == 0:
            return "{}{}".format(url_root, api_id)
        elif total_count >= LIMIT_MAX_AND_OFFSET.value:
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
            api_id = os.path.basename(record_obj["link"]["url"])  # 20190502 revised by CJuice. Added ["url"].
            datasets_dictionary[dataset_name] = api_id
        return datasets_dictionary

    def build_today_date_string():
        """
        Build a string representing todays date.

        :return: string representing date formatted as Year Month Day. Formatted to meet Socrata accepted style
        """
        return "{:%Y-%m-%d}".format(date.today())

    def calculate_percent_null(null_count_total, total_data_values):
        """
        Calculate the percent of all possible data values, not rows or columns, that are null

        :param null_count_total: Total number of null values
        :param total_data_values: Denominator in division to calculate percent. Total records, total data values, etc.
        :return: Percent value as a float, rounded to two decimal places
        """
        if total_data_values == 0:
            return 0.0
        else:
            percent_full_float = float(null_count_total / total_data_values) * 100.0
            return round(percent_full_float, 2)

    def calculate_time_taken(start_time):
        """
        Calculate the time difference between now and the value passed as the start time

        :param start_time: Time value representing start of processing
        :return: Difference value between start time and current time
        """
        return (time.time() - start_time)

    def calculate_total_number_of_null_values_per_dataset(null_counts_list):
        """
        Calculate the total number of null/empty values in a dataset

        :param null_counts_list: List of numeric values representing null counts per column in dataset
        :return: Integer value representing total
        """
        return sum(null_counts_list)

    def calculate_total_number_of_values_in_dataset(total_records_processed, number_of_fields_in_dataset):
        """
        Calculate the total number of values in a dataset from the number of records and columns/fields.

        :param total_records_processed: Total number or records processed
        :param number_of_fields_in_dataset: Total number of columns in the dataset
        :return:
        """
        if number_of_fields_in_dataset is None:
            return 0
        else:
            return float(total_records_processed * number_of_fields_in_dataset)

    def create_socrata_client(credentials_json, dataset_key):
        """
        Create and return a Socrata client for use.

        :param credentials_json: the json code from the credentials file
        :param dataset_key: the dictionary key of interest
        :return: Socrata connection client
        """
        dataset_credentials = credentials_json[dataset_key]
        access_credentials = credentials_json["access_credentials"]
        for key, value in dataset_credentials.items():  # Value of None in json is seen as string, need to convert or fails
            if value == 'None':
                dataset_credentials[key] = None
        maryland_domain = dataset_credentials["maryland_domain"]
        maryland_app_token = dataset_credentials["app_token"]
        username = access_credentials["username"]
        password = access_credentials["password"]
        return Socrata(domain=maryland_domain, app_token=maryland_app_token, username=username, password=password)

    def generate_freshness_report_json_objects(dataset_url):
        """
        Makes request to socrata url for dataset and processes response into json objects

        :param dataset_url: url to which the request is made
        :return: json objects in dictionary form
        """
        json_objects = None
        url = dataset_url
        try:
            response = requests.get(url)
        except Exception as e:
            if hasattr(e, "reason"):
                print("build_datasets_inventory(): Failed to reach a server. Reason: {}".format(e.reason))
            elif hasattr(e, "code"):
                print("build_datasets_inventory(): The server couldn't fulfill the request. Error Code: {}".format(e.code))
            exit()
        else:
            json_objects = response.json()
        return json_objects

    def generate_id_from_args(*args, separator="."):
        """
        Create a string from args, separated by separator value.

        :param args: Any number of arguements to be used
        :param separator: Character to separate the args
        :return: String value of args separated by separator
        """
        sep = str(separator)
        arg_stringified_list = [str(arg) for arg in args]
        return sep.join(arg_stringified_list)

    def get_dataset_identifier(credentials_json, dataset_key):
        """
        Get the unique Socrata dataset identifier from the credentials json file

        :param credentials_json: the json code from the credentials file
        :param dataset_key: the dictionary key of interest
        :return: string, unique Socrata dataset identifier
        """
        return credentials_json[dataset_key]["app_id"]

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

    def make_zipper(dataset_headers_list, record_list):
        """
        Zip headers and data values and return a dictionary

        :param dataset_headers_list: List of headers for dataset
        :param record_list: List of values in the record
        :return: dictionary of zip results
        """
        return dict(zip(dataset_headers_list, record_list))

    def read_json_file(file_path):
        """
        Read a .json file and grab all contents.

        :param file_path: Path to the .json file
        :return: the contents of the .json file
        """
        with open(file_path, 'r') as file_handler:
            filecontents = file_handler.read()
        return filecontents

    def upsert_to_socrata(client, dataset_identifier, zipper):
        """
        Upsert data to Socrata dataset.

        :param client: Socrata connection client
        :param dataset_identifier: Unique Socrata dataset identifier. Not the data page identifier but the primary page id.
        :param zipper: dictionary of zipped results (headers and data values)
        :return: None
        """
        try:
            client.upsert(dataset_identifier=dataset_identifier, payload=zipper, content_type='json')
        except Exception as e:
            print("Error upserting to Socrata: {}. {}".format(dataset_identifier, e))
        return

    def write_dataset_results_to_csv(root_file_destination_location, filename, header_list=None, records_list_list=None):
        """
        Write a csv file containing the analysis results specific to a single dataset

        :param root_file_destination_location: Path to the location of the file directory where the file will be created
        :param filename: Name of the file for field level results of datasets
        :param header_list: List of headers for the data
        :param records_list_list: List of lists of values to be written to csv as records
        :return: None
        """
        file_path = os.path.join(root_file_destination_location, filename)
        try:
            if os.path.exists(file_path) and records_list_list is not None:
                with open(file_path, 'a') as file_handler:
                    for record_list in records_list_list:
                        dataset_name,field_name_key,null_count_value,total_number_of_dataset_records,percent,hyperlink,api_id,unique_field_id,date_analyzed,row_id = record_list
                        file_handler.write("{},{},{},{},{:6.2f},{},{},{},{},{}\n".format(dataset_name,
                                                                                      field_name_key,
                                                                                      null_count_value,
                                                                                      total_number_of_dataset_records,
                                                                                      percent,
                                                                                      hyperlink,
                                                                                      api_id,
                                                                                      unique_field_id,
                                                                                      date_analyzed,
                                                                                      row_id))
            else:
                with open(file_path, 'w') as file_handler:
                    file_handler.write("{}\n".format(",".join(header_list)))
        except IOError as io_err:
            print(io_err)
            exit()
        return

    def write_overview_stats_to_csv(root_file_destination_location, filename, header_list=None, record_list=None):
        """
        Write analysis results for entire process, as an overview of all datasets, to .csv

        :param root_file_destination_location: Path to the location where the file directory where the file will be created
        :param filename: Name of the overview analysis file
        :param header_list: List of headers for the data
        :param record_list: List of values to be written to csv as a record
        :return: None
        """

        file_path = os.path.join(root_file_destination_location, filename)
        try:
            if os.path.exists(file_path) and record_list is not None:
                dataset_name, hyperlink, total_number_of_dataset_columns, total_number_of_dataset_records, total_number_of_values, total_number_of_null_fields, percent_null, api_id, data_provider, date_analyzed, row_id = record_list
                with open(file_path, 'a') as file_handler:
                    file_handler.write("{},{},{},{},{},{},{:6.2f},{},{},{},{}\n".format(dataset_name,
                                                                                     hyperlink,
                                                                                     total_number_of_dataset_columns,
                                                                                     total_number_of_dataset_records,
                                                                                     total_number_of_values,
                                                                                     total_number_of_null_fields,
                                                                                     percent_null,
                                                                                     api_id,
                                                                                     data_provider,
                                                                                     date_analyzed,
                                                                                     row_id)
                                       )
            else:
                with open(file_path, "w") as file_handler:
                    file_handler.write("{}\n".format(",".join(header_list)))
        except IOError as io_err:
            print(io_err)
            exit()
        return

    def write_problematic_datasets_to_csv(root_file_destination_location, filename, dataset_name=None, message=None, resource=None):
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
            if os.path.exists(file_path):
                with open(file_path, 'a') as file_handler:
                    file_handler.write("{},{},{}\n".format(dataset_name, message, resource))
            else:
                with open(file_path, "w") as file_handler:
                    file_handler.write("DATASET NAME,PROBLEM MESSAGE,RESOURCE\n")
        except IOError as io_err:
            print(io_err)
            exit()
        return

    def write_script_performance_summary(root_file_destination_location, filename, start_time,
                                         number_of_datasets_in_data_freshness_report, dataset_counter,
                                         valid_nulls_dataset_counter, valid_no_null_dataset_counter,
                                         problem_dataset_counter):
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
                scriptperformancesummaryhandler.write("Process time (minutes),{:6.2f}\n".format(calculate_time_taken(start_time=start_time)/60.0))
        except IOError as io_err:
            print(io_err)
            exit()
        return

    # FUNCTIONALITY
    if TURN_ON_WRITE_OUTPUT_TO_CSV:
        print("Writing to csv (TURN_ON_WRITE_OUTPUT_TO_CSV.value = True)")
    if TURN_ON_UPSERT_OUTPUT_TO_SOCRATA:
        print("Upserting to Socrata (TURN_ON_UPSERT_OUTPUT_TO_SOCRATA.value = True)")

    # Initiate csv report files
    problem_datasets_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                  filename=PROBLEM_DATASETS_FILE_NAME)
    write_problematic_datasets_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                      filename=problem_datasets_csv_filename)

    if TURN_ON_WRITE_OUTPUT_TO_CSV:
        # Optional output to CSV's, per original functionality. Initiate files here.
        field_level_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                 filename=FIELD_LEVEL_STATS_FILE_NAME)
        write_dataset_results_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                     filename=field_level_csv_filename,
                                     header_list=FIELD_LEVEL_STATS_SOCRATA_HEADERS)
        overview_csv_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                              filename=OVERVIEW_LEVEL_STATS_FILE_NAME)
        write_overview_stats_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                    filename=overview_csv_filename,
                                    header_list=OVERVIEW_LEVEL_STATS_SOCRATA_HEADERS)

    # Need an inventory of all Maryland Socrata datasets; will gather from the data freshness report.
    data_freshness_url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS,
                                           api_id=DATA_FRESHNESS_REPORT_API_ID,
                                           limit_amount=LIMIT_MAX_AND_OFFSET,
                                           offset=0,
                                           total_count=0)
    freshness_report_json_objects = generate_freshness_report_json_objects(dataset_url=data_freshness_url)
    dict_of_socrata_dataset_IDs = build_datasets_inventory(freshness_report_json_objects=freshness_report_json_objects)
    number_of_datasets_in_data_freshness_report = len(dict_of_socrata_dataset_IDs)
    dict_of_socrata_dataset_providers = {}
    for record_obj in freshness_report_json_objects:
        data_freshness_dataset_name = (record_obj["dataset_name"])
        data_freshness_report_dataset_name_noillegal = handle_illegal_characters_in_string(
            string_with_illegals=data_freshness_dataset_name,
            spaces_allowed=True)
        data_freshness_data_provider = (record_obj["data_provided_by"])
        provider_name_noillegal = handle_illegal_characters_in_string(
            string_with_illegals=data_freshness_data_provider,
            spaces_allowed=True)
        dict_of_socrata_dataset_providers[data_freshness_report_dataset_name_noillegal] = os.path.basename(
            provider_name_noillegal)

    # Socrata related variables, derived
    credentials_json_file_contents = read_json_file(SOCRATA_CREDENTIALS_JSON_FILE)
    credentials_json = load_json(json_file_contents=credentials_json_file_contents)
    socrata_client_field_level = create_socrata_client(credentials_json=credentials_json,
                                                       dataset_key="field_level_dataset")
    socrata_client_overview_level = create_socrata_client(credentials_json=credentials_json,
                                                          dataset_key="overview_level_dataset")
    socrata_field_level_dataset_app_id = get_dataset_identifier(credentials_json=credentials_json,
                                                                dataset_key="field_level_dataset")
    socrata_overview_level_dataset_app_id = get_dataset_identifier(credentials_json=credentials_json,
                                                                   dataset_key="overview_level_dataset")

    # Variables for next lower scope (alphabetic)
    dataset_counter = 0
    problem_dataset_counter = 0
    valid_no_null_dataset_counter = 0
    valid_nulls_dataset_counter = 0

    # Need to inventory field names of every dataset and tally null/empty values
    for dataset_name, dataset_api_id in dict_of_socrata_dataset_IDs.items():
        dataset_name_with_spaces_but_no_illegal = handle_illegal_characters_in_string(string_with_illegals=dataset_name,
                                                                                      spaces_allowed=True)
        url_socrata_data_page = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS,
                                                  api_id=dataset_api_id)
#_______________________________________________________________________________________________________________________
        # FOR TESTING - avoid huge datasets on test runs
        # huge_datasets_api_s = (REAL_PROPERTY_HIDDEN_NAMES_API_ID.value,)
        # if dataset_api_id not in huge_datasets_api_s:
        #     print("Dataset Skipped Intentionally (TESTING): {}".format(dataset_name_with_spaces_but_no_illegal))
        #     continue
#_______________________________________________________________________________________________________________________

        dataset_counter += 1
        print("{}: {} ............. {}".format(dataset_counter, dataset_name_with_spaces_but_no_illegal.upper(),
                                               dataset_api_id))

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
            if dataset_name.startswith(MD_STATEWIDE_VEHICLE_CRASH_STARTSWITH):
                problem_message = "Intentionally skipped. Dataset was an excel file as of 20180409. Call to Socrata endlessly returns empty json objects."
                is_problematic = True
                break
            cycle_record_count = 0
            url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS,
                                    api_id=dataset_api_id,
                                    limit_amount=LIMIT_MAX_AND_OFFSET,
                                    offset=socrata_record_offset_value,
                                    total_count=total_record_count)
            print(url)

            try:
                socrata_url_response = requests.get(url)
            except Exception as e:
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
                for key in socrata_url_response.headers.keys():
                    socrata_response_info_key_list.append(key.lower())
            else:
                pass

            # Only need to get the field headers the first time through
            if dataset_fields_string == None and "x-soda2-fields" in socrata_response_info_key_list:
                dataset_fields_string = socrata_url_response.headers["X-SODA2-Fields"]
            elif dataset_fields_string == None and "x-soda2-fields" not in socrata_response_info_key_list:
                is_special_too_many_headers_dataset = True
            else:
                pass

            # If Socrata didn't send the headers see if the dataset is one of the two known to be too big
            if field_headers == None and is_special_too_many_headers_dataset and dataset_api_id == REAL_PROPERTY_HIDDEN_NAMES_API_ID:
                json_file_contents = read_json_file(file_path=REAL_PROPERTY_HIDDEN_NAMES_JSON_FILE)
            elif field_headers == None and is_special_too_many_headers_dataset and dataset_api_id == CORRECTIONAL_ENTERPRISES_EMPLOYEES_API_ID:
                json_file_contents = read_json_file(file_path=CORRECTIONAL_ENTERPRISES_EMPLOYEES_JSON_FILE)
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

            # Need a dictionary of headers to store null count, but only on first time through.
            if len(null_count_for_each_field_dict) == 0:
                for header in field_headers:
                    null_count_for_each_field_dict[header] = 0

            if number_of_columns_in_dataset == None:
                number_of_columns_in_dataset = len(field_headers)

            response_list_of_dicts = socrata_url_response.json()

            # Some datasets are html or other type but socrata returns an empty object rather than a json object with
            #   reason or code. These datasets are then not recognized as problematic and throw off the tracking counts.
            if len(response_list_of_dicts) == 0:
                problem_message = "Response object was empty"
                problem_resource = url
                is_problematic = True
                break

            for record in response_list_of_dicts:
                inspect_record_for_null_values(field_null_count_dict=null_count_for_each_field_dict, record_dictionary=record)

            record_count_increase = len(response_list_of_dicts)
            cycle_record_count += record_count_increase
            total_record_count += record_count_increase

            # Any cycle_record_count that equals the max limit indicates another request is needed
            if cycle_record_count == LIMIT_MAX_AND_OFFSET:

                # Give Socrata servers small interval before requesting more
                time.sleep(0.2)
                socrata_record_offset_value = cycle_record_count + socrata_record_offset_value
            else:
                more_records_exist_than_response_limit_allows = False

        # Calculate statistics for outputs
        total_number_of_null_values = calculate_total_number_of_null_values_per_dataset(
            null_counts_list=null_count_for_each_field_dict.values())
        total_number_of_values_in_dataset = calculate_total_number_of_values_in_dataset(
            total_records_processed=total_record_count,
            number_of_fields_in_dataset=number_of_columns_in_dataset)
        percent_of_dataset_are_null_values = calculate_percent_null(null_count_total=total_number_of_null_values,
                                                                    total_data_values=total_number_of_values_in_dataset)

        if is_problematic:
            problem_dataset_counter += 1
            write_problematic_datasets_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                              filename=problem_datasets_csv_filename,
                                              dataset_name=dataset_name_with_spaces_but_no_illegal,
                                              message=problem_message,
                                              resource=problem_resource
                                              )
        else:
            if total_number_of_null_values > 0:
                valid_nulls_dataset_counter += 1
            else:
                valid_no_null_dataset_counter += 1

            # Field Level
            field_records_list_list = []
            for field_name_key, null_count_value in null_count_for_each_field_dict.items():
                unique_field_id = generate_id_from_args(dataset_api_id, field_name_key)
                unique_row_id_field_level = generate_id_from_args(unique_field_id, build_today_date_string())
                percent_nulls_in_field = calculate_percent_null(null_count_total=null_count_value,
                                                                total_data_values=total_record_count)
                field_level_record_list = [dataset_name_with_spaces_but_no_illegal, field_name_key, null_count_value,
                                           total_record_count, percent_nulls_in_field, url_socrata_data_page, dataset_api_id,
                                           unique_field_id, build_today_date_string(), unique_row_id_field_level]
                field_records_list_list.append(field_level_record_list)
                zipper_field_level = make_zipper(dataset_headers_list=FIELD_LEVEL_STATS_SOCRATA_HEADERS,
                                                 record_list=field_level_record_list)
                if TURN_ON_UPSERT_OUTPUT_TO_SOCRATA:
                    upsert_to_socrata(client=socrata_client_field_level,
                                      dataset_identifier=socrata_field_level_dataset_app_id, zipper=zipper_field_level)

            # Overview Level
            unique_row_id_overview_level = generate_id_from_args(dataset_api_id, build_today_date_string())
            overview_level_record_list = [dataset_name_with_spaces_but_no_illegal, url_socrata_data_page,
                                    number_of_columns_in_dataset, total_record_count, total_number_of_values_in_dataset,
                                    total_number_of_null_values, percent_of_dataset_are_null_values, dataset_api_id,
                                    dict_of_socrata_dataset_providers[dataset_name_with_spaces_but_no_illegal],
                                    build_today_date_string(), unique_row_id_overview_level
                                    ]
            zipper_overview_level = make_zipper(dataset_headers_list=OVERVIEW_LEVEL_STATS_SOCRATA_HEADERS,
                                                record_list=overview_level_record_list)
            if TURN_ON_UPSERT_OUTPUT_TO_SOCRATA:
                upsert_to_socrata(client=socrata_client_overview_level,
                                  dataset_identifier=socrata_overview_level_dataset_app_id, zipper=zipper_overview_level)
                print("\tUPSERTED: {}".format(dataset_name))

            if TURN_ON_WRITE_OUTPUT_TO_CSV:
                # Optional output to CSV's, per original functionality. Write output here.
                # Append dataset results to the field level stats file

                write_dataset_results_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                             filename=field_level_csv_filename,
                                             header_list=None,
                                             records_list_list=field_records_list_list)

                # Append the overview stats for each dataset to the overview stats csv
                write_overview_stats_to_csv(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                            filename=overview_csv_filename,
                                            header_list=None,
                                            record_list=overview_level_record_list)
                print("\tWRITTEN TO CSV: {}".format(dataset_name))

    socrata_client_overview_level.close()
    socrata_client_field_level.close()

    performance_summary_filename = build_csv_file_name_with_date(today_date_string=build_today_date_string(),
                                                                 filename=PERFORMANCE_SUMMARY_FILE_NAME)
    write_script_performance_summary(root_file_destination_location=ROOT_PATH_FOR_CSV_OUTPUT,
                                     filename=performance_summary_filename,
                                     start_time=process_start_time,
                                     number_of_datasets_in_data_freshness_report=number_of_datasets_in_data_freshness_report,
                                     dataset_counter=dataset_counter,
                                     valid_nulls_dataset_counter=valid_nulls_dataset_counter,
                                     valid_no_null_dataset_counter=valid_no_null_dataset_counter,
                                     problem_dataset_counter=problem_dataset_counter
                                     )

    print("Process time (minutes) = {:4.2f}\n".format(calculate_time_taken(process_start_time)/60.0))
    return


if __name__ == "__main__":
    main()
