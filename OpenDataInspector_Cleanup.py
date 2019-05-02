"""

"""


def main():
    # IMPORTS
    from sodapy import Socrata
    import dateutil.parser as parser
    from datetime import datetime
    import json
    import os
    import requests
    import time
    import pandas as pd

    # VARIABLES
    _ROOT_URL_FOR_PROJECT = os.path.dirname(__file__)
    baseline_date = datetime(2018, 8, 3)
    LIMIT_MAX_AND_OFFSET = 10000
    ROOT_MD_OPENDATA_DOMAIN = r"https://opendata.maryland.gov"
    ROOT_URL_FOR_DATASET_ACCESS = r"{root}/resource/".format(root=ROOT_MD_OPENDATA_DOMAIN)
    SOCRATA_CREDENTIALS_JSON_FILE = os.path.join(_ROOT_URL_FOR_PROJECT,
                                                 r"EssentialExtraFilesForOpenDataInspectorSuccess\Credentials_OpenDataInspector_ToSocrata.json")
    overview_outdated_row_ids_list = []
    field_outdated_row_ids_list = []

    # ASSERTS
    # CLASSES

    # FUNCTIONS
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
        if total_count is None and limit_amount == 0 and offset == 0:
            return "{}{}".format(url_root, api_id)
        elif total_count >= LIMIT_MAX_AND_OFFSET:
            return "{}{}.json?$limit={}&$offset={}".format(url_root, api_id, limit_amount, offset)
        else:
            return "{}{}.json?$limit={}".format(url_root, api_id, limit_amount)

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
        maryland_app_token = dataset_credentials["app_token"]
        username = access_credentials["username"]
        password = access_credentials["password"]
        return Socrata(domain=ROOT_MD_OPENDATA_DOMAIN, app_token=maryland_app_token, username=username,
                       password=password)

    def get_dataset_identifier(credentials_json, dataset_key):
        """
        Get the unique Socrata dataset identifier from the credentials json file

        :param credentials_json: the json code from the credentials file
        :param dataset_key: the dictionary key of interest
        :return: string, unique Socrata dataset identifier
        """
        return credentials_json[dataset_key]["app_id"]

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

    # FUNCTIONALITY

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



    # Overview level operations
    more_overview_records_exist_than_response_limit_allows = True
    overview_cycle_record_count = 0
    overview_total_record_count = 0
    overview_record_offset_value = 0
    while more_overview_records_exist_than_response_limit_allows:
        overview_dataset_url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS,
                                                 api_id=socrata_overview_level_dataset_app_id,
                                                 limit_amount=LIMIT_MAX_AND_OFFSET,
                                                 offset=overview_record_offset_value,
                                                 total_count=overview_total_record_count)
        print(overview_dataset_url)
        overview_response = requests.get(url=overview_dataset_url)
        overview_json = overview_response.json()

        for obj in overview_json:
            overview_date = obj.get("date", None)
            overview_date_obj = parser.parse(overview_date)
            overview_row_id = obj.get("row_id", None)
            if overview_date_obj < baseline_date:
                overview_outdated_row_ids_list.append({":row_id": overview_row_id, ":deleted": True})
        print(overview_outdated_row_ids_list)

        number_of_overview_records_returned = len(overview_json)
        print(number_of_overview_records_returned)
        overview_cycle_record_count += number_of_overview_records_returned
        overview_total_record_count += number_of_overview_records_returned

        # Any cycle_record_count that equals the max limit indicates another request is needed
        print(overview_cycle_record_count)
        if overview_cycle_record_count == LIMIT_MAX_AND_OFFSET:
            # Give Socrata servers small interval before requesting more
            time.sleep(0.2)
            overview_record_offset_value = overview_cycle_record_count + overview_record_offset_value
        else:
            more_overview_records_exist_than_response_limit_allows = False

    # socrata_client_overview_level.upsert(dataset_identifier=socrata_overview_level_dataset_app_id,
    #                                      payload=overview_outdated_row_ids_list,
    #                                      content_type='json')
    socrata_client_overview_level.close()
    exit()

    # Field level operations
    more_field_records_exist_than_response_limit_allows = True
    field_cycle_record_count = 0
    field_total_record_count = 0
    field_record_offset_value = 0
    while more_field_records_exist_than_response_limit_allows:
        field_dataset_url = build_dataset_url(url_root=ROOT_URL_FOR_DATASET_ACCESS,
                                              api_id=socrata_field_level_dataset_app_id,
                                              limit_amount=LIMIT_MAX_AND_OFFSET,
                                              offset=field_record_offset_value,
                                              total_count=field_total_record_count)
        field_response = requests.get(url=field_dataset_url)
        field_json = field_response.json()

        for obj in field_json:
            field_date = str(obj.get("date", None))
            field_date_obj = parser.parse(field_date)
            field_row_id = obj.get("row_id", None)
            if field_date_obj < baseline_date:
                overview_outdated_row_ids_list.append({":row_id": field_row_id, ":deleted": True})
        number_of_field_records_returned = len(field_json)


        #     if field_date_obj < baseline_date:
        #         field_outdated_row_ids_list.append({":row_id": field_row_id, ":deleted": True})
        # print(field_outdated_row_ids_list)

    # socrata_client_overview_level.upsert(dataset_identifier=socrata_overview_level_dataset_app_id,
    #                                      payload=field_outdated_row_ids_list,
    #                                      content_type='json')

    socrata_client_field_level.close()

    return


if __name__ == "__main__":
    main()
