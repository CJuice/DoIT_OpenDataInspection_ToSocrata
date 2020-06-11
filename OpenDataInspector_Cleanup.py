"""
Procedural script that deletes aged records in the ODI and GODI datasets on opendata.maryland.gov Socrata portal.

This script gets the ODI and GODI datasets on Socrata, examines the date value in every record, inventories the
records that meet the date logic check, and then deletes those records. It is intended to flush old records from the
datasets. It could easily be adapted to any dataset by changing the 4 by 4 code and providing and application id
for editing the datasets.

"""
# TODO: Documentation
# TODO: Switch baseline date to be a moving window of time. So, datetime.now - 12 months for example
# TODO: Deploy as a visual cron task

# TODO: Needs to be adjusted to run on GODI datasets too. Currently only hits ODI datasets

def main():

    # IMPORTS
    from datetime import datetime
    from sodapy import Socrata
    import configparser
    import dateutil.parser as parser
    import os
    import time

    # VARIABLES
    TESTING = True                              # OPTION

    _root_url_for_project = os.path.dirname(__file__)
    baseline_date = datetime(2018, 8, 3)  # FIXME
    config_file = None  # See variable assignment below. Depends on TESTING variable.
    field_outdated_row_ids_list = []
    limit_max_and_offset = 10000
    opendata_maryland_gov_domain = "opendata.maryland.gov"
    opendata_maryland_gov_url = r"https://{domain}".format(domain=opendata_maryland_gov_domain)
    overview_outdated_row_ids_list = []
    root_url_for_dataset_access = r"{root}/resource/".format(root=opendata_maryland_gov_url)

    # ASSERTS
    # CLASSES

    # FUNCTIONS
    def build_dataset_url(url_root: str, api_id: str, limit_amount: int = 0, offset: int = 0, total_count: int = None) -> str:
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
        elif total_count >= limit_max_and_offset:
            return "{}{}.json?$limit={}&$offset={}".format(url_root, api_id, limit_amount, offset)
        else:
            return "{}{}.json?$limit={}".format(url_root, api_id, limit_amount)

    def create_socrata_client(cfg_parser: configparser.ConfigParser, maryland_domain: str, dataset_key: str) -> Socrata:
        """
        Create and return a Socrata client for use.

        NOTE_1: It seems absolutely essential the the domain be a domain and not a url; 'https://opendata.maryland.gov'
            will not substitute for 'opendata.maryland.gov'.

        :param cfg_parser: config file parser
        :param dataset_key: the section key of interest
        :param maryland_domain: domain for maryland open data portal.
        :return: Socrata connection client
        """

        app_token = cfg_parser[dataset_key]["APP_TOKEN"]
        username = cfg_parser["DEFAULT"]["USERNAME"]
        password = cfg_parser["DEFAULT"]["PASSWORD"]
        return Socrata(domain=maryland_domain, app_token=app_token, username=username, password=password)

    def setup_config(cfg_file: str) -> configparser.ConfigParser:
        """
        Instantiate the parser for accessing a config file.
        :param cfg_file: config file to access
        :return:
        """
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(filenames=cfg_file)
        return cfg_parser

    # FUNCTIONALITY
    print(f"Testing variable = {TESTING}")

    if TESTING:
        config_file = r"EssentialExtraFilesForOpenDataInspectorSuccess\Credentials_TESTING.cfg"  # TEST

    else:
        config_file = r"EssentialExtraFilesForOpenDataInspectorSuccess\Credentials.cfg"  # PROD

    config_parser = setup_config(cfg_file=config_file)

    # Socrata related variables, derived
    socrata_client_field_level = create_socrata_client(cfg_parser=config_parser,
                                                       maryland_domain=opendata_maryland_gov_domain,
                                                       dataset_key="FIELD")
    socrata_field_level_dataset_app_id = config_parser["FIELD"]["APP_ID"]

    socrata_client_overview_level = create_socrata_client(cfg_parser=config_parser,
                                                          maryland_domain=opendata_maryland_gov_domain,
                                                          dataset_key="OVERVIEW")
    socrata_overview_level_dataset_app_id = config_parser["OVERVIEW"]["APP_ID"]

    # __________________________________________________________________________
    # Overview level operations
    print("Entering Overview Dataset Operations")
    more_overview_records_exist_than_response_limit_allows = True
    overview_total_record_count = 0
    overview_record_offset_value = 0

    while more_overview_records_exist_than_response_limit_allows:
        overview_cycle_record_count = 0

        # Only useful for understanding what the client.get call is doing
        print(build_dataset_url(url_root=root_url_for_dataset_access,
                                api_id=socrata_overview_level_dataset_app_id,
                                limit_amount=limit_max_and_offset,
                                offset=overview_record_offset_value,
                                total_count=overview_total_record_count))

        # for the private test datasets I had to use the client to access the data. May just move to this style.
        overview_response = socrata_client_overview_level.get(dataset_identifier=socrata_overview_level_dataset_app_id,
                                                              content_type="json",
                                                              limit=limit_max_and_offset,
                                                              offset=overview_record_offset_value)

        for obj in overview_response:
            overview_date = obj.get("date", None)
            overview_date_obj = parser.parse(overview_date)
            overview_row_id = obj.get("row_id", None)
            if overview_date_obj < baseline_date:
                overview_outdated_row_ids_list.append({"row_id": overview_row_id, ":deleted": True})

        # number_of_overview_records_returned = len(overview_json)
        number_of_overview_records_returned = len(overview_response)
        overview_cycle_record_count += number_of_overview_records_returned
        overview_total_record_count += number_of_overview_records_returned

        # Any cycle_record_count that equals the max limit indicates another request is needed
        if overview_cycle_record_count == limit_max_and_offset:

            # Give Socrata servers small interval before requesting more
            time.sleep(0.2)
            overview_record_offset_value = overview_cycle_record_count + overview_record_offset_value
        else:
            more_overview_records_exist_than_response_limit_allows = False

    print(overview_outdated_row_ids_list)

    delete_response_overview = socrata_client_overview_level.upsert(
        dataset_identifier=socrata_overview_level_dataset_app_id,
        payload=overview_outdated_row_ids_list,
        content_type='json')

    print(delete_response_overview)

    socrata_client_overview_level.close()

    # __________________________________________________________________________
    # Field level operations
    print("Entering Field Dataset Operations")
    more_field_records_exist_than_response_limit_allows = True
    field_total_record_count = 0
    field_record_offset_value = 0
    while more_field_records_exist_than_response_limit_allows:
        field_cycle_record_count = 0

        # Only useful for understanding what the client.get call is doing
        field_dataset_url = build_dataset_url(url_root=root_url_for_dataset_access,
                                              api_id=socrata_field_level_dataset_app_id,
                                              limit_amount=limit_max_and_offset,
                                              offset=field_record_offset_value,
                                              total_count=field_total_record_count)
        print(field_dataset_url)

        field_response = socrata_client_field_level.get(dataset_identifier=socrata_field_level_dataset_app_id,
                                                        content_type="json",
                                                        limit=limit_max_and_offset,
                                                        offset=field_record_offset_value)

        for obj in field_response:
            field_date = str(obj.get("date", None))
            field_date_obj = parser.parse(field_date)
            field_row_id = obj.get("row_id", None)
            if field_date_obj < baseline_date:
                field_outdated_row_ids_list.append({"row_id": field_row_id, ":deleted": True})

        # number_of_field_records_returned = len(field_json)
        number_of_field_records_returned = len(field_response)
        field_cycle_record_count += number_of_field_records_returned
        field_total_record_count += number_of_field_records_returned

        # Any cycle_record_count that equals the max limit indicates another request is needed
        if field_cycle_record_count == limit_max_and_offset:

            # Give Socrata servers small interval before requesting more
            time.sleep(0.2)
            field_record_offset_value = field_cycle_record_count + field_record_offset_value
        else:
            more_field_records_exist_than_response_limit_allows = False

    print(field_outdated_row_ids_list)

    delete_response_field = socrata_client_overview_level.upsert(
        dataset_identifier=socrata_field_level_dataset_app_id,
        payload=field_outdated_row_ids_list,
        content_type='json')

    print(delete_response_field)

    socrata_client_field_level.close()

    return


if __name__ == "__main__":
    main()
