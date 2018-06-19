# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 15:41:46 2018

@author: ADMIN
"""

import os
from os import listdir
import time
import re
import pandas as pd
import urllib2
from bs4 import BeautifulSoup


def get_industry(datasource, dataset_name):
    if "financ" in datasource: return "Finance"
    elif "health" in datasource: return "Healthcare"
    elif "manufact" in datasource: return "Manufacturing"
    elif "consumer" in datasource: return "Commerce"
    elif "climate" in datasource: return "Climate"
    elif "energy" in datasource: return "Energy"
    elif "local" in datasource: return "Local Govt"
    elif "quandl" in datasource: return "Finance"
    elif "industry_data" in datasource:
        if "i_facts" in dataset_name: return "Finance"
        elif "nadac" in dataset_name: return "Healthcare"
        elif "sec" in dataset_name: return "Finance"
        elif "twitter" in dataset_name: return "Finance"
    else: return ""


def web_request_datagov(dataset_name):
    browser = urllib2.build_opener()
    browser.addheaders = [('User-agent', 'Mozilla/5.0')]
    url = "https://catalog.data.gov/dataset/" + dataset_name
    response = browser.open(url)
    html = response.read()
    bsObj = BeautifulSoup(html)
    return bsObj


def get_title_desc_publisher(datasource, dataset_name, sub_dataset_path):
    details = []
    if "datagov" in datasource:
        try:
            bsObj = web_request_datagov(dataset_name)
            scrapped_title = bsObj.find("section", {"class": "module-content"}).find("h1").text
            scrapped_title = re.sub('[^A-Za-z0-9]+', ' ', scrapped_title)

            scrapped_desc = bsObj.find("div", {"class": "notes embedded-content"}).find("p").text
            scrapped_desc = re.sub('[^A-Za-z0-9]+', ' ', scrapped_desc)

            organization_name = bsObj.find("div", {"role": "main"}).find("li", {"class": "home"})\
                                     .findNextSiblings()[1].text

            publisher_name = bsObj.find("div", {"role": "main"}).find("li", {"class": "home"})\
                                  .findNextSiblings()[2].text

            details.append(scrapped_title.encode("utf-8"))
            details.append(scrapped_desc.encode("utf-8"))
            details.append(organization_name)
            details.append(publisher_name.replace("&nbsp;", ""))

            # return scrapped_title.encode("utf-8"), scrapped_desc.encode("utf-8"), organization_name,\
                    # publisher_name.replace("&nbsp;", "")
        except Exception:
            raise
            # return re.sub('[^A-Za-z0-9]+', ' ', dataset_name).encode("utf-8")

    elif "quandl" in datasource:
        with open(os.path.join(sub_dataset_path, 'info.info')) as dataset_info:
            dataset_metadata = dataset_info.readlines()

            details.append(dataset_metadata[0])
            details.append(dataset_metadata[2])
            details.append("")
            details.append(dataset_name.split('_')[0])

            # return dataset_metadata[0], dataset_metadata[2], "", dataset_name.split('_')[0]

    return details


def main():
    df = pd.DataFrame()
    i = 0
    datasets_path = os.path.join(os.getcwd(), "datasets")
    try:
        for datasource in listdir(datasets_path):
            print ">>>> Datasource >>  ", datasource
            datasource_path = os.path.join(datasets_path, datasource)
            try:
                for super_dataset in listdir(datasource_path):
                    print "SUPER >>", super_dataset
                    for sub_dataset in listdir(os.path.join(datasource_path, super_dataset)):
                        print "SUB >>>", sub_dataset
                        sub_dataset_path = os.path.join(datasource_path, super_dataset, sub_dataset)
                        for data_file in listdir(sub_dataset_path):
                            if (data_file.endswith(".csv") or data_file.endswith(".json")
                                    or data_file.endswith(".jsonl")):
                                print "FILE :: ", data_file
                                df.loc[i, 'datasource_name'] = datasource
                                df.loc[i, 'process_area'] = ""
                                dataset_name = super_dataset.replace("-", "_")[:60] + '_'\
                                               + sub_dataset.replace("-", "_")[:39]
                                df.loc[i, 'dataset_name_in_git'] = dataset_name
                                df.loc[i, 'dataset_name_in_yaml'] = "cortex/dataset_" + dataset_name
                                df.loc[i, 'industry'] = get_industry(datasource, dataset_name)
                                
                                details = get_title_desc_publisher(datasource, super_dataset, sub_dataset_path)
                                if len(details) > 0:
                                    df.loc[i, 'title_scrapped'] = details[0]
                                    df.loc[i, 'description_scrapped'] =  details[1]
                                    df.loc[i, 'organization_name'] = details[2]
                                    df.loc[i, 'publisher_name'] = details[3]
                                if 'datagov' in datasource:
                                    df.loc[i, 'url'] = "https://catalog.data.gov/dataset/" + super_dataset

                                file_size = os.path.getsize(os.path.join(sub_dataset_path, data_file))
                                df.loc[i, 'size_in_mb'] = file_size/float(1024*1024)
                                df.loc[i, 'file_name'] = data_file
                                df.loc[i, 'file_format'] = data_file.split(".")[-1]

                                i += 1
                                
                num_files = 0; num_folders = 0
                for _, dirnames, filenames in os.walk(datasource_path):
                    num_folders += len(dirnames)
                    num_files += len(filenames)
                print "Processed: ", num_folders, "datasets and ", num_files, "files in ", datasource
            except WindowsError:
                raise
    except WindowsError:
        raise

    inventory_writer = pd.ExcelWriter(os.path.join(os.getcwd(), "datasets_inventory_summary1.xlsx"),
                                      engine='xlsxwriter')
    df.to_excel(inventory_writer, 'Datasets')
    inventory_writer.save()

if __name__ == "__main__":
    prog_run_time = time.clock()
    main()
    # send_notification_email()
    print "\nProcessed time: ", (time.clock() - prog_run_time) / 60, " Min."
