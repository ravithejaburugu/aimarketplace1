# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 13:52:03 2018

@author: ADMIN
"""

import time
import os
import requests
import json
import urllib2
import zipfile
import re
import pandas as pd
import codecs
from pget.down import Downloader


quandl_path = os.path.join(os.getcwd(), "datasets", "quandl")
quandl_apikey = 'o7xFVwAfWTqCsY5mgMGh'  # "zWss8KsbxmzVojqwVr9E"
q_databases_url = "https://www.quandl.com/api/v3/databases?api_key={0}&page={1}"
q_codes_url = "https://www.quandl.com/api/v3/databases/{0}/codes.json?api_key={1}"
q_datasets_url = "https://www.quandl.com/api/v3/datasets/{0}?api_key={1}"


def getQuandlDatabases():
    """Makes a collection of non-premium Quandl databases into 'quandlinfo.json' that are to be downloaded."""
    page = 0
    database_codes = []
    premium_codes = []
    total_codes = 0
    json_data = {}
    quandl_databases = {'databases':[]}
    try:
        while len(database_codes) == 0 or json_data['meta']['total_count'] > total_codes:
            page += 1
            q_db_URL = q_databases_url.format(quandl_apikey, str(page))
            json_data = (requests.get(q_db_URL)).json()
            
            for d in json_data['databases']:
                if not d['premium']:
                    quandl_databases['databases'].append(d)
                    database_codes.append(d['database_code'])
                if d['premium']:
                    premium_codes.append(d['database_code'])

            total_codes = len(database_codes) + len(premium_codes)
        print len(database_codes)
        with open(os.path.join(quandl_path, 'quandlinfo.json'), 'a+') as quandlinfo:
            json.dump(quandl_databases, quandlinfo, indent=4)
    except:
        raise


def getCSVQuandlCodesFromZips():
    """From 'quandlinfo.json', database codes are observed to download Zips which are extracted to super folders."""
    with open(os.path.join(quandl_path, 'quandlinfo.json')) as quandlinfo:
        quandl_databases = json.load(quandlinfo)
        for quandl_info in quandl_databases['databases']:
            qcode = quandl_info['database_code']
            url_name = quandl_info['url_name']
            super_dataset = url_name.replace('-', '_').lower()

            # print "CODE >> " + qcode
            zip_filename = qcode + '.zip'
            zip_file_path = os.path.join(quandl_path, zip_filename)

            if not os.path.isfile(zip_file_path):
                try:
                    print "downloading..." + zip_filename
                    time.sleep(3)
                    resp = urllib2.urlopen(q_codes_url.format(qcode, quandl_apikey))
                    zipcontent = resp.read()
    
                    with open(zip_file_path, 'wb') as zfw:
                        zfw.write(zipcontent)
                except Exception:
                    print "EXCEPTION while Downloading.. " + zip_filename
                    continue

            if not os.path.isdir(os.path.join(quandl_path, super_dataset)):
                try:
                    with zipfile.ZipFile(zip_file_path, "r") as zfr:
                        zfr.extractall(os.path.join(quandl_path, super_dataset))
                except Exception:
                    print "EXCEPTION while Extracting.. " + zip_filename
                    continue


def downloadQuandlDatasets():
    """Every super folder contains a csv of codes to download actual csv datasets in their sub folders
    along with their meta information of description, etc. in a separate info file."""
    quandl_info = []
    with open(os.path.join(quandl_path, 'quandlinfo.json')) as quandlinfo:
        quandl_databases = json.load(quandlinfo)
        quandl_info = quandl_databases['databases']

    for super_dataset in os.listdir(quandl_path):
        super_dataset_path = os.path.join(quandl_path, super_dataset)

        if os.path.isdir(super_dataset_path):
            for csv_file in os.listdir(super_dataset_path):
                print '-->> ', csv_file
                csv_file_r = None
                csvlines = []
                json_data = ''
                sub_dataset_name = ''
                dataset_description = ''
                try:
                    try:
                        with open(os.path.join(super_dataset_path, csv_file)) as csv_file_r:
                            csvlines = csv_file_r.readlines()
                    except IOError:
                        continue
                        
                    if len(csvlines) == len(os.listdir(super_dataset_path))-1:
                        continue
                        
                    for i, line in enumerate(csvlines, start=1):
                        codeline = line.split(',')
                        if len(codeline) > 1:
                            dataset_code = codeline[0]

                            time.sleep(1)
                            resp = os.popen("curl " + q_datasets_url.format(dataset_code, quandl_apikey))
                            resp_data = resp.read()
                            try:
                                json_data = json.loads(resp_data)
                            except ValueError:
                                continue
                                                        
                            sub_dataset_name = re.sub("[^A-Za-z0-9 ]+", "", json_data["dataset"]["name"])
                            sub_dataset_name = re.sub(" +", " ", sub_dataset_name).replace(" ", "_").lower()

                            print str(i) + " >>> " + sub_dataset_name

                            sub_dataset_path = os.path.join(super_dataset_path, sub_dataset_name)
                            if not os.path.isdir(sub_dataset_path):
                                os.mkdir(sub_dataset_path)
                            
                            data_file = dataset_code.split("/")[1] + '.csv'
                            if not os.path.isfile(os.path.join(sub_dataset_path, data_file)):
                                df = pd.DataFrame(data=json_data["dataset"]["data"],
                                                  columns=json_data["dataset"]["column_names"])
                                df.to_csv(os.path.join(sub_dataset_path, data_file), index=False)

                                file_size = os.path.getsize(os.path.join(sub_dataset_path, data_file))
                                size_in_mb = file_size/float(1024*1024)
                                #if size_in_mb > 1:
                                with codecs.open(os.path.join(sub_dataset_path, 'info.info'), 'w', encoding='utf-8') as sub_data_info:
                                    super_dataset_desc = [info['description'] for info in quandl_info
                                                          if dataset_code.split("/")[0] in info['database_code']]
                                    if (not super_dataset_desc) or len(super_dataset_desc) == 0 \
                                            or super_dataset_desc == None:
                                        super_dataset_desc = ''
                                    else:
                                        super_dataset_desc = super_dataset_desc[0] + '\n'
                                    dataset_description =  super_dataset_desc + json_data["dataset"]["description"]
                                    sub_data_info.write(json_data["dataset"]["name"] + '\n'
                                                        + sub_dataset_name + '\n'
                                                        + dataset_description + '\n'
                                                        + str(q_datasets_url.split('?')[0])\
                                                          .format(dataset_code))
                except Exception:
                    if csv_file_r:
                        csv_file_r.close()
                    print '222', sub_dataset_name
                    print '333', dataset_description[:50]
                    raise
                    #continue


def main():
    # getQuandlDatabases()
    # getCSVQuandlCodesFromZips()
    downloadQuandlDatasets()


if __name__ == "__main__":
    prog_run_time = time.clock()
    main()
    print "\nProcessed time: ", (time.clock() - prog_run_time) / 60, " Min."
