# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 12:24:55 2018

@author: ADMIN
"""

import os
import shutil

root_folder = os.getcwd()

rajiv_datasets_path = os.path.join(os.getcwd(), 'datasets_rajiv')
cleaned_datasets_path = os.path.join(os.getcwd(), 'datasets')

#print rajiv_datasets_path
#print cleaned_datasets_path

dataSources = ['dataworld_datasociety', 'dataworld_uci']
for dataSource in dataSources:
    dataSrcPath = os.path.join(rajiv_datasets_path, dataSource)
    for superDataset in os.listdir(dataSrcPath):
        superDatasetPath = os.path.join(dataSrcPath, superDataset)
        for subDataset in os.listdir(superDatasetPath):
            if 'deploy-dataset' not in subDataset:
                try:
                    print superDataset
                    srcPath = os.path.join(superDatasetPath, subDataset)
                    #print os.listdir(srcPath)
                    
                    destnPath = os.path.join(cleaned_datasets_path, dataSource,
                                             superDataset, subDataset)
                    print destnPath
                    #if not isexists()
                    shutil.move(srcPath, destnPath)
                except:
                    print("EXCEPTION")
                    continue
