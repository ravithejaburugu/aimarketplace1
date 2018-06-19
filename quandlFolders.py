# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 15:09:09 2018

@author: ADMIN
"""


import os
import shutil

root_folder = os.getcwd()

rajiv_datasets_path = os.path.join(os.getcwd(), 'datasets_rajiv')
cleaned_datasets_path = os.path.join(os.getcwd(), 'datasets')

dataSource = 'quandl'
quandlPath = os.path.join(cleaned_datasets_path, dataSource)

for path, dirs, files in os.walk(quandlPath):
    print path
    for superDataset in dirs:
        #print superDataset
        #print os.listdir(os.path.join(quandlPath, superDataset))
        superPath = os.path.join(quandlPath, superDataset)
        for subDataset in os.listdir(superPath):
            filescount = len(os.listdir(os.path.join(superPath, subDataset)))
            #print "  >> " + subDataset + " - " + str(len(os.listdir(os.path.join(superPath, subDataset))))
            if filescount > 1:
                print superDataset + "  >> " + subDataset
        
    break