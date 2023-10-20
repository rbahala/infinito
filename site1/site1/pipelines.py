# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
from os.path import exists
import scrapy
from scrapy.exporters import CsvItemExporter
from pymongo import MongoClient

class Site1Pipeline:
    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def exporter_for_item(self):
        self.file = open(f"site1/spiders/data/cloudflare.csv", "ab")
        self.exporter = CsvItemExporter(self.file, include_headers_line=False)
        self.exporter.start_exporting()        
    
    def process_item(self, item, spider):
        self.exporter_for_item()
        self.exporter.export_item(item)
        self.save_item(item)

        return item
        
    
    def save_item(self, item):
        client = MongoClient("localhost", 27017)
        db = client.blogs_db
        collection = db.cloudflare
        item['authors'] = [ author.strip() for author in item['authors'].split(";")]
 
        try:
            collection.insert_one(item)
        except Exception as e:
            print(f"Exception error: {e}")