import json
import os
from collections import defaultdict
from datetime import datetime
from configurationReader import ConfigReader


class JSONDuplicateRemover:

    def __init__(self,section_name:str,reader: ConfigReader):
        #initialize config reader to read duplicate configuration section in the config.ini
        self.section_name = section_name
        self.reader = reader
        

    def readJSON_config(self):
        #Reads configuration from the .ini file.

        self.section_name_dict = self.reader.get_section(self.section_name)
        if self.section_name_dict:
            return self.section_name_dict
        else:
            return {}

    def filter_duplicates(self, data,composite_keys, source_date_key):

        #Removes duplicates from JSON data based on composite keys
        data = data.sort_values(by=source_date_key, ascending=False)
        data = data.drop_duplicates(subset=[i for i in composite_keys], keep='first')
        
        return data

    def save_json(self,data,key,output_file_dir,filename):

        #Saves JSON data to a file
        json_data = data.to_json(orient='records')
        js_data = json.loads(json_data)
        root_js_data = {key:js_data}
        current_dir = os.getcwd()
        config_file_path = os.path.join(current_dir,output_file_dir, filename)

        with open(config_file_path, 'w') as file:
            json.dump(root_js_data, file, indent=4)
