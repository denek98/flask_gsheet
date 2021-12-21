from oauth2client.service_account import ServiceAccountCredentials

import gspread
import gspread_dataframe as gd
import os
import pandas as pd
import json

class Gsheet():
    '''
    Main methods to use :
    1. dataframe_from_input() - returns dataframe that was collected from google sheet
    2. upload_new_data_to_sheet(DataFrame) - uploads given DataFrame to google sheet
    '''
    def __init__(self):

        self.__app_path = os.path.join(os.path.abspath(os.getcwd()),'application')
        # ---------------------------------------------------------------------------
        # Gsheet settings 
        self.__settings_json_file_name = 'settings.json'
        self.__settings_json_file_path = os.path.join(self.__app_path,self.__settings_json_file_name)
        self.__gsheet_settings = self.__get_gsheet_settings_from_json()
        self.__sheet_url = self.__gsheet_settings['sheet_url']
        self.__input_sheet_name = self.__gsheet_settings['input_sheet_name']
        self.__output_sheet_name = self.__gsheet_settings['output_sheet_name']
        # ---------------------------------------------------------------------------
        # Google authentification setup
        self.__creds_file_name = self.__gsheet_settings['creds_file_name']
        self.__creds_file_path = os.path.join(self.__app_path,'google_sheet',self.__creds_file_name)  
        # ---------------------------------------------------------------------------

    @property
    def dataframe_from_input(self):
        '''
        Property to get dataframe from google sheet
        '''
        return self.__read_data_from_sheet_to_dataframe()

    def __get_gsheet_settings_from_json(self):
        '''
        Method to extract settings from json file
        Returns dictionary with setting
        '''
        with open(self.__settings_json_file_path, "r") as jsonFile:
            gsheet_settings = json.load(jsonFile)["gsheet_settings"]
            jsonFile.close()
        return gsheet_settings

    def __auth_for_gsheet(self,sheet_name):
        '''
        Method to get access to google sheet
        :param sheet_name: name of sheet to get access to
        :type sheet_name: string
        Returns connection to given sheet. Needed for reading/writing 
        data from/to google sheet
        '''
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.__creds_file_path, scope)
        client = gspread.authorize(creds)
        wsh = client.open_by_url(self.__sheet_url).worksheet(sheet_name)
        return wsh

    def __read_data_from_sheet_to_dataframe(self):
        '''
        Method to read data from google sheet and represent
        it as dataframe
        (!) Method reads info only from INPUT sheet
        Returns dataframe with google sheet's info
        '''
        auth_gsheet = self.__auth_for_gsheet(self.__input_sheet_name)
        gsheet_df = gd.get_as_dataframe(auth_gsheet)
        gsheet_df.dropna(inplace=True,how = 'all')
        gsheet_df.dropna(axis = 1,inplace=True,how = 'all')
        return gsheet_df

    def __get_headers_from_output_sheet(self):
        '''
        (!) Returns DataFrame
        Need this method to keep all columns in resulted DataFrame
        '''
        auth_gsheet = self.__auth_for_gsheet(self.__output_sheet_name)
        gsheet_df = gd.get_as_dataframe(auth_gsheet,header = None) # (!) 'header = None' is needed to check for empty columns
        gsheet_df.dropna(inplace=True,how = 'all')
        gsheet_df.dropna(axis = 1,inplace=True,how = 'all')
        gsheet_df.columns = gsheet_df.iloc[0] # setting header as first row of DataFrame
        output_headers = gsheet_df.iloc[0:0] # getting header as DataFrame
        return output_headers

    def upload_new_data_to_sheet(self,dataframe_to_upload):
        '''
        Concats headers of output sheet and given df
        Overwrites output sheet with the result of concatination
        :param dataframe_to_upload: DataFrame which needs to be written
        to google sheet
        :type dataframe_to_upload: DataFrame
        (!) Resulting columns will always be the same as they are in Output tab
        '''
        auth_gsheet = self.__auth_for_gsheet(self.__output_sheet_name)
        headers_from_output_sheet = self.__get_headers_from_output_sheet()
        new_df = pd.concat([headers_from_output_sheet,dataframe_to_upload]) # Concating given DataFrame and headers from output
        new_df.drop(columns=[col for col in new_df if col not in headers_from_output_sheet.columns.tolist()], inplace=True) # Checking if resulted dataframe contains extra columns and drops them
        new_df.dropna(inplace=True,how = 'all')
        gd.set_with_dataframe(auth_gsheet,new_df,resize=True)

