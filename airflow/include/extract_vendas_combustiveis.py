import os
import boto3
import logging
import subprocess
import pandas as pd


class extractVendasCombustiveis:
    def __init__(self, s3client):
        self.s3client = s3client
        self.url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
        self.filename = self.url.split('/')[-1]

    def remove_temp_files(self) -> None:
        logging.info('Removing temp files...')
        bashCommand = f'rm vendas*.xls && rm sales*.parquet && rm -rf ./converted'
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        logging.info('Temp files deleted!')
        return

    def download_file(self) -> None:
        """Downloads the excel file from the web
        """
        try:
            logging.info('Getting the file...')
            bashCommand = f'curl -L -O {self.url}'
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            logging.info('File downloaded!')
            return
        except:
            logging.error('Error when trying to download the file.')
            self.remove_temp_files()
            raise

    def convert_file(self) -> None:
        """Convert the xls file
        """
        try:
            logging.info('Converting the file...')
            bashCommand = 'libreoffice --headless --convert-to xls --outdir ./converted ./vendas-combustiveis-m3.xls'
            process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()
            logging.info('File converted!')
            return
        except:
            logging.error('Error when trying to convert the file.')
            self.remove_temp_files()
            raise
    
    def unshift(self, data: list) -> list:
        """Fix the shifted data problem.
        Cycles between the values and create a new variable with a shift of 1 column.
        The shift index increases in 1 for each row and ends the cyclic at the 13th column.

        Args:
            data (list): Values for each month and total for the record

        Returns:
            (list): Data with the column's order fixed.
        """
        global idx_shift
        if idx_shift > 12:
            idx_shift = 1  # Resets cyclic the shift index if the month range reaches the end
        else:
            idx_shift += 1  # Increase the data shift index for the actual row

        data_fixed = [0]*13  # Init the fixed data variable
        for i in range(13):
            idx = i + idx_shift
            if idx > 12:
                idx = idx - 13  # Resets the cyclic the month range if it reaches the end
            data_fixed[i] = data[idx]

        return data_fixed

    def fix_and_upload_sheet_to_s3(self, sheet_name: str, tablename: str) -> None:
        """ unshift the sheet and upload to the S3 bronze bucket

        Args:
            sheet_name (str): Sheet's name to be extracted.
            tablename (str): Table's name to be used in the parquet file.
        """
        filepath = f"{tablename}.parquet"

        try:
            df = pd.read_excel(f'./converted/{self.filename}', sheet_name=sheet_name)

            # Unshift the data
            for rowindex, row in df.iterrows():
                df.iloc[rowindex, 4:17] = self.unshift(row[4:17].values.flatten().tolist())

            df.to_parquet(path=filepath)
            boto3.resource('s3').Bucket('anp-bronze').upload_file(filepath, filepath)
            return
        except:
            logging.error('Error when trying to upload the sheet.')
            self.remove_temp_files()
            raise
    
    def get_vendas_combustiveis(self):
        """Main function for the data extraction
        """
        global idx_shift
        idx_shift = 0

        self.download_file()
        self.convert_file()
        self.fix_and_upload_sheet_to_s3(sheet_name='DPCache_m3', tablename='sales_oil_fuels')
        self.fix_and_upload_sheet_to_s3(sheet_name='DPCache_m3_2', tablename='sales_diesel')
        self.remove_temp_files()


if __name__ == '__main__':
    # This code is for debug only.
    logging.basicConfig(level='INFO')
    logging.getLogger("urllib3").setLevel(logging.WARNING)        

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3client = boto3.client(
        "s3", aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        )

    pipeline = extractVendasCombustiveis(s3client)
    pipeline.get_vendas_combustiveis()
