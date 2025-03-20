import boto3
import os
from pandas import DataFrame


class s3Interactions:
    def __init__(self, aws_secret, access_key):
        self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=aws_secret)

    def list_files(self, s3_path):
        """
        Get a list of all keys in an S3 bucket.

        :param s3_path: Path of S3 dir.
        """
        keys = []

        if not s3_path.startswith('s3://'):
            s3_path = 's3://' + s3_path

        bucket = s3_path.split('//')[1].split('/')[0]
        prefix = '/'.join(s3_path.split('//')[1].split('/')[1:])

        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)
            try:
                for obj in resp['Contents']:
                    keys.append(obj['Key'])
            except:
                return []

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return keys

    def get_file(self, bucket, file_path):
        response = self.s3_client.get_object(Bucket=bucket, Key=file_path)
        return response['Body'].read().decode('utf-8')

    def delete_file(self, bucket, file_path):
        self.s3_client.delete_object(Bucket=bucket, Key=file_path)

    def write_file(self, file_to_upload ,bucket, file_path):
        try:
            response = self.s3_client.upload_file(file_to_upload, bucket, file_path)
        except Exception as e:
            raise e
        return True

    def check_file_exists(self, file_path, file_name):
        files = self.list_files(file_path)
        if file_name in files:
            return True
        return False

    def dump_data_in_s3(self, bucket_name:str, file_prefix:str, df:DataFrame):
        date_cols = ["month", "year", "day"]
        cols = df.columns
        select_cols = list(set(cols)-set(date_cols))
        dates = df[[*date_cols]].drop_duplicates()
        for idx, row in dates.iterrows():
            year = row["year"]
            month = row["month"]
            day = row["day"]
            date = f"{year}_{month}_{day}"
            df[((df["month"]==month) & (df["year"]==year) & (df["day"]==day))][[*select_cols]].to_csv(f"tmp_{file_prefix}.csv")
            try:
                self.write_file(bucket=bucket_name, file_path=f"{year}/{month}/{file_prefix}_{date}.csv", file_to_upload=f"tmp_{file_prefix}.csv")
            except Exception as e:
                os.remove(f"tmp_{file_prefix}.csv")
                raise f"Error while uploading file {year}/{month}/{file_prefix}_{date}.csv: {e}"
            print(f"added {file_prefix} at : {year}/{month}/{file_prefix}_{date}.csv")
