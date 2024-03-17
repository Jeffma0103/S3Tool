from boto3.session import Session
import boto3
import os
import getpass


#Session.client init
aws_access_key_id = "ASIA5LZ7KUBTSXZENOTK"
aws_secret_access_key = "jVSsdUx8Pna696QH7zW8EFVaAMo/bxtQgwWuLX3r"
aws_session_token = "IQoJb3JpZ2luX2VjEK///////////wEaDmFwLW5vcnRoZWFzdC0xIkgwRgIhAIzDy3bL/+/1aQIZ5Lg2DMsXHYdK+y8SBiRFJA8c8m6AAiEAibCURFHcLmA0/dDJG1lUNWCGvd4U2QAPBagvpZ0f6S4qqwMIuP//////////ARAAGgw5MTg3MTg5NDc0MzEiDBJcwmIpeXLV6qgsGSr/AshV7S21K+vzubqGDer/TO3i549LnMmq198ntZHqBpyrLfxHqqh9oaNj9lKRPpkEikFtIzXEhMEDyIztmCXgTKH5Fei7wUqFsUptYfNfQvGpL/NWFHuOtM+Ivlp24/2kVg6DRNpgRktIzBE3OdNFIwG/lukZqsp21JmhDhBUOfsBQAxuSXGRoqx5sHg5HuWocm3Uiq+nfZ3jZ8kxdLRKd9E9+z3JvcLEeubGpTkwueXF8yXJby54JLwdOjAQ0lkdygecs6pUMh8ScbP8kCa9fb0sRbXD7mR3ljv6Yjr6/7vkppL8ziwaOglzKH8IDiAKMDevS9AqrVX9BLHxz2H8GaAqgVY/FMRDMCi5zB1pp3JklSeEgg5b9++28zUE6I6PGoRDROApr0IFHbSasoq0Uy+Flh+WJtqH3Y03JiFjqHl3SOl9dxrEYv78q0S7CYlqwBsMJ/bW+cnFFuOVWrDkymf/ikTUGXrpUWa0/3VDL3x1UEIfPq6Weyini999VT3QMO+g2q8GOqUB7ReipxaRv7MApi2IU6mAMU0vkE6nkWGDiJGjHtxjSFHhXNTNP6ylgwV4MVqOkx0EA803eiEJCFTSFanG5s5mRTKcJPT9ywdsQeiJ1oESXEj/n64FzR/3844avINAeK4xROT42py+K2lwuO1g+X4ukFDEdO+tLBSvptSBfYSIH//BzJaVfUU074bPekdt3KBiQ/CToTHDPGe8z2UdKcE625Ufvd6E"
url = "https://d-956779aa4b.awsapps.com/start#"
region_name = "ap-northeast-1"
bucket_name = "videopass-staging-source-upload"
folder_name = "thumbnail_w/"
file_name = "KKB202403080930.zip"



class S3Helper:
    def __init__(self, access_key, secret_key, session_token):
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.region_name = "ap-northeast-1"
        self.bucket_name = bucket_name
        self.url = url

        self.client = boto3.client(
            service_name='s3',
            region_name=self.region_name,
            aws_session_token=self.session_token,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )


    def get_bucket_list(self):
        result = []
        try:
            buckets_resp = self.client.list_buckets()
            for bucket in buckets_resp["Buckets"]:
                result.append(bucket["Name"])
        except TypeError:
            print("TypeError occurred")
            result.append("Config is incorrect")
        return result


    def search_bucket(self):
        buckets_resp = self.client.list_buckets()
        i = 0
        key_word = input("Keyword : ")
        for bucket in buckets_resp["Buckets"]:
            bucket_name = bucket["Name"]
            if key_word in bucket_name :
                i += 1
                print(f"{i} - {bucket_name}")
            else:
                continue
        if i == 0:
            print("No Results")

    # 列出 bucket 下的 folder 下的檔案
    def get_object_folder_list(self, bucket_name, folder_name):
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        i = 0
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])


    def download_metadata(self, bucket_name, destination_folder):
        destination_path = "data/metadata.zip"
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=destination_folder)

        if 'Contents' in response:
            # 如果存儲桶中有文件，則根據上傳時間對文件進行排序
            files = response['Contents']
            sorted_files = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            # 返回最新文件的名稱
            latest_file_key = sorted_files[0]['Key']
            # print(f"{latest_file_key}.zip")
            self.client.download_file(bucket_name, latest_file_key, destination_path)
        else:
            return None

    def download_mezzanine(self, bucket_name, destination_folder):
        destination_path = "data/mezzanine.mp4"
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=destination_folder)

        if 'Contents' in response:
            # 如果存儲桶中有文件，則根據上傳時間對文件進行排序
            files = response['Contents']
            sorted_files = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            # 返回最新文件的名稱
            latest_file_key = sorted_files[0]['Key']
            # print(f"{latest_file_key}.zip")
            self.client.download_file(bucket_name, latest_file_key, destination_path)
        else:
            return None

    def download_Thumbnail(self, bucket_name, destination_folder):
        destination_path = "data/thumbnail.png"
        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=destination_folder)

        if 'Contents' in response:
            # 如果存儲桶中有文件，則根據上傳時間對文件進行排序
            files = response['Contents']
            sorted_files = sorted(files, key=lambda x: x['LastModified'], reverse=True)
            # 返回最新文件的名稱
            latest_file_key = sorted_files[0]['Key']
            # print(f"{latest_file_key}.zip")
            self.client.download_file(bucket_name, latest_file_key, destination_path)
        else:
            return None


helper = S3Helper(aws_access_key_id, aws_secret_access_key, aws_session_token)
# print(helper.get_bucket_list())
helper.download_Thumbnail(bucket_name, folder_name)

