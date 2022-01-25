from google.cloud import storage;
from apache_beam import

list_file_append=[]
list_blob_name=[]
if __name__ == '__main__':
   def fn_With_Cloudstorage():

        storage_client=storage.Client()
        bucket_name = 'york-project-bucket'
        #table_id = york-project-bucket/jayatestwithoutcsv/2021-12-19-22-43
        bucket = storage_client.get_bucket(bucket_name)
        delimiter='/'
        blobs = bucket.list_blobs(prefix='jayatestwithoutcsv/2021-12-19-22-43')
        for blob in blobs:
            list_blob_name.append(blob.name)

        for name_blob in list_blob_name:

           if('jayatestwithoutcsv/2021-12-19-22-43/part-r-' in name_blob):
               print(name_blob)
               blob_file= bucket.get_blob(name_blob)
               list_file_append.append(blob_file.download_as_text())


        with open('/Users/yorkmac040/PycharmProjects/pythonProject1/final.csv','w') as f:

           for i in list_file_append:
              f.write(i)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob('jayapythontest/jayaCSV1')
        blob.upload_from_filename('final.csv')


   def fn_With_ApachePipeline():




