import configparser
import boto3

BUCKET = 'udacity-dend'


def list_files(s3, bucket_str, path):
    """ Check files in given bucket and path.
    
    Args:
        s3(boto3.resource): An s3 resource object.
        bucket_str(string): Bucket name.
        path(string): Path under which we look for files.
    Return: None
    """
    bucket =  s3.Bucket(bucket_str)
    for obj in bucket.objects.filter(Prefix=path):
        print(obj)

def read_file(s3, bucket_str, path):
    """ Read a file in the S3 path.

    Args:
        s3(boto3.resource): An s3 resource object.
        bucket_str(string): Bucket name.
        path(string): Path under which we look for files.
    Return:
        string: Content of the file, encoded with utf-8.
    """
    obj = s3.Object(bucket_str, path)
    return obj.get()['Body'].read().decode('utf-8')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=config['USER']['ACCESS_KEY'],
                           aws_secret_access_key=config['USER']['ACCESS_SECRET']
                         )
    

    # list_files(s3, BUCKET, 'song_data')
    list_files(s3, BUCKET, 'log_data')
    json = read_file(s3, BUCKET, 'log_json_path.json')
    print(json)



if __name__ == "__main__":
    main()