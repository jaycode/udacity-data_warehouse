import configparser
import boto3

BUCKET = 'udacity-dend'


def list_files(s3, bucket_str, path):
    bucket =  s3.Bucket(bucket_str)
    for obj in bucket.objects.filter(Prefix=path):
        print(obj)

def read_json(s3, bucket_str, path):
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
    json = read_json(s3, BUCKET, 'log_json_path.json')
    print(json)



if __name__ == "__main__":
    main()