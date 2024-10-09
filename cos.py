
import ibm_boto3
from ibm_botocore.client import Config, ClientError

# Constants for IBM COS values
COS_ENDPOINT = "https://s3.eu-de.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
COS_API_KEY = "zcmRKL20NeJ6_ImktRXU3-5zUF_3NVBHcMoVEe7tCG6Q" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/9f8f95eee4714473a87fa319d964063c:0901a11a-4f7c-4011-b7a1-be9e2f6ebe5e::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"

def get_buckets():
    print("Retrieving list of buckets")
    try:
        buckets = cos_client.list_buckets()
        for bucket in buckets["Buckets"]:
            print("Bucket Name: {0}".format(bucket["Name"]))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve list buckets: {0}".format(e))

def get_bucket_contents(bucket_name):
    print("Retrieving bucket contents from: {0}".format(bucket_name))
    try:
        files = cos_client.list_objects(Bucket=bucket_name)
        for file in files.get("Contents", []):
            print("Item: {0} ({1} bytes).".format(file["Key"], file["Size"]))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))

def get_bucket_contents_v2(bucket_name, max_keys):
    print("Retrieving bucket contents from: {0}".format(bucket_name))
    try:
        # create client object
        cos_client = ibm_boto3.client("s3",
            ibm_api_key_id=COS_API_KEY,
            ibm_service_instance_id=COS_INSTANCE_CRN,
            config=Config(signature_version="oauth"),
            endpoint_url=COS_ENDPOINT)

        more_results = True
        next_token = ""

        while (more_results):
            response = cos_client.list_objects_v2(Bucket=bucket_name, MaxKeys=max_keys, ContinuationToken=next_token)
            files = response["Contents"]
            for file in files:
                print("Item: {0} ({1} bytes).".format(file["Key"], file["Size"]))

            if (response["IsTruncated"]):
                next_token = response["NextContinuationToken"]
                print("...More results in next batch!\n")
            else:
                more_results = False
                next_token = ""

    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))

def get_item(bucket_name, item_name):
    print("Retrieving item from bucket: {0}, key: {1}".format(bucket_name, item_name))
    try:
        file = cos_client.get_object(Bucket=bucket_name, Key=item_name)
        print("File Contents: {0}".format(file["Body"].read()))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve file contents: {0}".format(e))

# Create resource
cos_resource = ibm_boto3.resource("s3",
    ibm_api_key_id=COS_API_KEY, 
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

# Create client
cos_client = ibm_boto3.client("s3",
    ibm_api_key_id=COS_API_KEY,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)

get_buckets()
get_bucket_contents("angel-bucket2")
get_item("angel-bucket2", "m31.FITS")