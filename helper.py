INTERNAL_BUCKET = {}
PUBLIC_BUCKET = {}

def uploadFileStreamToOss(full_file_name, stream):
    #Make sure your ram role has the upload oss policy. Refer to: fc-policy-sample-code_allow-upload-oss 
    bucket = INTERNAL_BUCKET
    bucket.put_object(full_file_name, stream)
    print(f"File uploaded into: {full_file_name}")

def downloadFileStreamFromOss(full_file_name):
    #Make sure your ram role has the download oss policy. Refer to: fc-policy-sample-code_allow-download-oss 
    bucket = INTERNAL_BUCKET
    content = bucket.get_object(full_file_name).read()
    print (f"File stream: {content}")
    return content

def downloadFileObjectFromOss(full_file_name, destination_dir):
    #Make sure your ram role has the download oss policy. Refer to: fc-policy-sample-code_allow-download-oss 
    import os
    os.system(f"mkdir -p {'/'.join(destination_dir.split('/')[:-1])}")
    bucket = INTERNAL_BUCKET
    bucket.get_object_to_file(full_file_name, destination_dir).__dict__
    print(f"File Downloaded: {destination_dir}")
    return ""

def signOssObject(http_method, file_name, expiration_time, headers={}, public_access=False):
    bucket = PUBLIC_BUCKET if public_access==True else INTERNAL_BUCKET
    signed_url = f"{bucket.sign_url(http_method, file_name, expiration_time, headers)}"
    print(f"Signed Url expired in {expiration_time} seconds: {signed_url[0:80]} <redacted because too long>")
    return signed_url

def renderImage(file_url):
    import urllib.request
    img = urllib.request.urlopen(file_url).read()
    print(f"Image stream: {img[0:20]} <redacted because too long>")
    return img

def downloadFileFromUrl(file_url): 
    import urllib.request
    content = urllib.request.urlopen(file_url).read()
    print(f"File stream: {content}")
    return content

def extractZipFileToOss(source_zip_file, destination_dir="", clean=True):
    if len(destination_dir)>0 and destination_dir[-1] != "/":
        destination_dir+= "/"
    from io import BytesIO
    import oss2
    import zipfile
    source_bucket = INTERNAL_BUCKET
    destination_bucket = INTERNAL_BUCKET
   
    object_stream = source_bucket.get_object(source_zip_file).read()
    myzipfile = zipfile.ZipFile(BytesIO(object_stream))
    
    if clean:
        print("Cleaning target directory")
        existing_files=[]
        for obj in oss2.ObjectIterator(destination_bucket, prefix = destination_dir):
            existing_files.append(obj.key)
            if len(existing_files)>0:
                destination_bucket.batch_delete_objects(existing_files)
    print("Uploading ")
    for filename in myzipfile.namelist():
        destination_bucket.put_object(f"{destination_dir}{filename}", myzipfile.open(filename).read())

def multipart_upload(source_full_file_name, destination_full_file_name, chunk_size):
    print(source_full_file_name, destination_full_file_name)
    import os
    import oss2
    total_size = os.path.getsize(source_full_file_name)
    part_size = oss2.determine_part_size(total_size, preferred_size = chunk_size)
    bucket = INTERNAL_BUCKET
    upload_id = bucket.init_multipart_upload(destination_full_file_name).upload_id
    with open(source_full_file_name, 'rb') as fileobj:
        parts = []
        part_number = 1
        offset = 0
        while offset < total_size:
            num_to_upload = min(part_size, total_size - offset)
            result = bucket.upload_part(destination_full_file_name, upload_id, part_number,oss2.SizedFileAdapter(fileobj, num_to_upload))
            parts.append(oss2.models.PartInfo(part_number, result.etag))
            offset += num_to_upload
            part_number += 1
        
        bucket.complete_multipart_upload(destination_full_file_name, upload_id, parts)
        
    return total_size

def simple_upload(source_file_name, destination_file_name):
    print(source_file_name, destination_file_name)
    bucket = INTERNAL_BUCKET
    with open(source_file_name, 'rb') as fileobj:
        bucket.put_object(destination_file_name, fileobj)


#prepare_form_upload_key(context, "2020-02-10T00:00:00Z", 1048576) 
def prepare_form_upload_key(context, expiration_time, max_file_size):
    import hashlib
    import hmac
    creds = context.credentials
    policy=f'{{"expiration":"{expiration_time}","conditions":[["content-length-range", 0, {max_file_size}]]"}}'.encode("UTF-8")
    base64policy = base64.b64encode(policy)
    signature = base64.b64encode(hmac.new(creds.accessKeySecret.encode("UTF-8"), base64policy, hashlib.sha1).digest())
    print(f"creds.accessKeyId")
    print(f"base64_encode_policy: {base64policy}")
    print(f"signature: {signature}")   
    #req = Request("POST", "http://doit-build-5263863254564512.oss-ap-southeast-5.aliyuncs.com/test.csv")
    #signature2 = auth._sign_url(req, "doit-build-5263863254564512", "test.csv", 500)

SQL_CONNECTION = {}
def connect_to_mysql(context, mysql_config):
    import minipymysql as pymysql
    return pymysql.connect(
        host=mysql_config['host'],
        port=3306,
        user=mysql_config['username'],
        password=decrypt_string(context, mysql_config['password']),
        db=mysql_config['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.SSDictCursor)

def get_configuration(context, group_code):
    import os
    from tablestore import OTSClient
    creds = context.credentials
    client = OTSClient(os.environ['TABLE_STORE_ENDPOINT'],
        creds.accessKeyId, creds.accessKeySecret,
        os.environ['TABLE_STORE_INSTANCE_NAME'],
        sts_token=creds.securityToken)
    primary_key = [('group', group_code)]
    columns_to_get = []
    consumed, return_row, next_token = client.get_row(
        os.environ['TABLE_STORE_TABLE_NAME'],
        primary_key,
        columns_to_get, None, 1)
    json = {}
    for att in return_row.attribute_columns:
        json[att[0]] = att[1]
    return json

def decrypt_string(context, encrypted_string):
    import json
    from aliyunsdkcore.auth import credentials
    from aliyunsdkcore.client import AcsClient  
    from aliyunsdkkms.request.v20160120.DecryptRequest import DecryptRequest
    creds = context.credentials
    sts_credentials = credentials.StsTokenCredential(creds.accessKeyId, creds.accessKeySecret, creds.securityToken) 
    client = AcsClient(region_id = 'ap-southeast-5',credential = sts_credentials)
    request = DecryptRequest()
    request.set_CiphertextBlob(encrypted_string)
    response = str(client.do_action_with_exception(request), encoding='utf-8')
    return json.loads(response)['Plaintext']

NOTIFICATION_CONFIG = {}
def send_email(origin, subject, message, to, cc=None, bcc=None, attachments=None):
    import requests
    import json
    data = {
                "subject": subject,
                "message": message,
                "to": to,
                "cc": cc if cc is not None else "",
                "bcc": bcc if bcc is not None else "",
                "attachments": attachments if attachments is not None else []
            }
     
    token = NOTIFICATION_CONFIG['x-authorization-token']
    endpoint = NOTIFICATION_CONFIG['endpoint']
    headers = {'Content-Type': 'application/json', 'x-authorization-token': token, 'Origin':f"{NOTIFICATION_CONFIG['endpoint'].replace('notificationapi', origin)}"}
    resp = requests.post(f"{endpoint}/email",
        data=json.dumps(data), headers=headers)
        