"""Create or Destroy data resource
"""

import configparser
import argparse
import psycopg2
import csv
import json
import time
import logging
import boto3
from botocore.exceptions import ClientError

# Initial environement
"""
There are 6 functions support for setup environment store data:
    - get_config: Load config file to launch cluster
    
    - update_configfile: Update config file to launch cluster
    
    - update_credentials_aws: Update credential part for dwh.cfg file
    
    - open_tcp_port: Configure cluster that make redshift publicly accessible
    
    - connect_database: Connection to database
    
    - check_redshift_cluster_status: Check redshift is running or not
"""

def get_config():
    """
    Loads in the config object from the dwh.cfg file

    Returns
    config: config object from dwh.cfg file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config

def update_configfile(items_info: dict, section: str):
    """Update config file stored same folder

    Args:
        items_info (dict): dictionary contain attributes store 
        dwh.cfg file
        section (str): section in dwh.cfg file
    """
    config = get_config()
    # config.read('dwh.cfg')
    aws_config = config[section]
    
    for key, value in items_info.items():
        aws_config[key] = value

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

def update_credentials_aws(path_to_file: str = "new_user_credentials.csv"):
    """Function to update credentials for dwh.cfg file

    Args:
        path_to_file (str, optional): a direct path dwh.cfg file. 
        Defaults to "new_user_credentials.csv".
    """
    result = {'AWS_ACCESS_KEY_ID': 'Access key ID', 'AWS_SECRET_ACCESS_KEY': 'Secret access key'}
    with open(path_to_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        dict_item = list(map(dict, reader))
        result = dict((key, dict_item[0][result[key]]) for key in result.keys())
    update_configfile(result, 'AWS_ACCESS') 

def open_tcp_port(ec2, config, redshift):
    """Open an incomming TCP port to access
    to the cluster endpoint.
    Args:
        ec2 (EC2): an EC2 to to open TCP port
        redshift (Redshift Cluster): Redshift cluster information
    """
    cluster_status = redshift.describe_clusters(
        ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    )
    cluster_prop = cluster_status['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=cluster_prop['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(f"Deafult SG: {defaultSg}")
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.get('CLUSTER', 'DB_PORT')),
            ToPort=int(config.get('CLUSTER', 'DB_PORT'))
        )
    except Exception as e:
        print(e)
        print("Can not open connection TCP port!")
        return
    print("Open TCP connect successfully!")

def connect_database():
    """connect database

    connect database connects to the redshift database
    Returns:
    conn: database connection.

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST = config.get('CLUSTER', 'HOST')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    CONNECTION_STRING = "host={} dbname={} user={} password={} port={}".format(
        HOST,
        DB_NAME, 
        DB_USER, 
        DB_PASSWORD, 
        DB_PORT,
    )
    print('Connecting to RedShift', CONNECTION_STRING)
    conn = psycopg2.connect(CONNECTION_STRING)
    print('Connected to Redshift')
    return conn

def check_redshift_cluster_status(config, redshift):
    """Check redshift cluster status
    for a particular redshift cluster, finds the current status of the cluster
    returns false if it is creating, true if running, or None if not initiated
    
    Paramters:
    config: configuration object
    redshift: redshift boto3 client
    Returns:
    status: status of the created redshift cluster
    """
    try:
        cluster_status = redshift.describe_clusters(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
        )
    except Exception as e:
        print('could not get cluster status', e)
        return None
    return cluster_status['Clusters'][0]

# Start redshift
"""
There are 4 functions to create resource to store data
    - create_bucket: Create an S3 bucket in a specified region
    
    - create_ec2: Create ec2 client to open incomming TCP Port
    to access cluster endpoint.
    
    - create_iam: Create IAM creates the neccessary iam role 
    and policy to be able to read from S3
    
    - create_redshift_cluster: initiates the creation of a redshift cluster created based on the 
    cluster parameters given in the config file (dwh.cfg)
"""
# Create client for IAM, EC2, Redshift
def create_bucket(bucket_name, config):
    """Create an S3 bucket in a specified region
    
    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    
    Args:
        bucket_name (str): Name of the bucket
        config (configuration): Configure to get region 
    """
    region = config.get('AWS_ACCESS', 'AWS_REGION')
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True
            

def create_ec2(config):
    """Create ec2 client to open incomming TCP Port
    to access cluster endpoint.
    """ 
    ec2 = boto3.resource(
        'ec2',
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_ACCESS', 'AWS_REGION')
    )
    print("Create EC2 Successfully")
    return ec2


def create_iam(config):
    """
    Create IAM creates the neccessary iam role and policy to be able to read from S3

    Parameters:
    config: config object from the dwh.cfg file
    Returns:
    role: created role attached to the redshift cluster.
    """
    IAM = boto3.client(
        'iam',
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_ACCESS', 'AWS_REGION')
    )
    iam_role_name = config.get('IAM_ROLE', 'NAME')

    # role = None
    try:
        role = IAM.create_role(
            RoleName=iam_role_name,
            Description='Allows Redshift to Access Other AWS Services',
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        print('IAM Role Arn:', role['Role']['Arn'])
        update_configfile({'redshift_arn':role['Role']['Arn']}, 'IAM_ROLE') # Update role arn if not already exists
    except IAM.exceptions.EntityAlreadyExistsException:
        print('IAM role already exists.')
        role = IAM.get_role(RoleName=iam_role_name)
        print('IAM Role Arn:', role['Role']['Arn'])
        update_configfile({'redshift_arn':role['Role']['Arn']}, 'IAM_ROLE') # Update role arn if already exists
    try:
        IAM.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )
    except Exception as e:
        raise e
    print('Successfully Created Role, and Attached S3 Read-Only Policy.')
    return role

def create_redshift_cluster(config, ec2, iam_role):
    """create redshift cluster
    initiates the creation of a redshift cluster created based on the 
    cluster parameters given in the config file 
    and waits till the cluster is up and running.
    Parameters:
    config: config object
    iam_role: iam role created for the redshift cluster
    Returns:
    redshift_cluster: metadata regarding the redshift cluster
    """

    redshift = boto3.client(
        'redshift',
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_ACCESS', 'AWS_REGION'), 
    )

    print('Creating Redshift Cluster...')
    try:
        response = redshift.create_cluster(
            ClusterType=config.get('CLUSTER', 'DWH_CLUSTER_TYPE'),
            DBName=config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            Port=int(config.get('CLUSTER', 'DB_PORT')),
            IamRoles=[
                iam_role['Role']['Arn']
            ],
            NumberOfNodes=int(config.get('CLUSTER', 'NODE_COUNT'))
        )
        print('Create Cluster Call Made.')
    except Exception as e:
        print('Could not create cluster', e)
        
    cluster_initiated = time.time()
    status_checked = 0
    while True:
        print('Getting Cluster Status..')
        cluster_status = check_redshift_cluster_status(config, redshift)
        
        status_checked += 1
        if cluster_status['ClusterStatus'] == 'available':
            dwh_endpoint = cluster_status['Endpoint']['Address'] # Update dwh_endpoint
            update_configfile({"HOST":dwh_endpoint}, 'CLUSTER')
            break
        # print('Cluster Status', cluster_status)
        print('Status Checked', status_checked)
        print('Time Since Initiated', time.time() - cluster_initiated)
        time.sleep(5)
    # open_tcp_port(ec2, config, redshift)
    print('Cluster is created and available.')

# Create table
"""
There is 1 functions that reate table that store data in PostgreSQL
    - create_tables_from_file: run sql statement to create table
"""
def create_tables_from_file(conn, cur, path_to_file):
    """Create table from sql file.

    Args:
        conn (Connection): Connection to the database.
        cur (Cursor): Cursor to execute queries from sql file.
        path_to_file (str): Path to sql file that has been defined.
    """
    with open(path_to_file, 'r') as file:
        query = file.read()
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Could not create table", e)

# Stop redshift
"""
There is 2 functions that stop redshift cluster:
    - remove_iam: Remove iam remove the IAM roles and policies created
    - delete_redshift_cluster: Delete redshift cluster
"""
def remove_iam(config):
    """remove iam

    remove iam should remove the IAM roles and policies created

    Parameters:
    config: configuration object

    """

    IAM = boto3.client(
        'iam',
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_ACCESS', 'AWS_REGION')
    )
    iam_role_name = config.get('IAM_ROLE', 'NAME')
    iam_policy_arn = config.get('IAM_ROLE', 'ARN')

    try:
        IAM.detach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )
        print('detached role policy')
    except Exception as e:
        print('Could not remove role policy', e)

    try:
        IAM.delete_role(
            RoleName=iam_role_name
        )
        print('removed iam role.')
    except Exception as e:
        print('Could not remove IAM role', e)

def delete_redshift_cluster(config):
    """delete redshift cluster

    delete redshift clsuter should remove the existing redshift cluster

    Parameters:
    config: configuration object

    """
    redshift = boto3.client(
        'redshift',
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS_ACCESS', 'AWS_REGION'), 
    )
    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
        print('deleted redshift cluster.')
    except Exception as e:
        print('could not delete redshift cluster', e)

    cluster_delete_actioned = time.time()
    while True:
        cluster_status = check_redshift_cluster_status(config, redshift)
        if cluster_status is None:
            print('Cluster is deleted successfully!')
            break
        print('Cluster is', cluster_status['ClusterStatus'])
        time.sleep(5)
        print('Time since delete actioned', time.time() - cluster_delete_actioned)

def main(args):
    config = get_config()
    if args.launch:
        update_credentials_aws()
        iam_role = create_iam(config)
        ec2 = create_ec2(config)
        create_redshift_cluster(config, ec2, iam_role)
    
    if args.stop:
        remove_iam(config)
        delete_redshift_cluster(config)
    
    if args.create_table:
        conn = connect_database()
        cur = conn.cursor()
        path_to_file = 'create_tables.sql'
        print("CREATING TABLE...")
        create_tables_from_file(conn, cur, path_to_file)
        print("CREATING TABLE SUCCESSFULLY!")
        conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="An action working with cluster")
    parser.add_argument('--launch', dest='launch', default=False, action='store_true', help="Launch Redshift cluster.")
    parser.add_argument('--stop', dest='stop', default=False, action='store_true', help='Stop and delete Redshift clluster.')
    parser.add_argument('--create_table', dest='create_table',default=False, action='store_true', help='Create and load data into table.')
    args = parser.parse_args()
    main(args=args)