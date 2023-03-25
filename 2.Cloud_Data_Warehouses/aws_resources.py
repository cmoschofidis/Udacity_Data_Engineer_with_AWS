import boto3
import json
import configparser
import os
from pathlib import Path
import time
import logging
from botocore.exceptions import ClientError
import sys
import argparse

## Load configs
path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "dwh.cfg")
config = configparser.ConfigParser()
config.read(config_path)


try:
    ## aws creds
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    ## dwh_cluster params
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    
    ## dwh db params
    DWH_DB = config.get("DB", "DWH_DB")
    DWH_DB_USER = config.get("DB", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DB", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DB", "DWH_PORT")
except:
    print('could not read configuration file')
    sys.exit(1)


def create_resources():
    """
    Create all AWS resources
    """
    try:
        ec2 = boto3.resource('ec2',
                        region_name="us-east-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

        s3 = boto3.resource('s3',
                        region_name="us-east-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

        redshift = boto3.client('redshift',
                        region_name="us-east-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        ) 

        iam = boto3.client('iam', 
                        region_name="us-east-1",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
        logging.debug("Created all required AWS resources")
    except:
        logging.info(" Couldn't create the required resources")
    return iam, redshift, ec2, s3
    
def create_iam_role(iam):
    """Function to create and attach IAM role.
    Keyword arguments:
    iam -- iam resource
    """
    try:
        logging.debug('Creating a new IAM Role')
        role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allows Redshift clusters to call AWS services',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )

    except ClientError as e:
        logging.exception(e)

    # Define policy to access S3 bucket (ReadOnly)
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    # Get the IAM role ARN
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.debug('IAM: {}, ARN: {} created'.format(DWH_IAM_ROLE_NAME, role_arn))
    config.set('IAM_ROLE', 'ARN', str(role_arn))
    with open('dwh.cfg', 'w') as f:
        config.write(f)
        
    return role_arn


def create_redshift_cluster(redshift, role_arn):
    try:
        redshift.create_cluster(        
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            # Roles (for s3 access)
            IamRoles=[role_arn]  
        )
        logging.debug('Creating RS Cluster')
    except ClientError as e:
        logging.exception(e)

def create_tcp_port(ec2, vpc_id):
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        logging.debug('Opening TCP connection.')
    except ClientError as e:
        print(e)

def delete_rds_cluster(redshift):
    """Function to delete Redshift Cluster.
    Keyword arguments:
    redshift -- redshift resource
    """

    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
        logging.debug('Deleting Redshift Cluster {}.'.format(DWH_CLUSTER_IDENTIFIER))

    except ClientError as e:
        logging.exception(e)


def delete_iam_role(iam):
    """Function to detach and delete IAM role.
    Keyword arguments:
    iam -- iam resource
    """

    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        logging.debug('Deleting IAM role {}.'.format(DWH_IAM_ROLE_NAME))

    except ClientError as e:
        logging.exception(e)


def main(argument):
    iam, redshift, ec2, s3= create_resources()
    if argument.delete:
        delete_rds_cluster(redshift)
        delete_iam_role(iam)
        logging.info("All resources are deleted. Your pocket is safe!!")
    else:
        role_arn = create_iam_role(iam)
        create_redshift_cluster(redshift, role_arn)

        while True:
            cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if cluster_props['ClusterStatus'] == 'available':
                logging.info('Redshift Cluster is available and created at {}'.format(cluster_props['Endpoint']))
                config.set('DB', 'HOST', str(cluster_props['Endpoint']['Address']))
                with open('dwh.cfg', 'w') as f:
                    config.write(f)
                create_tcp_port(ec2, cluster_props['VpcId'])
                break
            logging.info('Cluster status: {} - Waiting...'.format(cluster_props['ClusterStatus']))
            time.sleep(10)
        logging.info("The DWH is ready to work with")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', action='store_true', default=False)
    args = parser.parse_args()
    main(args)
