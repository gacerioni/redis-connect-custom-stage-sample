import requests
import time
import oracledb
import sys
import os
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger()

# Load environment variables with defaults if not provided
REDIS_CONNECT_HOST = os.getenv('REDIS_CONNECT_HOST', '34.170.196.119')
REDIS_CONNECT_PORT = os.getenv('REDIS_CONNECT_PORT', '8282')
JOB_NAME = os.getenv('JOB_NAME', 'oracle-job')
JOB_CONFIG_FILE = os.getenv('JOB_CONFIG_FILE', 'redis_connect_oracle_CHINOOK.json')

ORACLE_HOST = os.getenv('ORACLE_HOST', '34.170.196.119')
ORACLE_PORT = os.getenv('ORACLE_PORT', '1521')
ORACLE_SERVICE_NAME = os.getenv('ORACLE_SERVICE_NAME', 'ORCLPDB1')
ORACLE_USER = os.getenv('ORACLE_USER', 'C##dbzuser')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'blabla')

# Base URL for Redis Connect
REDIS_CONNECT_BASE_URL = f'http://{REDIS_CONNECT_HOST}:{REDIS_CONNECT_PORT}/connect/api/v1'

def load_job_config(job_name, job_config_file):
    url = f'{REDIS_CONNECT_BASE_URL}/job/config/{job_name}'
    files = {'file': (job_config_file, open(job_config_file, 'rb'), 'application/json')}
    logger.info(f'Loading job configuration for job {job_name}...')
    try:
        response = requests.post(url, files=files)
        response.raise_for_status()
        logger.info(f'Job configuration {job_name} loaded successfully.')
    except Exception as e:
        logger.error(f'Error loading job configuration: {e}')
        sys.exit(1)

def start_job_load_mode(job_name):
    url = f'{REDIS_CONNECT_BASE_URL}/job/transition/start/{job_name}/load'
    logger.info(f'Starting job {job_name} in LOAD mode...')
    try:
        response = requests.post(url)
        response.raise_for_status()
        logger.info(f'Job {job_name} started in LOAD mode.')
    except Exception as e:
        logger.error(f'Error starting job in LOAD mode: {e}')
        sys.exit(1)

def wait_for_load_completion(job_name):
    url = f'{REDIS_CONNECT_BASE_URL}/cluster/jobs/claim/all'
    logger.info(f'Waiting for job {job_name} LOAD completion...')
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            jobs = response.json()
            job_status = None
            for job in jobs:
                if job['jobName'] == job_name:
                    job_status = job['jobStatus']
                    break
            if job_status == 'STOPPED':
                logger.info(f'Job {job_name} LOAD completed successfully.')
                break
            elif job_status == 'FAILED':
                logger.error(f'Job {job_name} LOAD failed.')
                sys.exit(1)
            else:
                logger.debug(f'Job {job_name} status: {job_status}. Waiting for completion...')
                time.sleep(5)
        except Exception as e:
            logger.error(f'Error getting job status: {e}')
            time.sleep(5)

def get_oracle_scn():
    logger.info('Connecting to Oracle to get current SCN...')
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
        )
        cursor = connection.cursor()
        cursor.execute('SELECT current_scn FROM v$database')
        scn = cursor.fetchone()[0]
        cursor.close()
        connection.close()
        logger.info(f'Current Oracle SCN: {scn}')
        return str(scn)
    except Exception as e:
        logger.error(f'Error connecting to Oracle or fetching SCN: {e}')
        sys.exit(1)

def set_checkpoint(job_id, scn, commit_scn):
    url = f'{REDIS_CONNECT_BASE_URL}/job/checkpoint/{job_id}'
    data = {
        'scn': scn,
        'commit_scn': commit_scn
    }
    logger.info(f'Setting checkpoint for job {job_id} with SCN {scn} and COMMIT_SCN {commit_scn}...')
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        logger.info(f'Checkpoint set for job {job_id} with SCN {scn} and COMMIT_SCN {commit_scn}.')
    except Exception as e:
        logger.error(f'Error setting checkpoint: {e}')
        sys.exit(1)

def get_job_checkpoint(job_id):
    url = f'{REDIS_CONNECT_BASE_URL}/job/checkpoint/{job_id}'
    logger.info(f'Getting checkpoint for job {job_id}...')
    try:
        response = requests.get(url)
        response.raise_for_status()
        checkpoint = response.json()
        logger.info(f'Current checkpoint for job {job_id}: {checkpoint}')
    except Exception as e:
        logger.error(f'Error getting checkpoint: {e}')

def start_job_stream_mode(job_name):
    url = f'{REDIS_CONNECT_BASE_URL}/job/transition/start/{job_name}/stream'
    logger.info(f'Starting job {job_name} in STREAM mode...')
    try:
        response = requests.post(url)
        response.raise_for_status()
        logger.info(f'Job {job_name} started in STREAM mode.')
    except Exception as e:
        logger.error(f'Error starting job in STREAM mode: {e}')
        sys.exit(1)

def wait_for_claim(job_name):
    url = f'{REDIS_CONNECT_BASE_URL}/cluster/jobs/claim/all'
    logger.info(f'Waiting for job {job_name} to be claimed...')
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()
            jobs = response.json()
            for job in jobs:
                if job['jobName'] == job_name and job['jobStatus'] == 'CLAIMED':
                    logger.info(f'Job {job_name} successfully claimed by {job["jobOwner"]}.')
                    return
            logger.debug(f'Job {job_name} not yet claimed. Checking again...')
            time.sleep(5)
        except Exception as e:
            logger.error(f'Error checking job claim status: {e}')
            time.sleep(5)

def main():
    job_name = JOB_NAME
    job_config_file = JOB_CONFIG_FILE
    job_id = f'{{connect}}:job:{job_name}'

    # Step 1: Load the job configuration
    load_job_config(job_name, job_config_file)

    # Step 2: Get the current SCN from Oracle
    scn = get_oracle_scn()
    logger.info(f'Current SCN from Oracle is {scn}')

    # Step 3: Start the job in LOAD mode
    start_job_load_mode(job_name)

    # Step 4: Wait for LOAD completion
    wait_for_load_completion(job_name)

    # Step 5: Set the checkpoint with the SCN and COMMIT_SCN
    set_checkpoint(job_id, scn, scn)  # Set commit_scn to match scn for simplicity

    # Optionally, get the job checkpoint to confirm
    get_job_checkpoint(job_id)

    # Step 6: Start the job in STREAM mode
    start_job_stream_mode(job_name)

    # Step 7: Wait for job to be claimed
    wait_for_claim(job_name)

if __name__ == '__main__':
    main()
