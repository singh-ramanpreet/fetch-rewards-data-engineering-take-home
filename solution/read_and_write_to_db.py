from datetime import datetime
from msticpy.data import data_obfus
import localstack_client.session as boto3
import psycopg2
from getpass import getpass


# Globals
QUEUE_NAME = "login-queue"
NUM_MSGS = 100
TABLE_NAME = "user_logins"


def get_queue():
    """
    Get Queue URL

    Returns
    -------
    str
        SQS Queue Url
    """
    sqs = boto3.client("sqs")
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    return queue_url


def receive_message(queue_url):
    """
    Retrive message from SQS Queue.

    Parameters
    ----------
    queue_url : str
        SQS Queue Url 
    Returns
    -------
    dict
        recieved message (JSON)
    """
    sqs = boto3.client("sqs")
    messages = sqs.receive_message(QueueUrl=queue_url)
    return messages


def parse_msg(msg):
    """
    Parse and flatten JSON message received from AWS SQS.

    Parameters
    ----------
    msg : dict
        message recieved from SQS (JSON)
    Returns
    -------
    dict
        parsed and flatten dict
    """
    msg_body = eval(msg["Messages"][0]["Body"].replace("null", "None"))
    date = msg["ResponseMetadata"]["HTTPHeaders"]["date"]

    # map app_version to integer
    app_version = list(map(int, msg_body["app_version"].split(".")))
    app_version.reverse()
    msg_body["app_version"] = sum(x * (100 ** i)
                                  for i, x in enumerate(app_version))

    # parse date
    msg_body["create_date"] = datetime.strptime(
        date, "%a, %d %b %Y %H:%M:%S %Z")

    # mask ip and device_id using msticpy package.
    # https://msticpy.readthedocs.io/en/latest/data_acquisition/DataMasking.html
    msg_body["masked_ip"] = data_obfus.hash_ip(msg_body["ip"])
    msg_body["masked_device_id"] = data_obfus.hash_item(
        msg_body["device_id"], delim="-")
    del msg_body["ip"]
    del msg_body["device_id"]
    return msg_body


def write_to_db(parsed_msg, cursor):
    """
    Wrtie parsed data to postgres database

    Parameters
    ----------
    parsed_msg : dict
        parsed data
    cursor: 
        cursor to postgres database connection
    Returns
    -------
    """
    columns = list(parsed_msg.keys())
    values = tuple(parsed_msg.values())
    n_columns = len(columns)
    cursor.execute(
        f"INSERT INTO {TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join('%s' for i in range(n_columns))}) ON CONFLICT DO NOTHING",
        values)
    return


def main():
    queue_url = get_queue()

    # connect to postgres
    db_passwd = getpass("Postgres DB Password: ")
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password=db_passwd, host="localhost", port="5432")
    cursor = conn.cursor()

    for i in range(NUM_MSGS):
        msg = receive_message(queue_url)
        parsed_msg = parse_msg(msg)
        write_to_db(parsed_msg, cursor)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
