from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import datetime
import csv
import requests
import pandas as pd

emails = list(pd.read_json("/opt/airflow/dags/config/emails.json")['emails'])
# if there is configuration json in dag_run.conf
if 'email' in '{{dag_run.conf}}':
    emails = set(emails)
    for email in '{{dag_run.conf["email"]}}':
        emails.add(email)
    emails = list(emails)

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2)
}


def checkUpdate():
    api_key = '5jZ1DJCY75xIVzGmrEBVDvmNmL74VCqo'
    book_lists = ['combined-print-and-e-book-fiction', 'combined-print-and-e-book-nonfiction']
    latest_modify = pd.read_csv('/opt/airflow/dags/files/latest_modify.csv')

    for book_list in book_lists:
        r = requests.get(f'https://api.nytimes.com/svc/books/v3/lists/{{ds}}/{book_list}.json?api-key={api_key}')
        data = r.json()
        if data['last_modified'] != latest_modify[book_list][0]:
            latest_modify[book_list][0] = data['last_modified']
            latest_modify.to_csv('/opt/airflow/dags/files/latest_modify.csv',index=False)

            sent = pd.DataFrame({'email': []})
            sent.to_csv(f'/opt/airflow/dags/files/{book_list}_sent.csv',index=False)

            wait = pd.DataFrame({'email': emails})
            wait.to_csv(f'/opt/airflow/dags/files/{book_list}_wait.csv',index=False)

            title = []
            author = []
            publisher = []
            description = []
            amazon_product_url = []

            for book in data['results']['books']:
                title.append(book['title'])
                author.append(book['author'])
                publisher.append(book['publisher'])
                description.append(book['description'])
                amazon_product_url.append(book['amazon_product_url'])

            book_dict = {
                'title': title,
                'author': author,
                'publisher': publisher,
                'description': description,
                'amazon_product_url': amazon_product_url
            }

            book_df = pd.DataFrame(book_dict, columns=['title', 'author', 'publisher', 'price', 'description','amazon_product_url'])
            book_df.to_csv(f'/opt/airflow/dags/files/{book_list}.csv', index=False)

        else:

            sent = pd.read_csv(f'/opt/airflow/dags/files/{book_list}_sent.csv')
            hashset = set(list(sent['email']))
            new_add = []
            for email in emails:
                if email not in hashset:
                    new_add.append(email)

            wait = pd.DataFrame({'email': new_add})
            wait.to_csv(f'/opt/airflow/dags/files//{book_list}_wait.csv',index=False)

def wait2sent():    
    book_lists = ['combined-print-and-e-book-fiction', 'combined-print-and-e-book-nonfiction']
    
    for book_list in book_lists:
        wait = pd.read_csv(f'/opt/airflow/dags/files/{book_list}_wait.csv')
        sent = pd.read_csv(f'/opt/airflow/dags/files/{book_list}_sent.csv')

        sent = pd.concat([sent, wait])
        sent.to_csv(f'/opt/airflow/dags/files/{book_list}_sent.csv',index=False)
        
        wait = pd.DataFrame({'email':[]})
        wait.to_csv(f'/opt/airflow/dags/files/{book_list}_wait.csv',index=False)


with DAG(
        dag_id = "book_recommendation", 
        start_date=datetime.datetime(2024, 1, 7),
        schedule_interval="@daily", 
        default_args=default_args, 
        catchup=False
        ) as dag:
    
    check_update = PythonOperator(
        task_id = 'check_update',
        python_callable = checkUpdate
    )


    send_emails_1 = EmailOperator(
        task_id='send_emails_1',
        to=list(pd.read_csv('/opt/airflow/dags/files/combined-print-and-e-book-fiction_wait.csv')['email']),
        subject="This week's list of combined-print-and-e-book-fiction",
        files=['/opt/airflow/dags/files/combined-print-and-e-book-fiction.csv'],
        html_content="<h3>This week's list of combined-print-and-e-book-fiction</h3>"
    )
    
    send_emails_2 = EmailOperator(
        task_id='send_emails_2',
        to=list(pd.read_csv('/opt/airflow/dags/files/combined-print-and-e-book-nonfiction_wait.csv')['email']),
        subject="This week's list of combined-print-and-e-book-nonfiction",
        files=['/opt/airflow/dags/files/combined-print-and-e-book-nonfiction.csv'],
        html_content="<h3>This week's list of combined-print-and-e-book-nonfiction</h3>"
    )

    status_wait2sent = PythonOperator(
        task_id = 'status_wait2sent',
        python_callable = wait2sent
    )
    
    check_update >> [send_emails_1,send_emails_2] >> status_wait2sent


