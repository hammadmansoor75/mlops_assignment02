import requests
from bs4 import BeautifulSoup
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_extraction_transformation_upload',
    default_args=default_args,
    description='A simple ETL DAG for extracting, transforming, and uploading data',
    schedule_interval='@daily',
    start_date=days_ago(0),
    catchup=False,
)



def extract_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract links
    links = [link.get('href') for link in soup.find_all('a')]
    
    # Extract titles
    titles = [title.text for title in soup.find_all('h2')]
    
    # Extract descriptions
    descriptions = [description.text.strip() for description in soup.find_all('p')]
    
    return links, titles, descriptions

def preprocess_data(text):
    # Convert text to lowercase
    text = text.lower()
    
    # Remove numbers
    text = re.sub(r'\d+', '', text)
    
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    
    # Tokenize the text
    tokens = word_tokenize(text)
    
    # Remove stop words
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]
    
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_text = [lemmatizer.lemmatize(word) for word in filtered_tokens]
    
    # Join the lemmatized tokens back into a single string
    preprocessed_text = ' '.join(lemmatized_text)
    
    return preprocessed_text

# Function to integrate data extraction and transformation
def extract_and_transform(url, file_name):
    links, titles, descriptions = extract_data(url)
    
    # Preprocess descriptions
    preprocessed_descriptions = [preprocess_data(description) for description in descriptions]
    with open(file_name, 'w') as file:
        for description in preprocessed_descriptions:
            file.write(description + '\n')
    return links, titles, preprocessed_descriptions

# URLs of the websites
dawn_url = 'https://www.dawn.com/'
bbc_url = 'https://www.bbc.com/'

# Extracting and transforming data from dawn.com
extract_and_transform(dawn_url, "dawn_data.txt")
 # Extracting and transforming data from bbc.com
extract_and_transform(bbc_url, "bbc_data.txt")




def extract_transform_dawn():
    # Extracting and transforming data from dawn.com
    extract_and_transform(dawn_url, "dawn_data.txt")



def extract_transform_bbc():
    # Extracting and transforming data from bbc.com
    extract_and_transform(bbc_url, "bbc_data.txt")


def upload_dawn():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication.
    drive = GoogleDrive(gauth)
    file_path2 = "dawn_data.txt"
    folder_id = "1F3JbxtgLc0yTsye--78NdCl3cKgR8meE"  # You can find the folder ID in the URL of the folder.
    file2 = drive.CreateFile({'parents': [{'id': folder_id}]})
    file2.SetContentFile(file_path2)
    file2.Upload()
    print(" DAWNFile uploaded successfully!")


def upload_bbc():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication.
    drive = GoogleDrive(gauth)
    file_path = "bbc_data.txt"
    folder_id = "1F3JbxtgLc0yTsye--78NdCl3cKgR8meE"  # You can find the folder ID in the URL of the folder.
    file = drive.CreateFile({'parents': [{'id': folder_id}]})
    file.SetContentFile(file_path)
    file.Upload()
    print(" BBCFile uploaded successfully!")

def upload_google_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication.
    drive = GoogleDrive(gauth)

    # Choose the file to upload and the destination folder in Google Drive.
    file_path = "bbc_data.txt"
    file_path2 = "dawn_data.txt"
    folder_id = "1F3JbxtgLc0yTsye--78NdCl3cKgR8meE"  # You can find the folder ID in the URL of the folder.

    # Upload the file.
    file = drive.CreateFile({'parents': [{'id': folder_id}]})
    file.SetContentFile(file_path)
    file.Upload()
    print(" BBCFile uploaded successfully!")

    file2 = drive.CreateFile({'parents': [{'id': folder_id}]})
    file2.SetContentFile(file_path2)
    file2.Upload()
    print(" DAWNFile uploaded successfully!")


upload_google_drive()

with dag:
    extract_transform_dawn_task = PythonOperator(
        task_id='extract_transform_dawn',
        python_callable=extract_transform_dawn,
    )

    extract_transform_bbc_task = PythonOperator(
        task_id='extract_transform_bbc',
        python_callable=extract_transform_bbc,
    )

    upload_dawn_task = PythonOperator(
        task_id='upload_dawn',
        python_callable=upload_dawn,
    )

    upload_bbc_task = PythonOperator(
        task_id='upload_bbc',
        python_callable=upload_bbc,
    )

    extract_transform_dawn_task >> upload_dawn_task
    extract_transform_bbc_task >> upload_bbc_task


