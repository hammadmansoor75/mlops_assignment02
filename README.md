**Python Script to Extract Data from dawn,bbc and than pushing it to google drive using dvc.**

**Data Extraction:**
I used web scrapping tools in python like requests and BeautifulSoup for extracting data. Data includes links, titles, and description.

**Data Transformation: **
In data transformation, all the text is converted into lower case and extra spaces are removed. Then the data is stored in different files for different sources.

**DVC Setup:**
Dvc is initalized and the data files are pushed to dvc for tracking.

**Airflow Dag:**
Dag script is written to automate the process of extraction, transformation, and file upload which runs the pipeline after one day.


**Challenges:**
Airflow Installation Challenges: Airflow is not installing on my PC so i cannot test the dag script. There are issues with sql path and environment 
variables.
