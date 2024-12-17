
# <p align="center"> Spotify Data Pipeline with Airflow </p>

This project showcases the development of an automated data pipeline for processing Spotify user activity data. The pipeline extracts, cleans, and transforms data using the Spotify API, Apache Airflow, Jupyter, PostgreSQL, and Power BI for visualization.


## Tech Stack used:  
Spotify API, Apache Airflow, Amazon S3, PostgreSQL, SQL, Pandas, Python, Jupyter Notebooks, Power BI <br></br>

## Project Workflow:

- Data Extraction: Extracts daily listening history for two months from the Spotify API.
- Data Storage: The raw data is stored in Amazon S3 for scalable and secure storage.
- Automation with Apache Airflow: The extraction and loading of data into S3 are automated using Apache Airflow, ensuring regular updates and smooth operation.
- Data Cleaning: Loads the data from S3 into Jupyter for cleaning, including data type conversions, removing duplicates, and other necessary transformations using pandas.
- Data Transformation: The cleaned data is then transferred to PostgreSQL, where it is transformed into a dimension model using SQL to enable efficient and structured analysis
- Data Visualization: The data is loaded from PostgreSQL into Power BI for analysis and visualization, providing insights into song listening behavior and user engagement trends. <br></br>


## Architecture Diagram

![Spotify Architecture ](https://github.com/user-attachments/assets/c8e565f3-1fa7-479b-917b-75fe91ce42c6) <br></br>


## Data Model 

![spotify_data_model](https://github.com/user-attachments/assets/795b0289-9af1-4a99-bdd5-68f6cbb28bf5) <br></br>

## Dimensional Model

![spotify_dimension_model](https://github.com/user-attachments/assets/18bc80e1-6202-412c-9c79-bfd1782841ba) <br></br>



## Dashboard

### - *First Page* 
![1-dashboard](https://github.com/user-attachments/assets/3fec7261-520d-4351-9823-a5dddd0f484c) <br></br>

### - *Second Page*
![2-dashboard](https://github.com/user-attachments/assets/176df2d7-e695-4d83-88ea-640788085349) <br></br>


## Steps to Execute Project

#### *Step 1: Setup AWS S3*

            1. Create an AWS account and set up an S3 bucket.
            2. Create two folders inside the bucket: tracks (for track details) and artists (for artist details).

#### *Step 2: Store Configuration in `config.ini`*

            1. Store AWS credentials, folder paths, and Spotify API credentials (Client ID, Secret, Redirect URI) in a config.ini file.

#### *Step 3: Installing Airflow*

            1. Enable WSL:
                - Search for "Turn Windows features on or off" and check "Windows Subsystem for Linux". Restart if required.
            2. Install WSL
                - Open Command Prompt as Administrator and run: `wsl --install`. 
            3. Follow the prompts to set up your Linux environment (username and password)
            4. update packages to ensure all packages are current:
                - Run: `sudo apt update` 
            5. Install Required Packages:
                - Run: `sudo apt install python3 python3-pip` to install Python and pip
            6. Create Virtual Environment:
                - Execute:
                    `python3 -m pip install virtualenv`
                    `mkdir airflowcoreproject`
                    `cd airflowcoreproject`
                    `python3 -m venv airflowvenv`
            7. Activate Environment
                - Run: `source airflowvenv/bin/activate` to activate the virtual environment each session.
            8. Install Apache Airflow
                - Use pip to install Airflow with a compatibility constraint:
                     `pip install "apache-airflow==2.6.2" --constraint "<constraint-url>"`
            9. Start Airflow Services
                - Initialize the database: `airflow db init`
                - Start the web server: `airflow webserver --port 8085`
                - Start the scheduler in a new terminal: `airflow scheduler`

You Tube Video for Airflow Installation: https://www.youtube.com/watch?v=rxbdg9DEgQw <br></br>
            
#### *Step 4: Extracting Data using Spotify API*

            1. Set Up Spotify Developer Account:
                - Create an account on Spotify for Developers.
                - Generate client credentials (Client ID and Client Secret).

            2. Understand API Endpoints:
                - Familiarize yourself with the Spotify API documentation.
                - Identify the endpoint for retrieving user playback history (e.g., Get Recently Played Tracks).

            3. Authentication:
                - Implement OAuth 2.0 for authorization using libraries like spotipy or direct API requests. Request access and refresh tokens to maintain a session.

            4. API Request Implementation:
                - Write a Python script to call the Spotify API. Fetch the last 50 recently played tracks data. Store metadata such as track name, artist, played time, and album.

            5. Schedule Regular Requests:
                - Use the refresh token to fetch new data at regular intervals. Automate this step later using Airflow.

📂 Related Code: All the above steps are implemented in `spotify_project/etl_scripts/spotify_etl.py` <br></br>

#### *Step 5: Scheduling File to Run Daily in Airflow*

            1. Create a Python script (an Airflow DAG) that contains the task logic and needs to run daily. (spotify_etl_dag.py)
            2. Place the script in the appropriate directory accessible by Airflow (airflow/dags/spotify_etl_dag.py)
            3. Define an Airflow DAG (Directed Acyclic Graph) in a `.py` file, specifying:
                - The schedule interval using `schedule_interval="@daily"`.
                - The default arguments such as `start_date` and `retries`.
            4. - Use the Airflow `PythonOperator` or other operators to execute the script.
            5. Test the DAG by running it manually in the Airflow web interface.
            6. Activate the DAG in the web interface to enable daily execution.

Collect data for any duration you need, such as 30 days, 15 days, or any custom period <br></br>

#### *Step 6: Extracting Data From S3 to Jupyter notebook For Data Cleaning*

            1. Extract Data from S3:
                - Use Python libraries like boto3 to download files from the S3 bucket.
            2. Clean the Data:
                - Handle missing values, duplicate records, or inconsistent formats.
                - Standardize columns such as timestamps.
            3. Save the cleaned data in New DataFrame

#### *Step 7: Extracting Cover URLs from Spotify API*
            1. Set Up Spotify Developer Account
            2. Install spotipy and pandas using pip
            3. Read the CSV file containing track URLs and loop through each track URL to fetch the associated cover URL
            4. After collecting the cover URLs, the cover_url column is added to the DataFrame, and the updated data is saved into a new CSV file, `tracks_with_covers.csv` 

📂 Related Code: All the above steps are implemented in `spotify_project/cover_urls/extract_cover_url.py` <br></br>


#### *Step 8: Loading into PostgreSQL*

            1. Set Up PostgreSQL Database:
                - Create a database for storing the Spotify data.
            2. Write to PostgreSQL:
                - Use Python libraries like psycopg2 or SQLAlchemy to load data.
                - Write the cleaned DataFrame to the respective tables.
            3. Transform Data:
                - Perform data transformations such as creating a dimension model:
                        - Fact Table: Playback history.
                        - Dimension Tables: Artists, albums, genres, etc. 

📂 Related Code: All the above steps are implemented in `spotify_project/spotify_dimension_modelling.sql` <br></br>

#### *Step 9: Analysis and Insights*
            1. Connect to the Database:
                - Connect Power BI to PostgreSQL to load the transformed data for analysis.
            2. Perform Data Analysis
                - Analyze key trends such as the most played tracks, top artists, and listening patterns.
                - Generate KPIs and create meaningful visualizations to highlight insights.
            3. Generate Reports:
                - Develop interactive dashboards or reports to present the findings and provide clear insights into user behavior.
