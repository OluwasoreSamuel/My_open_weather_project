# OPEN WEATHER PROJECT
 Hello, This repository contains my dag code for my airflow, my lambda function codes and my Redshift Table Query. I'm also adding the steps taken for this project.

 The objective for this project is to use airflow to automate the ETL process using open weather api and transferring the data to aws s3 bucket.Also, using lambda to copy the collected data to a new bucket to prevent the main data from external damages with another lambda function to help reduce the unncessary data collected which is to be transferred to a data warehouse(Redshift). Power bi would be used for visualization.


## Tools used for this project
1. Ec2 Instance.
2. s3 buckets.
3. Aiflow.
4. Lambda.
5. Redshift. 

## Steps taken with each tools
* OPEN WEATHER
    - I created an account with [Open weather](https://openweathermap.org).
    - I also created an api key.
    - Under the api tab, i used <https://openweathermap.org/current#name>  api requests with both city name and country code.
* EC2 INSTANCE
    - I created an instance which uses ubuntu. Under the IAMRole, s3fullaccess and redshiftfullaccess permissions are added.

    - Using ssh config settings, i moved to  VS Code.
    - I added the necessary updates with linus command, Created a virtual Environment and added other neccesary installations.

* S3 BUCKET

    3 Buckets where created for this project. they are listed below:

        1. oluwasore-open-weather-project: This bucket recieves the csv file directly from Open weather using my ETL function and Airflow module called PythonOperator to automate the process.

        2. tranferred-oluwasore-open-weather-project: This bucket contains the copied data from the previous bucket. i did this as a measure to prevent any damages from the main bucket, so as the keep the original data authentic.

        3. cleaned-oluwasore-open-weather-project: Here, i removed some columns which are not needed in the data warahouse.I collected the data from the previous bucket,reduced the columns and put it into this buscket.

* AIRFLOW
    Some modules where used in the Dag for the tasks in this project.

        - PythenOperator: This calls the function. Using the arguement python_callable to call the function created for the task.
        - BashOperator: bash_command Moves the the data file to aws s3 bucket.
        - S3KeySensor: this helps confirm and check if the file is in the designated bucket.
        - S3toRedshiftOperator: This helps to transfer data from the bucket to the designated Schema and table created in Redshift. 
* LAMBDA 
    2 functions where created to perform some specific tasks.

        1. TransferFile-Lambda.py: This helps transfer the file from the original bucket to the bucket created for it.

        2. ConvertFile-Lambda.py: This helps remove some unwanted columns from the Tranfer-bucket and moves it to a new bucket made for the data warehouse.
* REDSHIFT

    Under my cluster,a table was created containing the columns from my cleaned-bucket. Each columns has it's datatype.  The data would be transferred into the table with a task automated in airflow.