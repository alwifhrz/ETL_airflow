# ETL_airflow

In this project, the ETL process is carried out using airflow, from data source https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page. The data will be extracted by airflow, transformed in spark (stand alone), and loaded with the output displayed in the log in airflow.

This project can solve the problem of finding
1. How many taxi trips were there on February 15
output:

![WhatsApp Image 2023-07-16 at 23 16 23](https://github.com/alwifhrz/ETL_airflow/assets/105298857/fd73dda3-6472-4d2b-8266-a5c55962d853)

2. The longest trip for each day
output:

![WhatsApp Image 2023-07-16 at 23 16 51](https://github.com/alwifhrz/ETL_airflow/assets/105298857/c480259c-37cc-433f-8445-2472910ad18a)

3. Top 5 Most frequent `dispatching_base_num`
output:

![WhatsApp Image 2023-07-16 at 23 17 05](https://github.com/alwifhrz/ETL_airflow/assets/105298857/822e8760-0ec2-4dfa-9b56-9a196dad6272)

4. Top 5 Most common location pairs (PUlocationID and DOlocationID)
output:

![WhatsApp Image 2023-07-16 at 23 17 27](https://github.com/alwifhrz/ETL_airflow/assets/105298857/6a01391f-af42-4a9f-8e9c-fe373a1d4ee5)

