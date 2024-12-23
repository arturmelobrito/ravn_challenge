# Data Sources and Challenges

## 1. Luas Passenger Numbers

The first data source was relatively simple. The endpoint directly returned a CSV file, and the main challenge was to standardize the dataset. Specifically, I had to remove the rows containing the aggregated month values as well as the rows where the two operational lines were aggregated. After this, I ensured that the data was consistent with a monthly granularity, making it easier to process and analyze.

## 2. Dublin Bus Passenger Numbers

For the second source, the endpoint also returned a CSV file directly, which made the data acquisition process easier. However, there was still a challenge with aggregated rows, specifically the row for the total values of all months in the year. I had to remove this aggregated row to maintain a consistent level of granularity for the data.

## 3. Weather Data - Met Éireann

The third data source presented more complexity. The endpoint returned a ZIP file, which required additional handling to extract and read the CSV file inside it. Moreover, this data source had issues with duplicated values. These duplicates were addressed during the data cleaning process to ensure the dataset's integrity.

## 4. Dublin Bikes

The fourth data source was more complicated. The endpoint was not immediately clear, so I had to explore the API catalog at "https://data.smartdublin.ie/dublinbikes-api/bikes/openapi.json" to understand which endpoint to use and what parameters were necessary. Initially, I thought something was wrong with my tests, but I soon realized that the endpoint returned data for only one day at a time. As a result, I had to create a logic that fetched the data day by day and inserted it into Snowflake in 15-day batches.

## 5. Cycle Counts

The fifth data source provided the CSV directly via the endpoint, so it was straightforward. The main task here was to standardize the column names and remove unnecessary information. Additionally, I had planned to pivot the table so that each row in the original table would be split into two rows—one for each direction at a particular location. The final table would have columns for time, address, direction, and number_of_bikes. However, due to time constraints, I was unable to implement this pivot operation.

# Reflections and Improvements

In general, I believe I did a good job building a proof of concept (PoC) for how I would begin to develop these data pipelines. However, there are several improvements that can be made, starting with the consideration of what data is static and what is not, and what really needs to be acquired on a daily basis versus what can be static or aggregated over a longer period of time.

## 1. Observability and Robustness

While Airflow already provides some level of observability, there are improvements that can be made to increase the robustness of the pipelines:

- Implementing **dead-letter queues** to handle failed records or tasks.
- Adding **error messaging** for alerts across different channels (e.g., email, Slack, etc.).
- Implementing a **data quality check** at the end of each pipeline to ensure the final output is correct and complete.
- Creating an **intermediate acquisition pipeline** that validates and inserts clean data from all the acquisition pipelines, ensuring consistency.

These steps are some of what differentiate a PoC from a production-ready pipeline.

## 2. Security

In terms of security, this project used root-level permissions and hardcoded credentials for simplicity, but this is not ideal for production. Normally, credentials should be managed securely, such as through services like **AWS Secrets Manager** or another secure storage for credentials. Additionally, sensitive data should never be exposed in the code, and permissions should be more granular to follow best practices.

## 3. Biggest Challenges: Environment Setup

One of my biggest challenges during this project was setting up the environment. I had never worked with Airflow locally before, so I had to learn how to configure the Airflow environment in Docker, passing all the necessary dependencies and credentials. In fact, half of my time on the project was spent on this configuration.


## Structuring New Pipelines

Each pipeline follows a well-defined structure: data acquisition, data cleaning, consistency checks, and insertion into the database. The first two steps are highly specific to each data source, type of data, frequency and volume. As such, it will always be necessary to spend extra time studying these stages to ensure they are properly handled for new sources. 

On the other hand, the consistency checks and insertion into the database are well standardized, which makes it easier to follow the structure already developed in the pipelines of the project. 

As previously mentioned, I planned to delegate the responsibility of consistency checks and database insertion to an intermediate DAG, which all acquisition pipelines would pass through. However, due to time constraints, I was unable to implement this. 

Finally, I believe that the project requirements were met with satisfactory quality and execution, demonstrating my expertise in building, orchestrating, and monitoring pipelines, as well as some infrastructure management.
