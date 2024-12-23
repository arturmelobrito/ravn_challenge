# Data Engineering Pipeline Project

## Objective

The goal of this project is to build a scalable and modular data pipeline that acquires, cleans, and inserts data into a Snowflake data warehouse. The pipeline will be orchestrated using Apache Airflow and will be containerized using Docker for easy portability and deployment across different environments.

## Technologies Used

### 1. Apache Airflow

Apache Airflow is used as the orchestrator for the entire data pipeline. It allows us to automate the flow of data through different stages of acquisition, cleaning, and insertion. With Airflow, we can manage task dependencies, retries, and logs, ensuring a reliable and maintainable pipeline.

- **Scalability**: It supports complex workflows and is highly scalable, which is essential as the project grows.
- **Flexibility**: Airflow provides flexibility in defining DAGs (Directed Acyclic Graphs) that represent the workflow of the data pipeline, making it easier to add new tasks and modify existing ones.

### 2. Snowflake

Snowflake is used as the data warehouse for storing and querying the processed data. Its cloud-native architecture allows for high performance, scalability, and ease of management. The use of Snowflake enables efficient data storage and querying for large datasets.

- **Cloud-native**: Snowflake is built for the cloud, offering flexibility and scalability without the need for hardware management.
- **Separation of Compute and Storage**: Snowflake's architecture allows for independent scaling of compute and storage, making it cost-effective.
- **Integrated Data Sharing**: Snowflake offers secure and easy ways to share data across different teams or organizations.

### 3. Docker

Docker is used to containerize the entire project, ensuring that the environment is consistent across different stages of development, testing, and production. By using Docker, we can guarantee that the pipeline runs the same way regardless of where it's executed.

- **Portability**: Docker ensures that the application can run consistently across different environments, whether on a developer’s local machine, in staging, or in production.
- **Isolation**: Docker containers provide a clean and isolated environment, making it easier to manage dependencies and avoid conflicts.
- **Scalability**: Docker makes it easier to scale components (like Airflow workers) in the cloud or on-premise.

### Requirements

To run this project, you need the following:

1. **Snowflake Account**: You must have a Snowflake account for storing and querying data.
2. **Docker**: Docker and Docker Compose must be installed on your machine. This allows you to run the entire project locally in containers.
   - Docker is supported on **Windows**, **Linux**, and **MacOS**, making it easy to run the project regardless of your operating system.

## Project Setup

### 1. Clone the Repository

Start by cloning the repository to your local machine:

```bash
git clone https://github.com/arturmelobrito/ravn_challenge.git
cd ravn_challenge
```

### 2. Configure Environment Variables

Create a `.env` file in the root directory of the project. This file will contain your Snowflake connection details and other environment variables required by Airflow.

```plaintext
AIRFLOW_UID=50000
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_snowflake_account_url
SNOWFLAKE_ROLE=your_snowflake_role
SNOWFLAKE_DATABASE=your_snowflake_database
```

### 3. Install Docker and Docker Compose

Make sure Docker and Docker Compose are installed on your machine. You can follow the official documentation for installation guides:

- [Install Docker](https://docs.docker.com/)
- [Install Docker Compose](https://docs.docker.com/compose/install/)

### 4. Create airflow directories

Run those 2 cli commands:

```
mkdir -p ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
``` 
### 5. Build and Run the Containers

Once Docker and Docker Compose are installed, you can build and run the airflow-init container to ensure airflow initializes correctly:

```
docker compose up airflow-init
```

If building was successfull, then run:

```
docker compose up
```

### 6. Access Airflow Web UI
Once the containers are up and running, you can access the Airflow web UI to monitor the pipeline. Open your browser and navigate to:

```
http://localhost:8080
```

The default login credentials are:

- Username: `airflow`
- Password: `airflow`

### 7. Trigger the DAGs

You will first trigger the `create_snowflake_database_and_schema` DAG to prepare the database. Then you just need to "unpause" the other DAGs, each for a different acquisition pipeline, that it will trigger automatically and after that, daily.