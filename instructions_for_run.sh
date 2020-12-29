# Pull the docker image ref https://github.com/puckel/docker-airflow
docker pull puckel/docker-airflow

# Build
docker build --rm -t puckel/docker-airflow .


# Clone this airflow repo
git clone https://github.com/soyelherein/airflow-dag-past-run-sense.git

# Start the airflow server and point the dag folder in your machine. Example-
docker run -d -p 8080:8080 -v /Users/soyelherein/airflow-dag-past-run-sense/dag:/usr/local/airflow/dags  puckel/docker-airflow webserver

# go to the airflow UI http://localhost:8080/admin

