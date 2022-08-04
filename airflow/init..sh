# Docker
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker ${USER}
source ~/.bashrc

# Airflow Environment
cd ~/aws-anp-fuel-sales-etl/airflow
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
docker-compose up --scale worker=3 -d
