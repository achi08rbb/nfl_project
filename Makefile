SHELL:=/bin/bash
-include .env
# Outside make:
# sudo apt install make

# This does not makee it available to the terminal session, only in this makefile
export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/google_credentials.json

# Clone repo and move to the repo directory to use the make file
# git clone https://github.com/rbblmr/nfl_project.git
# sudo apt install make

prerequisites: 
	@sudo apt update && sudo apt -y upgrade
	@sudo apt install docker.io python3.9 python3-pip -y
	@sudo pip install pipenv
	@-sudo groupadd docker
	@-sudo gpasswd -a ${USER} docker
	@echo "export $(cat ~/nfl_project/.env|xargs)"
# Do this to make the .env variables available in the session	
# exit
# ssh ${GCP_VM}.${GCP_ZONE}.${GCP_PROJECT_ID}
# @sudo service docker restart # Try not doing this

# Adding - before the command ignores any warning
	@-mkdir ~/bin
	@cd ~/bin; wget https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -O docker-compose
	@chmod +x ~/bin/docker-compose
	@touch ~/.bashrc
	@echo 'export PATH="${HOME}/bin:${PATH}"' >> ~/.bashrc
# echo -e makes the /t /n /r possible escapes

	@source ~/.bashrc
	@make terraform-setup

terraform-setup:
	curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
	sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com focal main"
	sudo apt-get update && sudo apt-get install terraform

gcloud-initialize:
	@gcloud init
	@mkdir -p ~/.google/credentials/
####################################### FIX TO ENABLE API ##############################	
# Enable API services
	@gcloud services enable iam.googleapis.com
	@gcloud services enable iamcredentials.googleapis.com
	@gcloud services enable dataproc.googleapis.com
#@gcloud services enable compute.googleapis.com

# Create Service Account
	@gcloud iam service-accounts create ${GCP_SERVICE_ACCOUNT_USER} --description="Service account for NFL project" --display-name="${GCP_SERVICE_ACCOUNT_USER}"

# Add roles
	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/viewer"

	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"
	
	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
	
	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectAdmin"

	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectCreator"

	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectViewer"

	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/dataproc.worker"

	@gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} --member="serviceAccount:${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/dataproc.serviceAgent"

# Download the credentials
	@cd ${GCP_CREDENTIALS_DIR} && gcloud iam service-accounts keys create ${GCP_CREDENTIALS_NAME} --iam-account=${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com

# Authenticate the credentials
	@gcloud auth application-default login


terraform-infra: 
	cd ~/nfl_project/terraform; terraform init; terraform plan -var="project=${GCP_PROJECT_ID}"; terraform apply -var="project=${GCP_PROJECT_ID}"

airflow-setup:
<<<<<<< HEAD
	cd ~/nfl_project/airflow; mkdir -p ./dags ./logs ./plugins; echo -e "AIRFLOW_UID=$$(id -u)" > ./airflow/.env"; docker-compose build; docker-compose up -d
=======
	cd ~/nfl_project/airflow; mkdir -p ./dags ./logs ./plugins; echo -e "AIRFLOW_UID=$$(id -u)" > ./airflow/.env; docker-compose build; docker-compose up -d
>>>>>>> refs/remotes/origin/main

airflow-gcloud-init:
# Google credentials variable must be available in the parent session
	docker exec -it $$(docker ps --filter "name=airflow-worker" --format "{{.ID}}") gcloud init
	docker exec -it $$(docker ps --filter "name=airflow-worker" --format "{{.ID}}") gcloud auth application-default login
# gcloud init
# gcloud auth application-default login
# port forward and go to localhost:8080 in browser, airflow:airflow
clean:
	cd ~/nfl_project/airflow; docker-compose down --volumes --rmi all
	cd ~/nfl_project/terraform; terraform destroy
vm-down:
	@gcloud compute instances delete ${GCP_VM} --zone ${GCP_ZONE}
