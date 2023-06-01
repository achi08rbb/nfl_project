SHELL:=/bin/bash
-include .env
# Outside prerequisites:
# sudo apt install make

# this makes it available to the terminal session not only in this makefile
export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/google_credentials.json

#Clone repo and move to the repo directory to use the make file
#git clone https://github.com/rbblmr/nfl_project.git
#sudo apt install make

prerequisites: 
	@sudo apt update && sudo apt -y upgrade
	@sudo apt install docker.io python3.9 python3-pip -y
	@sudo pip install pipenv
	@-sudo groupadd docker
	@-sudo gpasswd -a ${USER} docker
# exit
# ssh ml-instance.${GCLOUD_ZONE}.${GCLOUD_PROJECT_ID}
	@sudo service docker restart
# adding - before the command ignores any warning
	@-mkdir ~/bin
	@cd ~/bin; wget https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -O docker-compose
	@chmod +x ~/bin/docker-compose
	@touch ~/.bashrc
	@echo 'export PATH="${HOME}/bin:${PATH}"' >> ~/.bashrc
# echo -e makes the /t /n /r possible escapes
	@source ~/.bashrc

terraform-setup:
	curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
	sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com focal main"
	sudo apt-get update && sudo apt-get install terraform

gcloud-initialize:
	@gcloud init
	@mkdir -p ~/.google/credentials/
	@gcloud iam service-accounts create ${GCP_SERVICE_ACCOUNT_USER} --description="Service account for ML projects" --display-name="${GCP_SERVICE_ACCOUNT_USER}"
	
<<<<<<< HEAD
# @gcloud iam service-accounts get-iam-policy ${GCLOUD_SERVICE_ACCOUNT_USER}@macYhine-learning-387107.iam.gserviceaccount.com --format json > ~/.google/credentials/${GCLOUD_SERVICE_ACCOUNT_USER}-policy.json
=======
# @gcloud iam service-accounts get-iam-policy ${GCP_SERVICE_ACCOUNT_USER}@machine-learning-387107.iam.gserviceaccount.com --format json > ~/.google/credentials/${GCP_SERVICE_ACCOUNT_USER}-policy.json
>>>>>>> 2b83a45 (Update)
# 
# @gcloud iam service-accounts set-iam-policy ${GCP_SERVICE_ACCOUNT_USER}@machine-learning-387107.iam.gserviceaccount.com \
# ~/.google/credentials/${GCP_SERVICE_ACCOUNT_USER}-policy.json

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
<<<<<<< HEAD

	@cd ${GCLOUD_CREDENTIALS_DIR} && gcloud iam service-accounts keys create ${GOOGLE_CREDENTIALS_NAME} --iam-account=${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com
=======
	
	@cd ${GCP_CREDENTIALS_DIR} && gcloud iam service-accounts keys create ml_credentials.json --iam-account=${GCP_SERVICE_ACCOUNT_USER}@${GCP_PROJECT_ID}.iam.gserviceaccount.com
>>>>>>> 2b83a45 (Update)
	@gcloud auth application-default login


terraform-infrastracture: 
	cd ~/nfl_project/terraform; terraform init; terraform plan -var="project=${GCP_PROJECT_ID}"; terraform apply -var="project=${GCP_PROJECT_ID}"

airflow-setup:
	cd ~/nfl_project/airflow; mkdir -p ./dags ./logs ./plugins; echo -e "AIRFLOW_UID=$(id -u)" > ./airflow/.env; docker-compose build; docker-compose up -d

# In another shell session, get the worker id 
airflow-gcloud-init:
	docker exec -it $(docker ps --filter "name=airflow-worker-1" --format "{{.ID}}") gcloud init
	docker exec -it $(docker ps --filter "name=airflow-worker-1" --format "{{.ID}}") gcloud auth application-default login
# gcloud init
# gcloud auth application-default login
# port forward and go to localhost:8080 in browser, airflow:airflow

vm-down:
	@gcloud compute instances delete ${GCP_VM} --zone asia-east1-a
