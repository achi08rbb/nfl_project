SHELL:=/bin/bash
-include .env
# Outside prerequisites:
# sudo apt install make
export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/google_credentials.json

#Clone repo and move to the repo directory to use the make file

prerequisites: 
	@sudo apt update && sudo apt -y upgrade
	@sudo apt install docker.io python3.9 python3-pip -y
	@sudo pip install pipenv
	@-sudo groupadd docker
	@-sudo gpasswd -a ${USER} docker
# exit
# ssh ml-instance.${GCLOUD_ZONE}.${GCLOUD_PROJECT_ID}
	@sudo service docker restart
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
	@gcloud iam service-accounts create ${GCLOUD_SERVICE_ACCOUNT_USER} --description="Service account for ML projects" --display-name="${GCLOUD_SERVICE_ACCOUNT_USER}"
	
# @gcloud iam service-accounts get-iam-policy ${GCLOUD_SERVICE_ACCOUNT_USER}@macYhine-learning-387107.iam.gserviceaccount.com --format json > ~/.google/credentials/${GCLOUD_SERVICE_ACCOUNT_USER}-policy.json
# 
# @gcloud iam service-accounts set-iam-policy ${GCLOUD_SERVICE_ACCOUNT_USER}@machine-learning-387107.iam.gserviceaccount.com \
# ~/.google/credentials/${GCLOUD_SERVICE_ACCOUNT_USER}-policy.json

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/viewer"

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"
	
	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
	
	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectAdmin"

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectCreator"

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/storage.objectViewer"

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/dataproc.worker"

	@gcloud projects add-iam-policy-binding ${GCLOUD_PROJECT_ID} --member="serviceAccount:${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com" \
	--role="roles/dataproc.serviceAgent"

	@cd ${GCLOUD_CREDENTIALS_DIR} && gcloud iam service-accounts keys create ${GOOGLE_CREDENTIALS_NAME} --iam-account=${GCLOUD_SERVICE_ACCOUNT_USER}@${GCLOUD_PROJECT_ID}.iam.gserviceaccount.com
	@gcloud auth application-default login


terraform-infrastracture: 
	cd ~/nfl_project/terraform; terraform init; terraform plan -var="project=${GCLOUD_PROJECT_ID}"; terraform apply -var="project=${GCLOUD_PROJECT_ID}"

airflow-setup:
	cd ~/nfl_project/airflow; mkdir -p ./dags ./logs ./plugins; echo -e "AIRFLOW_UID=$(id -u)" > .env; docker-compose build; docker-compose up -d

# In another shell session, get the worker id 
airflow-gcloud-init:
	docker exec -it $(docker ps --filter "name=airflow-worker-1" --format "{{.ID}}") gcloud init
	docker exec -it $(docker ps --filter "name=airflow-worker-1" --format "{{.ID}}") gcloud auth application-default login
# gcloud init
# gcloud auth application-default login
# port forward and go to localhost:8080 in browser, airflow:airflow

vm-down:
	@gcloud compute instances delete ${GCLOUD_VM} --zone asia-east1-a
