
#! bin/bash
export $(cat .env|xargs)

gcloud init
gcloud compute instances create $GCP_VM --zone=$GCP_ZONE --image-family=ubuntu-2004-lts --image-project=ubuntu-os-cloud --machine-type=e2-standard-4 --boot-disk-size=30GB
gcloud compute config-ssh
ssh $GCP_VM.$GCP_ZONE.$GCP_PROJECT_ID


