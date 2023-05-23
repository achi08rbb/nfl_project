
#! bin/bash
export $(cat .env|xargs)

gcloud init
gcloud compute instances create $GCLOUD_VM --zone=$GCLOUD_ZONE --image-family=ubuntu-2004-lts --image-project=ubuntu-os-cloud --machine-type=e2-standard-4 --boot-disk-size=30GB
gcloud compute config-ssh
ssh $GCLOUD_VM.$GCLOUD_ZONE.$GCLOUD_PROJECT_ID


