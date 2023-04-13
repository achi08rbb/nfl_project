# Environment setup
 - For this project you'll need:
    - Python 3 (e.g. installed with Anaconda)
    - Google Cloud SDK
    - Docker with docker-compose
    - Terraform

# **Local Setup for Terraform and GCP**
Pre-Requisites
1. Terraform client installation: https://www.terraform.io/downloads
2. Cloud Provider account: https://console.cloud.google.com/
3. Clone the contents of this repo in your device
4. If running on windows, use MINGW/Git Bash for the commands shown here

# **GCP**

1.  After making your google cloud account. Follow instructions here in making your project: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/2_gcp_overview.md#initial-setup

2. Take note of your project ID 

3. Make a Service Account and grant it with the following roles:
    - ![](./images/2023-04-09-23-15-05.png)
    - ![](./images/2023-04-09-23-16-46.png)
    - BigQuery Admin
    - Storage Admin
    - Storage Object Admin
    - Storage Object Creator
    - Dataproc Worker
    - Dataproc Service Agent
    - Viewer
4. Download your Service Account credentials file
    - Store it in your project path, into a path like `<project-path>/.google/credentials/`
    - ![](./images/223-04-09-23-18-18.png)
    - ![](./images/223-04-09-23-18-41.png)
    - ![](./images/223-04-09-23-18-54.png)

5. Enable the following APIs:
    - https://console.cloud.google.com/apis/library/iam.googleapis.com
    - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
    - https://console.developers.google.com/apis/api/dataproc.googleapis.com/

6. In your CLI, create the environment variable for the path to your downloaded credentials `google_credentials.json`
    - name it `GOOGLE_APPLICATION_CREDENTIALS`
    
    ```
    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
    ```
7. Download [SDK](https://cloud.google.com/sdk/docs/quickstart) and choose the installer for your OS.

8. Initialize the SDK [following these instructions](https://cloud.google.com/sdk/docs/initializing)

    - In your MINGW/Git Bash CLI, enable `bash` by typing it in the command line
    - type `nano ~/.bashrc file` to edit the .bashrc file (located in your home directory `~` ) and append this at the end of the file:
    ```
    export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
    ```
    - type `source ~/.bashrc` to enable the change 
        - Do this everytime you start a new session of you CLI
    
    - run `gloud init`
    - choose the `nfl-project` when prompted

8. To obtain access credentials for your user account, run the following code and log in with the email associated with your google cloud:
    ```
    gcloud auth application-default login
    ```
9. You have set up your GCP! Let's build the infrastracture.


# **Terraform**
- Set up your GCP infrastracture using terraform
- The following resources will be created:
    1. Big Query: Data Warehouse
    2. Google Cloud Storage: Data Lake
    3. Google Dataproc: Spark Cluster (for running spark jobs)

1. After installing terraform, follow this guide: https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cligo 

2. Modify the project variable in the variables.tf according to your GCP Project ID.

3. Go to your CLI and `cd` to the terraform folder in the cloned project repo
    ```
    cd <path-where-you-cloned-the-repo>/nflproject/terraform
    ```

4. Follow these execution steps:
    ```
    # Initialize state file (.tfstate)
    terraform init

    # Check changes to new infra plan
    terraform plan -var="project=<your-gcp-project-id>"
    
    # Create new infra, this usually takes around 2 mins to complete
    terraform apply -var="project=<your-gcp-project-id>"

    # Delete infra after your work, to avoid costs on any running services
    terraform destroy
    ```

5. In case you get an error: refer to the guide: [terraform installation guide](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli) : 
    ```
    If you get an error that terraform could not be found, your PATH environment variable was not set up properly. Please go back and ensure that your PATH variable contains the directory where Terraform was installed.
    ```

# **Airflow**
Prerequisites:
1. Docker and docker compose 

https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2022/week_3_data_warehouse/airflow/1_setup_official.md

1. Go to your main project directory and move to the airflow folder
    `cd <path-to-your-project>/airflow
2. Make sure you've done `source ~/.bashrc`, as instructed in [GCP](#gcp) setup section, to have your GOOGLE_APPLICATION_CREDENTIALS available in the session.
3. Make the required directories for setting up airflow
    
    ```bash
    mkdir -p ./dags ./logs ./plugins
    ```

4. Make sure the dockerfile and docker-compose.yaml file is in the current working directory, which should be the airflow folder directory


    i. Start docker daemon by opening Docker Desktop in Windows and run the following in your CLI

        ```
        echo -e "AIRFLOW_UID=$(id -u) > .env
        ```

    ii. Build the image
    ```
    docker-compose build
    ```
    or (for legacy versions)
    ```
    docker build .
    ```    
    iii. Initialize the Airflow scheduler, DB, and other config
    ```
    docker-compose up airflow-init
    ```    
    iv. Kick up the all the services from the container:
    ```
    docker-compose up
    ```    
    v. In another terminal, run docker-compose ps to see which containers are up & running (there should be 7, matching with the services in your docker-compose file).

    vi. Login to Airflow web UI on localhost:8080 with default creds: airflow/airflow
    
    vii. On finishing your run or to shut down the container/s:
    ```
    docker-compose down
    ```
    viii. To stop and delete containers, delete volumes with database data, and download images, run:

    ```
    docker-compose down --volumes --rmi all
    
    or

    docker-compose down --volumes --remove-orphans

    ix. To check if your credentials are right where they should be, run:

    ```
    docker ps
    ```
    
Look for the airflow worker container id and run the following:
   
    ```
    docker-compose exec -it <container-id-of-airflow-worker> bash
    ```

    You can now navigate within the container as you would in your own local setup


- Make sure to run the following inside the airflow worker container so you could use gsutil later:
           
    ```
gcloud auth application-default login
    ```

