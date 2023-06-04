Prerequisites 
1. Create a GCP account with your Google email address. 

2. Setup your project named `nfl-project-de`. Take note of your `project ID`.

3. Download Google SDK

4. To run, go into the root of project directory `~/nfl_project/`. Make sure the folder follows the `nfl_project` name:

  1. Download `setup_vm.sh` and `.env` file. Modify the `.env` file with your GCP Project details. Setup GCP VM using the `setup_vm.sh`

      ```
      export $(grep -v '^#' .env | xargs)

      ./setup_vm.sh

      ssh $GCP_VM.$GCP_ZONE.$GCP_PROJECT_ID
      ```

- You should now be in the VM's shell

  2. Clone the repo and move to the repo directory to use the Makefile

      ```
      git clone https://github.com/rbblmr/nfl_project.git

      cd nfl_project
      ```

  3. Run this to have `make` avaiable as a command in the VM:

      ```
      sudo apt install make
      ```
  
  4. Install prerequisites:
    
      ```
      make prerequisites
      ```

  5. Exit the shell and start an SSH session again

      ```
      ssh $GCP_VM.$GCP_ZONE.$GCP_PROJECT_ID
      ```
  
  
  6. Initialize gcloud, choose to log in with new account (your gmail account):

      ```
      cd nfl_project

      make gcloud-initialize
      ```
  
  7. Create terraform infrastructure:

      ```
      make terraform-infra
      ```

  8. Setup airflow:

      ```
      make airflow-setup
      ```

  9. Initialize gcloud within the airflow worker:

      ```
      make airflow-gcloud-init
      ```
  
  10. Forward the 8080 port to access airflow in your browser. The easiest way to do this is in VSCode. You already have your config file when you used `gcloud compute config-ssh` in `setup_vm.sh`.

        a. Make sure you've installed `Remote-SSH` extension in VSCode

        b. Open a Remote Window and select `Connect to Host`
        
        c. Choose your $GCP_VM.$GCP_ZONE.$GCP_PROJECT_ID
        
        d. In the Terminal panel, choose ports and do port forwarding.
        
        e. You can now access the airflow UI in your browser

    Alternatively:
  
    Open another terminal session and move to the local location where you downloaded your .env in STEP 1 :

      ```
      export $(grep -v '^#' .env | xargs)

      ssh -L 8080:localhost:8080 $GCP_VM.$GCP_ZONE.$GCP_PROJECT_ID
      ```
      - You can now access `localhost:8080` in your browser
  
  13. In the airflow UI, enter the default credentials `username: airflow` `password:airflow` you should see two DAGs. Run them.

    a. Unpause the first DAG and WAIT for the run to finish before unpausing the second DAG