> ***NOTE***: This is an ongoing project. Data Visualization is yet to be done.
# NFL DATA ENGINEERING PROJECT

- This project uses Airflow (Docker-containerized) and GCP for the Data Lake, Data Warehouse, and Pyspark job submissions (Dataproc).
- The infrastracture is built using Terraform.

## Goal:
- Build an end-to-end data pipeline
  - Ingesting the data from multiple [ESPN API endpoints](https://gist.github.com/nntrn/ee26cb2a0716de0947a0a4e9a157bc1c)
  - Directly loading the result to Google Cloud Storage (Data Lake)
  - Transforming the data using Pyspark
  - Loading the transformed data to BigQuery (Data Warehouse)
  - Visualize the data using Tableau
  > ***NOTE***: This is an ongoing project. Data Visualization is yet to be done.
  
- For this project, I wanted to answer the following questions:
### Who among my teammates are `extraordinary`?
  - Every season, NFL has its leaderboard for various categories (Passing, Defense, Rushing, etc.):
    - Each category has multiple leaders coming from different teams
    - I wanted to find out the `top` leaders for each category and their teammates.
      - Do their teammates also perform above average? or
      - Is the top leader `carrying` its team?
 
 ### Weakness and Strengths of the Team
   - Each team has different stat categories (Passing, Defense, Rushing, Receiving, Scoring, etc.)
    - I want to get the percentile rank of each team for each category and visualize it using a radar chart
 
 ### Is a team more offensive or defensive?
   - Team stats can be compared with other teams to find out if a team is stronger in their offense or defense
   - This will be visualized using a scatter plot