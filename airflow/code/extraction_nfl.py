# Requirements
import requests
import pandas as pd
import json
import re
from pathlib import Path
import threading, queue
import os
import time
import pyarrow
import argparse
from bs4 import BeautifulSoup
import time

#=========================================================================
# Function of getting the TEAM DETAILS

def get_teams(teamIds:list, year:int) -> pd.DataFrame:
    """ Function for running the extraction workers of team details """

    # list for response df
    teams_df_list=[]

    # list for failed_requests
    failed_response=[]

    # column to add
    logos=[]

    # set for the Ids
    Ids=set()

    # set no of workers
    num_workers=32
    
    
    # team worker
    def team_worker(worker_num:int, q:queue) -> None:
        """ Getting the team details """
            
        # set the base url here
        base_url=f'http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/'

        with requests.Session() as session:
            while True:

                # add process id and transaction id to ids
                Ids.add(f'Worker: {worker_num}, PID: {os.getpid()}, TID: {threading.get_ident()}')

                # get the team number from the queue
                teamId=q.get()

                # set the url endpoint:
                endpoint = f"teams/{teamId}?lang=en&region=us"

                # get the data using session.get
                response = session.get(url=base_url+endpoint)

                if response.ok:
                    team=response.json()
                    team_df=pd.json_normalize(team)
                    teams_df_list.append(team_df)
                    try:
                        logo=team['logos'][0]['href'] # link
                    except:
                        logo='' # link
                    else:
                        team_df['logo']=logo
                else:
                    failed_response.append(teamId)
                q.task_done()

    # log: start
    print(f"Fetching for teams started")
    
    # make an empty queue
    q=queue.Queue() # called the method Queue from the queue module
    
    # loop over the teams_id list
    for teamId in teamIds:
        # add to queue
        q.put(teamId)
    
    # loop over the no. of workers
    for i in range(num_workers):
        # threading.Thread(target=function, args of the function, daemon).start()
        threading.Thread(target=team_worker, args=(i,q), daemon=True).start()
        
    # maintain the process while it's not finished
    q.join()
    
    
    teams_df=pd.concat(teams_df_list, ignore_index=True)
    
    # log: end
    print(f"Fetching for teams ended")
    
    return teams_df
#=========================================================================
# Function for getting TEAM STATS
            
def get_teams_stats(teamIds: list, year: int, season_type: int) -> pd.DataFrame:
    """" Main function for running the workers and getting the athlete id """
        
    teams_stats_list=[]

    failed_response=[]

    worker_num=32

    Ids= set()
    
        
    def team_stats_worker(worker_num: int, q: queue) -> None:
        """ Getting the team stats """
    
        base_url=f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/{season_type}/"

        with requests.Session() as session:
            while True:

                Ids.add(f"Worker: {worker_num}, PID:{os.getpid()}, TID:{threading.get_ident()}")

                teamId=q.get()

                endpoint=f"teams/{teamId}/statistics?lang=en&region=us"

                response=session.get(url=base_url+endpoint)

                if response.ok:
                    stats=response.json()
                    stats_df=pd.json_normalize(stats['splits']['categories'][0:10], ['stats'], meta=['displayName'], meta_prefix='categories.')
                    stats_df['teamId']=int(teamId)
                    teams_stats_list.append(stats_df)
                else:
                    failed_response.append(teamId)
                q.task_done()
                
                
    # log: start
    print(f"Fetching for teams_stats started")
    
    q=queue.Queue()
    
    for team in teamIds:
        q.put(team)
        
    for i in range(worker_num):
        threading.Thread(target=team_stats_worker, args=(i,q), daemon=True).start()
    
    q.join()
    
    stats_dfs=pd.concat(teams_stats_list, ignore_index=True).fillna(0)
    
    # log: end
    print(f"Fetching for team_stats ended")
    
    return stats_dfs
#=========================================================================
# Function for getting all the ATHLETE IDS
            
def get_athlete_ids() -> list:
    """ Main function for running the workers and getting the athlete id """
    
    urls=[]

    athlete_ids=[]

    failed_response_page=[]

    worker_num=32

    Ids= set()

    # get a sample json result to get the page count in the API endpoint
    pages=requests.get(f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes?limit=1000&page=1').json()['pageCount']
    
    
    def athlete_id_worker_1(worker_num: int, p:queue):
        """ Getting the athlete links """

        base_url=f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/"

        with requests.Session() as session:
            while True:

                Ids.add(f"Worker: {worker_num}, PID:{os.getpid()}, TID:{threading.get_ident()}")

                page=p.get()

                endpoint=f"athletes?limit=1000&page={page}"

                response=session.get(url=base_url+endpoint)

                if response.ok:
                    page_json=response.json()
                    for index in range(1, len(page_json['items'])):
                        urls.append(page_json['items'][index]['$ref'])

                else:
                    failed_response_page.append(page)

                p.task_done()
                
        
    def athlete_id_worker_2(worker_num: int, u:queue) -> None:
        """ Getting the athlete ids from the link using regex """
        
        with requests.Session() as session:
            while True:

                url=u.get()

                athlete_id=re.findall(r"\/(\w*)\?", url)[0]

                athlete_ids.append(athlete_id)

                u.task_done()


    # log: start
    print(f"Fetching for athlete ids started")
    
    # loop to get the pages content
    p=queue.Queue()
    for page in range(1, pages+1):
        p.put(page)

    
    for i in range(worker_num):
        threading.Thread(target=athlete_id_worker_1, args=(i,p), daemon=True).start()
        
    p.join() 
    
    # loop to get id from the url
    u=queue.Queue()
    for url in urls:
        u.put(url)
        
    for i in range(worker_num):
        threading.Thread(target=athlete_id_worker_2, args=(i,u), daemon=True).start()
    
    u.join()
    
    # log: end
    print(f"Fetching for athlete ids ended")
    
    return athlete_ids
#=========================================================================
# Function for getting ATHLETE STATS


def get_athletes_stats(athleteIds: list, year:int, season_type: int) -> pd.DataFrame:
    """ Main function for running the workers and getting the athlete stats """
    
    athletes_df_list = []

    failed_requests = []

    Ids = set()

    num_workers = 32

    def athletes_stats_worker(worker_num:int, q:queue) -> None:
        """ Getting the athlete stats """

        base_url = f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/{season_type}/athletes'

        with requests.Session() as session:
            while True:

                Ids.add(f'Worker: {worker_num}, PID: {os.getpid()}, TID: {threading.get_ident()}')

                athleteId = q.get()

                endpoint = f'/{athleteId}/statistics'

                response = session.get(url=base_url+endpoint)

                if response.ok:
                    athlete=response.json()
                    athlete_df=pd.json_normalize(data=athlete['splits']['categories'], record_path=['stats'], meta_prefix='split.categories.', meta=['displayName'])
                    athlete_df['athleteId']=athleteId
                    athletes_df_list.append(athlete_df)

                else:
                    pass
                q.task_done() 
    
    q = queue.Queue()
    for athlete in athleteIds:
        q.put(athlete)

    # log: start
    print(f"Fetching for athlete stats started")
    
    for i in range(num_workers):
        threading.Thread(target=athletes_stats_worker, args=(i, q), daemon=True).start()
        
    q.join()
    
    athletes_stats=pd.concat(athletes_df_list, ignore_index=True)
    
        
    # log: end
    print(f"Fetching for athlete stats ended")

    return athletes_stats

#=========================================================

# Function for getting ATHLETES DETAILS

def get_athletes(athleteIds: list, year:int) -> pd.DataFrame:
    """ Main function for running the workers and getting the athletes details """
    
    athletes_df_list=[]

    failed_response=[]

    worker_num=64

    Ids= set()


    def athletes_worker(worker_num: int, q: queue) -> None:
        """ Getting the athletes details """

        base_url=f"http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/"

        with requests.Session() as session:
            while True:

                Ids.add(f"Worker: {worker_num}, PID:{os.getpid()}, TID:{threading.get_ident()}")

                athleteId=q.get()

                endpoint=f"athletes/{athleteId}"

                athlete=session.get(url=base_url+endpoint)

                if athlete.ok:
                    athlete_json=athlete.json()
                    try:
                        athlete_json['team']
                        team_id=re.findall(r'\/(\w*)\?', athlete_json['team']['$ref'])
                        athlete_df=pd.json_normalize(athlete_json)
                        athlete_df['teamId']=team_id
                        athletes_df_list.append(athlete_df)
                    except:
                        pass
                else:
                    pass

                q.task_done()
    
    
    # log: start
    print(f"Fetching for athlete details started")
    q=queue.Queue()
    
    for athleteId in athleteIds:
        q.put(athleteId)
        
    for i in range(worker_num):
        threading.Thread(target=athletes_worker, args=(i,q), daemon=True).start()
    
    q.join() 
    
    athletes_df=pd.concat(athletes_df_list, ignore_index=True)
    
    # log: end
    print(f"Fetching for athlete details ended")
    
    return athletes_df
#=========================================================
# Function for getting DEFENSE STATS through webscraping

def webscrape_defense_stats(year:int, season_type:int) -> pd.DataFrame:
    """Getting the defense stats (allowed yards and score) from ESPN thru webscraping"""
    
    # log: start
    print(f"Fetching for teams defense stats started")
    
    url = requests.get(f"https://www.espn.com/nfl/stats/team/_/view/defense/season/{year}/seasontype/{season_type}").text
    
    soup = BeautifulSoup(url, "html.parser")
    
    tables = soup.find_all("table")
    
    stats=pd.read_html(str(tables[1]), flavor='bs4')[0]
    
    teams=pd.read_html(str(tables[0]), flavor='bs4', header=0)[0]
    
    teams_defense_stats_df=pd.concat([teams,stats], axis=1)

    teams_defense_stats_df.columns=['teamName',
            'gamesPlayed',
            'totalYDS',
            'totalYDSG',
            'passingYDS',
            'passingYDSG',
            'rushingYDS',
            'rushingYDSG',
            'points',
            'pointsPerGame']
    
    teams_defense_stats_df.columns=teams_defense_stats_df.columns.astype('string')    
    
    # log: end
    print(f"Fetching for teams defense stats ended")
    
    
    return teams_defense_stats_df

#=========================================================
# Function for getting LEADERS

def get_leaders(year: int, season_type: int) -> pd.DataFrame:
    """Getting all the leaders for the specified year and season type"""

    # log: start
    print(f"Fetching for leaders started")

    url=f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/{season_type}/leaders'

    leaders=requests.get(url)

    leaders=leaders.json()

    leaders_df=pd.json_normalize(leaders['categories'][0:10], record_path=['leaders'], meta=['name','shortDisplayName'])

    # log: end
    print(f"Fetching for leaders ended")
    
    return leaders_df

#============================================================
# Function for loading the data

def load_to_local(df_list: list, filenames: list):
    """Loading data to local storage"""
    
    # log: start
    print(f"Loading for nfl data to parquet started")
    
    for df, filename in zip(df_list, filenames):
        path=Path(f"./nfl_parquets/{filename}/{year}/{season_type}")
        path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(f"{path}/{filename}_parquet", engine='pyarrow')
    
    # log: end
    print(f"Loading for nfl data to parquet ended")

### Load data to GCS 
def load_to_gcs(df_list: list, filenames: list):
    print(f"Loading for nfl data to parquet started")
    path=f'gs://nfl-data-lake_nfl-project-de'
    for df, filename in zip(df_list, filenames):
        df.to_parquet(f'{path}/nfl_parquets/{filename}/{year}/{season_type}/{filename}_parquet', engine='pyarrow')
    print(f"Loading for nfl data to parquet ended")
#============================================================
if __name__=="__main__":
    
    parser=argparse.ArgumentParser()
    parser.add_argument('--year', required=True)
    parser.add_argument('--season_type', required=True)
    args=parser.parse_args()
    
    year=args.year
    season_type=args.season_type
        
    teamIds=[
     '1',
     '2',
     '3',
     '4',
     '5',
     '6',
     '7',
     '8',
     '9',
     '10',
     '11',
     '12',
     '13',
     '14',
     '15',
     '16',
     '17',
     '18',
     '19',
     '20',
     '21',
     '22',
     '23',
     '24',
     '25',
     '26',
     '27',
     '28',
     '29',
     '30',
     '33',
     '34'
    ]

    # Extracting
    # teams_df=get_teams(teamIds)
    
#     stats_df=get_teams_stats(teamIds)
    
#     athlete_ids=get_athlete_ids()
    
#     athletes_stats_df=get_athletes_stats(athlete_ids)
    
#     athletes_df=get_athletes(athlete_ids)
    
    teams_defense_stats_df=webscrape_defense_stats(year,season_type)
    
#     leaders_df=get_leaders(year,season_type)
    
    # Loading
    
    df_list=[
        # teams_df #, 
        # stats_df,
        # athletes_df,
        # athletes_stats_df,
        # leaders_df,
        teams_defense_stats_df
    ]

    filenames=[
        #'teams' 
        # 'team_stats',
        # 'athletes',
        # 'athletes_stats',
        # 'leaders',
        'teams_defense_stats'
    ]


    load_to_gcs(df_list, filenames)

#=================================================
    
    