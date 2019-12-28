import time
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np

def create_dataset(index_start, index_finish):
    """GET DATA FROM MATCH 11x11.ru/reports/...
    
    PARAMETERS:
    -----------------------
    int index_start :: start parse with 11x11.ru/reports/start
    int index_finish :: start parse with 11x11.ru/reports/finish
    
    RETURN:
    -----------------------
    list of dictionaries :: [{data_start}, {data_two}, ..., {data_finish}],
    data is time match, name players, viewers match, str. on field, score match, type blows, stratety, 
    see more on example.png on dir.
    """
    
    # Autohorization
    data = {
        'auth_name':'webscrap',
        'auth_pass':'webscrap1'
    }
    session = requests.session()
    session.post('http://11x11.ru/', data=data)
    
    ## Add data on list
    result_data = []
    errors = 0
    for i in range(index_start, index_finish):
        try: 
            data = BeautifulSoup(session.get("http://11x11.ru/reports/{}".format(i)).text, "lxml")

            data_table = data('td', colspan = 2)[1]('td', align = None)[2:]
            data_table_right = data('td', colspan = 2)[1]('td', align = 'right')[2:]
            
            time = data('b')[0].string
            viewers = data('p')[-1].string
            str_on_field, disposal, tactic, pressing, strategy, blows = map(lambda x: x.string, data_table)
            str_on_field_r, disposal_r, tactic_r, pressing_r, strategy_r, blows_r = map(lambda x: x.string, data_table_right)
            p1_name = data('h3')[2]('a')[0].string
            p2_name = data('h3')[3]('a')[0].string
            score = data('font')
            result_data.append({
                'time' : time,
                'viewers' : viewers,
                'str_on_field_1' : str_on_field,
                'str_on_field_2' : str_on_field_r,
                'disposal_1' : disposal,
                'disposal_2' : disposal_r,
                'tactic_1' : tactic,
                'tactic_2' : tactic_r,
                'pressing_1' : pressing,
                'pressing_2' : pressing_r, 
                'strategy_1' : strategy,
                'strategy_2' : strategy_r,
                'player_1' : p1_name,
                'player_2' : p2_name,
                'blows_1' : blows,
                'blows_2' : blows_r,
                'id_match' : i,
                'score_player_1' : score[0].string.split(':')[0],
                'score_player_2' : score[0].string.split(':')[1]
            })
        except:
            errors += 1
    return {'data' : result_data, 'errors' : errors}

def find_last_match_id():
    """Find last match id in real time"""
    try:
        data = {
        'auth_name':'webscrap',
        'auth_pass':'webscrap1'
        }
        session = requests.session()
        session.post('http://11x11.ru/', data=data)
    except:
        return 'Auhtorization failed'
    try:
        return int((BeautifulSoup(session.get('http://11x11.ru/xml/games/history.php?act=fullhistory').text, "lxml").
               findAll(href = re.compile('/reports'))[0]['href']).split('/')[-1])
    except:
        return "Not find last id match"
def dowland_chunks(numbers_chunks=3, lenght_cnunks=1000):
    """Dowlands parts datasets
    
    PARAMETERS:
    --------------------------
    int numbers_chunks :: numbers chunks for dowlands
    int lenght_chunks :: lenght chunks
    
    RETURN:
    --------------------------
    list datasets :: list of datasets (in the series form)
    """
    
    datasets = []
    last_id = find_last_match_id()
    for i in reversed(range(numbers_chunks)):
        print('Processing chunk, from {} to {}'.format(last_id - lenght_cnunks * (i + 1) + 1, last_id - lenght_cnunks * i))
        print(100/numbers_chunks * (numbers_chunks - i), '%')
        tmp_df = pd.DataFrame(create_dataset(last_id - lenght_cnunks * (i + 1) + 1, 
                                                 last_id - lenght_cnunks * i))['data']
        datasets.append(tmp_df)

    return datasets

def chucks_to_dataframe(chunks):
    tempory_df = pd.DataFrame()
    for df in chunks:
        tempory_df = pd.concat([tempory_df, pd.DataFrame(map(lambda x: dict(x), df))])
    return tempory_df

def get_dataframe(numbers_chunks=10, lenght_chunks=500):
    return chucks_to_dataframe(dowland_chunks(numbers_chunks, lenght_chunks))
