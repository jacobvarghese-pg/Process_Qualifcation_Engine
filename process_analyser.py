import pandas as pd
import numpy as np
import pytz
import time
import threading 
import queue
from datetime import datetime
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt
import statistics
pd.options.mode.chained_assignment = None  # default='warn'

#Eliminate all warning messages
import warnings
warnings.filterwarnings('ignore')

def get_data(server_name, measure, filter_name, start_time, end_time, database='databroadcaster', port=8086, schema='high_definition'):
    # Support both single tag and list
    if isinstance(filter_name, str):
        filter_name = [filter_name]
    # Support both timestamp and datetime inputs
    if isinstance(start_time, datetime):
        start_time = datetime.timestamp(start_time)
    if isinstance(end_time, datetime):
        end_time = datetime.timestamp(end_time)

    api_key="eyJrIjoiRThhYzMybWwwUGU5ODJReEJ5TnJLSHR4eFhKVmJvdVAiLCJuIjoidmljdG9yaWEiLCJpZCI6MX0="
    client = InfluxDBClient(host=server_name, port=port, database=database,ssl=True,verify_ssl=False,username="api_key",password=api_key)

  
    try:
        q_str = '''
            SELECT
                "time","value","Line State","Controller Name"
            FROM
                "{0}"."{1}"
            WHERE
                time >= {2:d}ms
                    AND
                time < {3:d}ms
        '''.format(schema, measure, int(start_time * 1000.0), int(end_time * 1000.0))
        #print(q_str)
        return pd.DataFrame(client.query(q_str).get_points())
    finally:
        client.close()


def get_metrics(df_01, devs = 2):
    
    #df_01 = df_01.groupby(['Reference Name']).resample('15s', on = 'time').sum(min_count = 15)/15 # basically creating an average where it's required to have 15 samples in the resample bin.

    df_01['mean'] = df_01['value'].mean(periods = 40) # use 40 periods for a 10min lag
    df_01.dropna(inplace = True)
    df_01.reset_index(inplace = True)
    
    data_mn = df_01['mean']
    avg_mn =  round(data.mean(),4)
    std_mn = round(data.std(),4)
    upper_lim_mn = avg_mn + devs*std_mn
    lower_lim_mn = avg_mn - devs*std_mn
    
    df_01['max'] = df_01['value'].max(periods = 40) # use 40 periods for a 10min lag
    df_01.dropna(inplace = True)
    df_01.reset_index(inplace = True)
    data_mx = df_01['max']
    avg_mx =  round(data.mean(),4)
    std_mx = round(data.std(),4)
    upper_lim_mx = avg_mx + devs*std_mx
    lower_lim_mx = avg_mx - devs*std_mx
       
    data_raw=df_01['value']
    avg_raw =  round(data_raw.mean(),4)
    std_raw = round(data_raw.std(),4)
    upper_lim_raw = avg_raw + devs*std_raw
    lower_lim_raw = avg_raw - devs*std_raw
    
    print("Mean: ", avg)
    print("STD: ", std)
    print("With {} High deviations, the limits are: ".format(devs),avg - devs*std, avg + devs*std)
    
    return avg_mn, std_mn, upper_lim_mn, lower_lim_mn, avg_mx, std_mx, upper_lim_mx, lower_lim_mx, avg_raw, std_raw, upper_lim_raw, lower_lim_raw

def average(arr, n):
    end =  n * int(len(arr)/n)
    return np.mean(arr[:end].reshape(-1, n), 1)

def maximum(arr, n):
    end =  n * int(len(arr)/n)
    return np.max(arr[:end].reshape(-1, n), 1)

def std_dev(arr, n):
    end =  n * int(len(arr)/n)
    return np.std(arr[:end].reshape(-1, n), 1)

    # Control Charting library

def save_figures_to_file(x,filename):
    # Dont show figs
    import matplotlib
    matplotlib.use('Agg')
    
    # Set random seed
    np.random.seed(42)

    # Define list variable for moving ranges
    MR = [0]
    
    # Get and append moving ranges
    i = 1
    for data in range(1, len(x)):
        MR.append(abs(x[i] - x[i-1]))
        i += 1

    # Convert list to pandas Series objects    
    MR = pd.Series(MR)
    x = pd.Series(x)
    #import pdb; pdb.set_trace()

    # Concatenate mR Series with and rename columns
    data = pd.concat([x,MR], axis=1)
    data.columns = ['x', 'mR']  

    # Plot x and mR charts
    fig, axs = plt.subplots(2, figsize=(30,30), sharex=True)
   
    # x chart
    axs[0].plot(data['x'], linestyle='-', marker='o', color='black')
    axs[0].axhline(statistics.mean(data['x']), color='blue')
    axs[0].axhline(statistics.mean(data['x'])+3*statistics.mean(data['mR'][1:len(data['mR'])])/1.128, color = 'red', linestyle = 'dashed')
    axs[0].axhline(statistics.mean(data['x'])-3*statistics.mean(data['mR'][1:len(data['mR'])])/1.128, color = 'red', linestyle = 'dashed')
    axs[0].set_title('Individual Chart')
    axs[0].set(xlabel='Unit', ylabel='Value')

    # mR chart
    axs[1].plot(data['mR'], linestyle='-', marker='o', color='black')
    axs[1].axhline(statistics.mean(data['mR'][1:len(data['mR'])]), color='blue')
    axs[1].axhline(statistics.mean(data['mR'][1:len(data['mR'])])+3*statistics.mean(data['mR'][1:len(data['mR'])])*0.8525, color='red', linestyle ='dashed')
    axs[1].axhline(statistics.mean(data['mR'][1:len(data['mR'])])-3*statistics.mean(data['mR'][1:len(data['mR'])])*0.8525, color='red', linestyle ='dashed')
    axs[1].set_ylim(bottom=0)
    axs[1].set_title('mR Chart')
    axs[1].set(xlabel='Unit', ylabel='Range')

    # Save the full figure...
    filename= filename + ".png"
    fig.savefig(filename)
   
    return fig

def save_data_to_file(measure, df):
    df_00 = df.copy()
    df_00 = df_00[df_00['Line State'] == 'Producing (Execute)']
    df_01 = df_00.copy()
               
    df_01['time'] = pd.to_datetime(df_00['time'])
    df_01.dropna(inplace = True)
    df_01 = df_01.reset_index(drop = True)
                
    data_avgd = df_01['value'].to_numpy()
    avgs = average(data_avgd, 200) #10 mins data average
    filename_avg= "Average Plot_10min " + str(measure)
    avg_plot = save_figures_to_file(avgs, filename_avg)
                
    data_maxd = df_01['value'].to_numpy()
    maxd = maximum(data_maxd, 200) #10 mins data average
    filename_max= "Max Plot_1hr " + str(measure)
    max_plot = save_figures_to_file(maxd, filename_max)

    return (filename_avg+".png", filename_max+".png")
    #return (avg_plot, max_plot)

def initialize_db():
    database='databroadcaster'
    proxy_url="badboihtr109.bcc.pg.com/api/datasources/proxy/1"
    api_key="eyJrIjoiRThhYzMybWwwUGU5ODJReEJ5TnJLSHR4eFhKVmJvdVAiLCJuIjoidmljdG9yaWEiLCJpZCI6MX0="
    source = InfluxDBClient(host=proxy_url, port=443, database=database,ssl=True,verify_ssl=False,username="api_key",password=api_key)
    source.ping()

if __name__ == "__main__":
    df_excel = pd.read_excel (r'C:\Users\varghese.j.5\PythonAnaconda\smartqualification\config.xlsx', sheet_name='Main')
    lines = ["badboihtr109.bcc.pg.com/api/datasources/proxy/1"]
    measures = df_excel['Tags']
    
    start = datetime.timestamp(datetime(2022,4,19,19))
    end = datetime.timestamp(datetime(2022,4,21,19))
    
    total_periods = int((end - start)/(3600)) # gives the number of 1 hr period in this time frame

    initialize_db()
    
    for line in lines:
        stds = []
    
        for measure in measures:
            print(line+' '+measure)
            df = pd.DataFrame()
            for i in range(total_periods):
                df.loc['Tag']=measure
                df = df.append(get_data(line, schema='high_definition', filter_name = 'Main',measure = measure, start_time = (start + i*8*3600), end_time = (start + (i+1)*8*3600)))
                df = df.reset_index(drop = True)
    
            
            save_data_to_file(measure, df)    

