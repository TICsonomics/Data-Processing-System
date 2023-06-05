#!/usr/bin/python3
#import mplfinance as mpf
#import yfinance as yf
#import talib 
import pandas_ta as ta
import pandas as pd
#import numpy as np
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

def MA(df, n):
    MA = pd.Series(df['Close'].rolling(min_periods=1, center=True, window=n).mean(), name = 'MA_' + str(n))
    df = df.join(MA)
    return df
# Exponential Moving Average
def EMA(df, n):
	df = MA(hist, 7)
    EMA = pd.Series(df['Close'].ewm(span=n, min_periods = 1).mean(), name='EMA_' + str(n))
    df = df.join(EMA)
    return df
# Momentum  
def MOM(df, n):  
    M = pd.Series(df['Close'].diff(n), name = 'MOM_' + str(n))  
    df = df.join(M)  
    return df
# Pivot Points, Supports and Resistances  
def PPSR(df):  
    PP = pd.Series((df['High'] + df['Low'] + df['Close']) / 3)  
    R1 = pd.Series(2 * PP - df['Low'])  
    S1 = pd.Series(2 * PP - df['High'])  
    R2 = pd.Series(PP + df['High'] - df['Low'])  
    S2 = pd.Series(PP - df['High'] + df['Low'])  
    R3 = pd.Series(df['High'] + 2 * (PP - df['Low']))  
    S3 = pd.Series(df['Low'] - 2 * (df['High'] - PP))  
    psr = {'PP':PP, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3}  
    PSR = pd.DataFrame(psr)  
    df = df.join(PSR)  
    return df
  

def graficador(df, columns, last_n, title):
    plot_df = df[columns].tail(last_n)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_title(title)
    plot_df.plot(ax=ax, figsize=(20, 8))
    return fig
def main():
  #start_date= input("start date")
  #close_date= input("close date")
  indicator = int(input("1: MA \n 2:EMA \n 3:Momentum \n4:PPSR"))
  tempo = int(input("1: 30 min \n 2:4 hrs \n 3:4days") )# 
  #start_date = '2023-06-01'
  #end_date = '2023-06-30'
  db_params = {
    'dbname': 'data_acquisition',
    'user': 'acquisition',
    'password': '0PZ9TVXV',
    'host': 'localhost',
    'port': '5432'
  }
 
  db_conection = create_engine('postgresql://%(user)s:%(password)s@%(host)s:%(port)s/%(dbname)s' % db_params) 
  #DATABASE_URI = 'postgres+psycopg2://username:password@localhost:5432/database_name'
  #engine = create_engine(DATABASE_URI)
  # Consulta SQL para obtener los datos filtrados
  if tempo ==1:
    querry = f"SELECT * FROM half_hour ORDER BY date_price DESC LIMIT 90" # WHERE date_price >= '{start_date}'"# AND date_price <= '{close_date}'"
  elif tempo ==2:
    queryy = f"SELECT * FROM four_hours ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_date}'"
  elif tempo == 3:
    queryy = f"SELECT * FROM four_days ORDER BY date_price DESC LIMIT 90" # WHERE date_price >= '{start_date}'"# AND date_price <= '{close_date}'"


  hist = pd.read_sql_query(queryy,db_conection)
  
  if indicator ==1:
    pro_df = MA(hist, 7)
    fig = graficador(pro_df, ['Close', 'MA_7'], 90, 'MA plot')
  elif indicator ==2:
    
    pro_df = EMA(hist,14)
    fig = graficador(pro_df, ['Close', 'MA_7', 'EMA_14'], 90, 'EMA plot')
  elif indicator ==3:
    pro_df = MOM(hist, 7)
    fig = graficador(pro_df, ['Close', 'MOM_7'], 90, 'Momentum plot')


  elif indicator ==4:
    #print("PIVOT POINTS, SUPPORTS AND RESISTANCES")
    pro_df = PPSR(hist)
    fig =graficador(pro_df, ['Close', 'PP', 'R1', 'S1', 'R2', 'S2', 'R3', 'S3'], 90, 'PPSR plot')
  


  # Borrar el contenido de la tabla 'procesados' si existe
  db_conection.execute(f"DROP TABLE IF EXISTS {processed_data}")

  pro_df.to_sql(processed_data, engine, index=False)


  print("Procesamiento y almacenamiento de datos completados.")
  return fig
if __name__ == "__main__":
  fig =main()
  plt.show()
