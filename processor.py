#!/usr/bin/python3
import pandas as pd
#import numpy as np
import psycopg2
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import stockstats
import mplfinance as mpf

def MA(df, n):
    MA = pd.Series(df['close_price'].rolling(min_periods=1, center=True, window=n).mean(), name = 'MA_' + str(n))
    df = df.join(MA)
    return df
# Exponential Moving Average
def MACD(df):
  
  #df = MA(hist, 7)
  #EMA = pd.Series(df['close_price'].ewm(span=n, min_periods = 1).mean(), name='EMA_' + str(n))
  #df = df.join(EMA)
  return df
# Momentum  
def MOM(df, n):  
    M = pd.Series(df['close_price'].diff(n), name = 'MOM_' + str(n))  
    df = df.join(M)  
    return df
# Pivot Points, Supports and Resistances  
def PPSR(df):  
    PP = pd.Series((df['High'] + df['Low'] + df['close_price']) / 3)  
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
  
def graficador2(indi,temporalidad):
   global asset
   global hist
   global stock_df
   options={'rsi_14':"b" ,'macd':"g" ,'kdjk':"r" }
   color =options[indi]
   figure = mpf.plot(hist, type='candle', figratio=(12, 8), title=f'{asset}________indicador:{indi}_________temporalidad:{temporalidad}', style='yahoo', 
          mav=(12, 26), volume=True, 
          addplot=[mpf.make_addplot(stock_df[indi], panel=1, color=color)])
   return figure
   
def graficador(df, columns, last_n, title):
    plot_df = df[columns].tail(last_n)
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_title(title)
    plot_df.plot(ax=ax, figsize=(20, 8))
    return fig
def main():
  global hist, asset,stock_df
  global pro_df
  #start_date= input("start date")
  #close_price_date= input("close_price date")
  indicator = int(input("1: RSI_14 \n 2:MACD \n 3:Indi_Stocástico \n 4:PPSR\n"))
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

  """---------------+--------
 bitcoin       | BTC
 ethereum      | ETH
 ripple        | XRP
 matic-network | MATIC
 polkadot      | DOT"""

  choose = int(input("choose assets ticket:\n 1:bitcoin \n2:ethereum \n3:ripple \n 4:matic_network\n 5:polkadot"))
  coins={1:"bitcoin" ,2:"ethereum" ,3:"ripple" ,4:"matic_network",5:"polkadot"}
  asset =coins[choose]

  temporalidad =""
  if tempo ==1:
    #asset = "bitcoin"  # Asset seleccionado
    #query = f"SELECT * FROM half_hours WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"
    query = f"SELECT date_price  , open_price , high_price , low_price ,close_price , volume   FROM half_hour;" #ORDER BY date_price DESC LIMIT 90"
    temporalidad="30 minutos"
  elif tempo ==2:
    temporalidad="4 horas"
    query = f"SELECT date_price  , open_price , high_price , low_price ,close_price , volume FROM four_hours;" #ORDER BY date_price DESC LIMIT 90"
    #asset = "ethereum"  
    #query = f"SELECT * FROM four_hours WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"
  elif tempo == 3:
    temporalidad="4 días"
    query = f"SELECT date_price  , open_price , high_price , low_price ,close_price , volume FROM four_days;" #ORDER BY date_price DESC LIMIT 90"
    #asset = "ripple"  
    #query = f"SELECT * FROM four_days WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"


  hist = pd.read_sql_query(query,db_conection)
  print(type(hist))
  hist.to_csv('h.csv', index=False)
  hist.drop(hist.columns[0], axis=1, inplace=True)
  hist.rename(columns={'close_price': 'close','open_price':'open','high_price':'high','low_price':'low'}, inplace=True)


  stock_df = stockstats.StockDataFrame.retype(hist)#,convert={'close': 'close_price'})
  #print(stock_df.columns)
  hist.index = pd.to_datetime(hist.index)
  if indicator ==1:
    #pro_df = MA(hist, 7)
    #stock_df['close_price'] 
    pro_df =stock_df['rsi_14'] 
    indi = 'rsi_14'
    fig = graficador2(indi,temporalidad)
  elif indicator ==2:
    
    pro_df =stock_df['macd'] 
    indi = 'macd'
    fig = graficador2(indi,temporalidad)
  elif indicator ==3:
    stock_df['kdjk']
    indi="indicador estocástico"
    fig = graficador2(indi,temporalidad)


  elif indicator ==4:
    #print("PIVOT POINTS, SUPPORTS AND RESISTANCES")
    pro_df = PPSR(hist)
    fig =graficador(pro_df, ['close_price', 'PP', 'R1', 'S1', 'R2', 'S2', 'R3', 'S3'], 90, 'PPSR plot')



  # Borrar el contenido de la tabla 'procesados' si existe
  #db_conection.execute(f"DROP TABLE IF EXISTS {processed_data}")

  #pro_df.to_sql(processed_data, engine, index=False)


  print("Procesamiento y almacenamiento de datos completados.")
  fig.savefig('nombre_archivo.png')
  pro_df.to_csv('nombre_archivo.csv', index=False)
if __name__ == "__main__":
  fig =main()
  plt.show()
