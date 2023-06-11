#!/usr/bin/python3
import pandas as pd
#import numpy as np
import psycopg2
import sqlalchemy
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
    PP = pd.Series((df['high'] + df['low'] + df['close']) / 3)  
    R1 = pd.Series(2 * PP - df['low'])  
    S1 = pd.Series(2 * PP - df['high'])  
    R2 = pd.Series(PP + df['high'] - df['low'])  
    S2 = pd.Series(PP - df['high'] + df['low'])  
    R3 = pd.Series(df['high'] + 2 * (PP - df['low']))  
    S3 = pd.Series(df['low'] - 2 * (df['high'] - PP))  
    psr = {'PP':PP, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3}  
    PSR = pd.DataFrame(psr)  
    df = df.join(PSR)  
    return df
  
def graficador2(indi,temporalidad,cols=None):
   global asset
   global hist
   global stock_df
   options={'rsi_14':"b" ,'macd':"g" ,'kdjk':"r" ,"ppsr":"b"}
   color =options[indi]
   if indi == "ppsr":
     stock_df = PPSR(hist)
     fig, ax = mpf.plot(hist, type='candle', figratio=(12, 8), title=f'{asset} Indicador: {indi} Temporalidad: {temporalidad}', style='yahoo',
                   mav=(12, 26), volume=True,
                   addplot=[mpf.make_addplot(stock_df[cols], panel=1, color=color)],
                   returnfig=True)
     return fig ,stock_df
   else:
     fig, ax = mpf.plot(hist, type='candle', figratio=(12, 8), title=f'{asset} Indicador: {indi} Temporalidad: {temporalidad}', style='yahoo',
                   mav=(12, 26), volume=True,
                   addplot=[mpf.make_addplot(stock_df[indi], panel=1, color=color)],
                   returnfig=True)
   return fig
   
"""def graficador(df, columns, title):
    fig, ax = mpf.plot(hist, type='candle', figratio=(12, 8), title=f'{asset} Indicador: {indi} Temporalidad: {temporalidad}', style='yahoo',
                   mav=(12, 26), volume=True,
                   addplot=[mpf.make_addplot(stock_df[indi], panel=1, color=color)],
                   returnfig=True)
    
    plot_df = df[columns]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_title(title)
    plot_df.plot(ax=ax, figsize=(20, 8))
    return fig"""
def main():
  global hist, asset,stock_df
  global pro_df
  #start_date= input("start date")
  #close_price_date= input("close_price date")
  choose = int(input("choose assets ticket:\n 1:bitcoin \n2:ethereum \n3:ripple \n 4:matic_network\n 5:polkadot"))
  coins={1:"bitcoin" ,2:"ethereum" ,3:"ripple" ,4:"matic_network",5:"polkadot"}
  asset =coins[choose]
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
  for indicador in range(4):
     indicator= indicador + 1
     for tiempo in range(3):
      #indicator = int(input("1: RSI_14 \n 2:MACD \n 3:Indi_Stocástico \n 4:PPSR\n"))
      #tempo = int(input("1: 30 min \n 2:4 hrs \n 3:4days") )# 
      tempo = tiempo +1
      #start_date = '2023-06-01'
      #end_date = '2023-06-30'


      """---------------+--------
      bitcoin       | BTC
      ethereum      | ETH
      ripple        | XRP
      matic-network | MATIC
      polkadot      | DOT"""

      

      temporalidad =""
      if tempo ==1:
        #asset = "bitcoin"  
        #query = f"SELECT * FROM half_hours WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"
        query = f"SELECT *  FROM half_hour;" #ORDER BY date_price DESC LIMIT 90"
        temporalidad="30 minutos"
      elif tempo ==2:
        temporalidad="4 horas"
        query = f"SELECT * FROM four_hours;" #ORDER BY date_price DESC LIMIT 90"
        #asset = "ethereum"  
        #query = f"SELECT * FROM four_hours WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"
      elif tempo == 3:
        temporalidad="4 días"
        query = f"SELECT * FROM four_days;" #ORDER BY date_price DESC LIMIT 90"
        #asset = "ripple"  
        #query = f"SELECT * FROM four_days WHERE coin_id = '{asset}' ORDER BY date_price DESC LIMIT 90"# WHERE date_price >= '{start_date}'"# AND date_price <= '{close_price_date}'"


      hist = pd.read_sql_query(query,db_conection)
      #print(type(hist))
      #hist.to_csv('h.csv', index=False)
      hist.drop(hist.columns[0], axis=1, inplace=True)
      #hist.rename(columns={'close_price': 'close','open_price':'open','high_price':'high','low_price':'low'}, inplace=True)


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
        indi='kdjk'
        fig = graficador2(indi,temporalidad)


      elif indicator ==4:
        #print("PIVOT POINTS, SUPPORTS AND RESISTANCES")
        indi='ppsr'
        cols= ['close', 'PP', 'R1', 'S1', 'R2', 'S2', 'R3', 'S3']
        fig,df =graficador2(indi,temporalidad,cols)



      
      #db_conection.execute(f"DROP TABLE IF EXISTS {processed_data}")

      #pro_df.to_sql(processed_data, engine, index=False)


      print("Procesamiento y almacenamiento de datos completados.")
      try:
        fig.savefig('images_output/{}-{}-{}.png'.format(asset, indi, temporalidad))
      except AttributeError:
        plt.savefig('images_output/{}-{}-{}.png'.format(asset, indi, temporalidad))
      stock_df.to_csv('pdfs_output/{}-{}-{}.csv'.format(asset, indi, temporalidad), index=False)
if __name__ == "__main__":
  main()
  
