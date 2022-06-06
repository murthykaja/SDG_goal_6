"""
Project : CSE 545: Big Data Analytics Project Report
Aim : SDG 6 - Clean Water and Sanitation
Team Memabers:
1. Murthy Kaja
2. Yashwanth M
3. Kowshik S
4. Nithin Reddy
System : Azure Databricks 
Databricks Runtime Version : 10.4 LTS (includes Apache Spark 3.2.1, Scala 2.12)
Worker Nodes :  4 (28 GB and 8 cores)
Driver Nodes :  1 (28 GB and 8 cores)
Code Description : The python file follows a importing data, preprocessing and analysis. Implementation of models and hyper parameter tuning. Finally results and plots for the prediciton.
"""



import pandas as pd
import numpy as np

"""#Import datsets"""

#df=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/BDA/euro_unprocessed.csv')
africa=pd.read_csv("/content/drive/MyDrive/Colab Notebooks/BDA/africa_data_no_WQI.csv")
euro=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/BDA/euro_wqi.csv')
india=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/BDA/india_processed.csv')

india.sort_values('date_reported')

xafrica=pd.read_csv("/content/drive/MyDrive/Colab Notebooks/BDA/africa_data_no_WQI.csv")
xeuro=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/BDA/euro_wqi.csv')

xeuro.country.unique()

#xafrica[xafrica.Country=='South Africa'].groupby(['date_reported']).mean()
xeuro[xeuro.country=='France'].groupby(['country','phenomenonTimeSamplingDate']).mean().reset_index()

euro=euro.sort_values('phenomenonTimeSamplingDate').interpolate(method='linear',limit_direction='both').reset_index(drop=True)
india=india.sort_values('date_reported').interpolate(method='linear',limit_direction='both').reset_index(drop=True)
africa=africa.sort_values('date_reported').interpolate(method='linear',limit_direction='both').reset_index(drop=True)

def get_na(x):
  return x.isna().sum()[x.isna().sum()>0]
print(get_na(euro))
print(get_na(africa))
print(get_na(india))

euro.dropna(inplace=True)

euro.median()

"""# Replace Zeros with Median values """

def replace_zero(x):
  return x.replace(0, x.replace(0,np.nan).median())

euro=replace_zero(euro)
india=replace_zero(india)
africa=replace_zero(africa)

euro['Date']=pd.DatetimeIndex(euro['phenomenonTimeSamplingDate'])
india['Date']=pd.DatetimeIndex(india['date_reported'])
africa['Date']=pd.DatetimeIndex(africa['date_reported'])

euro=euro.set_index('Date').drop('phenomenonTimeSamplingDate',axis=1)
india=india.set_index('Date').drop('date_reported',axis=1)
africa=africa.set_index('Date').drop('date_reported',axis=1)

india.drop('Country',axis=1, inplace=True)

africa.drop('Station',axis=1,inplace=True)

africa.reset_index()

import seaborn as sns
import matplotlib.pyplot as plt
for i in euro.country.unique():
  sns.lineplot(euro.reset_index()[euro.reset_index()['country']==i].Date,euro.reset_index()[euro.reset_index()['country']==i].WQI)
  plt.title(i)
  plt.show()

"""# Label Encoding Categorical Variables"""

from sklearn.preprocessing import LabelEncoder as le
lec=le()

euro['country']=lec.fit_transform(euro['country'])
india['Station']=lec.fit_transform(india['Station'])
africa['Country']=lec.fit_transform(africa['Country'])

"""# Fucntion to get water quality index"""

from os import error
def get_wqi(x):
  temp=0
  solids=0
  oxygen=0
  conduct=0

  if x.Water_temperature < 20:
    temp=1
  elif x.Water_temperature>=20 and x.Water_temperature<40:
    temp=(2-(x.Water_temperature/20))
  else:
    temp=0
  
  if x.Total_suspended_solids>=250:
    solids=0
  elif x.Total_suspended_solids<250 and x.Total_suspended_solids>=0:
    solids=(25-(x.Total_suspended_solids/10))
  
  if x.Dissolved_oxygen>=10:
    oxygen=25
  else:
    oxygen=x.Dissolved_oxygen*(2.5)
   
  if x.Conductivity<=200:
    conduct=20
  elif x.Conductivity>200 and x.Conductivity<4000:
    conduct= 20*((4000-x.Conductivity)/3800)
  else:
    conduct=0
  #return ((30*(x.Oxygen_saturation/100))+ solids+ oxygen + conduct)
  return temp*((30*(x.Oxygen_saturation/100))+ solids+ oxygen + conduct)



def get_wqi_india(x):
  temp=0
  solids=0
  oxygen=0
  conduct=0

  if x.Water_temperature < 20:
    temp=1
  elif x.Water_temperature>=20 and x.Water_temperature<40:
    temp=(2-(x.Water_temperature/20))
  else:
    temp=0
  
  if x.Total_suspended_solids>=250:
    solids=0
  elif x.Total_suspended_solids<250 and x.Total_suspended_solids>=0:
    solids=(25-(x.Total_suspended_solids/10))
  
  if x.Dissolved_oxygen>=10:
    oxygen=25
  else:
    oxygen=x.Dissolved_oxygen*(2.5)
   
  if x.Conductivity<=200:
    conduct=20
  elif x.Conductivity>200 and x.Conductivity<4000:
    conduct= 20*((4000-x.Conductivity)/3800)
  else:
    conduct=0

  if x.BOD==0:
    return temp*((30*(30/100))+ solids+ oxygen + conduct)
  elif x.BOD>12:
    return temp*((30*(0/100))+ solids+ oxygen + conduct)
  else:
    return ((30*((1-x.BOD/12)/100))+ solids+ oxygen + conduct)

africa['WQI']=africa.apply(get_wqi,axis=1)

len(africa['WQI'][africa['WQI']<=0])

import seaborn as sns
import matplotlib.pyplot as plt
for i in africa.Country.unique():
  sns.lineplot(africa.reset_index()[africa.reset_index()['Country']==i].Date,africa.reset_index()[africa.reset_index()['Country']==i].WQI)
  plt.title(i)
  plt.show()

india['WQI']=india.apply(get_wqi_india,axis=1)

india.to_csv('India.csv')
africa.to_csv('Africa.csv')

euro.country=euro.country.astype('float64')

euro=euro.reset_index().groupby(['Date','country']).mean().reset_index()

africa=africa.reset_index().groupby(['Date','Country']).mean().reset_index()

india=india.reset_index().groupby(['Date','Station']).mean().reset_index()

africa.Country=africa.Country.astype('float64')
india.Station=india.Station.astype('float64')

euro.set_index('Date', inplace=True)
africa.set_index('Date', inplace=True)
india.set_index('Date', inplace=True)

india.sort_index()

"""# Standardizing values"""

from re import X
from sklearn.preprocessing import StandardScaler as SS 

SS11=SS()
SS12=SS()
SS21=SS()
SS22=SS()
SS31=SS()
SS32=SS()

X_euro=euro.drop('WQI', axis=1)
y_euro=euro['WQI']

SS11.fit(X_euro)
X_norm_euro=SS11.transform(X_euro)

#SS12.fit(y_euro.values.reshape(-1,1))
#y_norm_euro=SS12.transform(y_euro.values.reshape(-1,1))
y_norm_euro=y_euro

X_africa=africa.drop('WQI', axis=1)
y_africa=africa['WQI']

SS21.fit(X_africa)
X_norm_africa=SS21.transform(X_africa)

#SS22.fit(y_africa.values.reshape(-1,1))
#y_norm_africa=SS22.transform(y_africa.values.reshape(-1,1))
y_norm_africa=y_africa

X_india=india.drop('WQI', axis=1)
y_india=india['WQI']

SS31.fit(X_india)
X_norm_india=SS31.transform(X_india)

#SS32.fit(y_india.values.reshape(-1,1))
#y_norm_india=SS32.transform(y_india.values.reshape(-1,1))

y_norm_india=y_india

len(y_norm_india)*0.8

"""# Spliting Data in train and test data

"""

from sklearn.model_selection import train_test_split as tts

X_train_euro, X_test_euro, y_train_euro, y_test_euro=X_norm_euro[:4973],X_norm_euro[4974:],y_norm_euro[:4973],y_norm_euro[4974:]#tts(X_norm_euro,y_norm_euro,test_size=0.2)

X_train_africa, X_test_africa, y_train_africa, y_test_africa=X_norm_africa[:1313],X_norm_africa[1314:],y_norm_africa[:1313],y_norm_africa[1314:]#tts(X_norm_africa,y_norm_africa,test_size=0.2)

X_train_india, X_test_india, y_train_india, y_test_india=X_norm_india[:11001],X_norm_india[11002:],y_norm_india[:11001],y_norm_india[11002:]#tts(X_norm_india,y_norm_india,test_size=0.2)

X_train_india.shape

len(y_train_euro), len(y_test_euro), len(X_train_euro)

len(y_train_africa), len(y_test_africa), len(X_train_africa)

len(euro.columns)

#X_train, y_train = split_series(euro_train,n_past, n_future)
#euro_train=euro.iloc[:int(euro.shape[0]*0.8)]
#euro_test=euro.iloc[int(euro.shape[0]*0.8):]
X_train = np.reshape(X_train_euro,(X_train_euro.shape[0],X_train_euro.shape[1],1))
#y_train = y_train_euro.reshape((y_train_euro.shape[0], y_train_euro.shape[1], 1))
y_train=y_train_euro
#X_test, y_test = split_series(euro_test,n_past, 1)
X_test = X_test_euro.reshape((X_test_euro.shape[0], X_test_euro.shape[1],1))
#y_test = y_test_euro.reshape((y_test_euro.shape[0], y_test_euro.shape[1], 1))
y_test=y_test_euro

"""# Model Implementation"""

import tensorflow as tf
from tensorflow.keras.layers import Dense, LSTM, Conv1D, MaxPooling1D, TimeDistributed, Flatten, Dropout, RepeatVector

multivariate_cnn_lstm = tf.keras.models.Sequential([
    Conv1D(filters=50, kernel_size=2,
           strides=1, padding='causal',
           activation='relu', 
           input_shape=X_train.shape[-2:]),
    LSTM(100, return_sequences=True),
    #LSTM(50, return_sequences=True),
    Flatten(),
    Dense(50, activation='relu'),
    Dense(1)
])


optimizer = tf.keras.optimizers.Adam(lr=4e-3, amsgrad=True)

multivariate_cnn_lstm.compile(loss='mean_absolute_error',
                          optimizer=optimizer)

tf.keras.utils.plot_model(multivariate_cnn_lstm)

multivariate_cnn_lstm.fit(X_train,y_train,epochs=12, batch_size=16)

y_pred=multivariate_cnn_lstm.predict(X_test)

from sklearn.metrics import mean_absolute_error as mae

from sklearn.metrics import mean_squared_error as mse
y_pred=multivariate_cnn_lstm.predict(X_test)
mae(y_test.values.reshape(-1,1),y_pred), mse(y_test.values.reshape(-1,1),y_pred)

plt.figure(figsize=(20,6))
plt.plot(y_test.sort_index())
#plt.plot(y_pred)
plt.plot(pd.DataFrame(y_pred).set_index(y_test.sort_index().index))
plt.xlabel('Time')
plt.ylabel('WQI')
plt.title('Europe WQI Prediction')
#plt.yscale('log')
plt.show()





"""# Model for Africa"""

#X_train, y_train = split_series(euro_train,n_past, n_future)
X_train = np.reshape(X_train_africa,(X_train_africa.shape[0],X_train_africa.shape[1],1))
#y_train = y_train_africa.reshape((y_train_africa.shape[0], y_train_africa.shape[1], 1))
y_train=y_train_africa
#X_test, y_test = split_series(euro_test,n_past, 1)
X_test = X_test_africa.reshape((X_test_africa.shape[0], X_test_africa.shape[1],1))
#y_test = y_test_africa.reshape((y_test_africa.shape[0], y_test_africa.shape[1], 1))
y_test=y_test_africa

africa.tail(300)

import tensorflow as tf
from tensorflow.keras.layers import Dense, LSTM, Conv1D, MaxPooling1D, TimeDistributed, Flatten, Dropout, RepeatVector

multivariate_cnn_lstm = tf.keras.models.Sequential([
    LSTM(100, return_sequences=True),
    #LSTM(50, return_sequences=True),
    Flatten(),
    Dense(50, activation='relu'),
    Dense(1)
])


optimizer = tf.keras.optimizers.Adam(lr=4e-3, amsgrad=True)

multivariate_cnn_lstm.compile(loss='mean_absolute_error',
                          optimizer=optimizer)

tf.keras.utils.plot_model(multivariate_cnn_lstm)

multivariate_cnn_lstm.fit(X_train,y_train,epochs=20, batch_size=16)

from sklearn.metrics import mean_absolute_error as mae

from sklearn.metrics import mean_squared_error as mse
y_pred=multivariate_cnn_lstm.predict(X_test)
mae(y_test.values.reshape(-1,1),y_pred), mse(y_test.values.reshape(-1,1),y_pred)

plt.figure(figsize=(20,6))
plt.plot(y_test.sort_index())
#plt.plot(y_pred)
plt.plot(pd.DataFrame(y_pred).set_index(y_test.sort_index().index))
plt.xlabel('Time')
plt.ylabel('WQI')
plt.title('Africa WQI Prediction')
#plt.yscale('log')
plt.show()





"""# Model for India"""

#X_train, y_train = split_series(euro_train,n_past, n_future)
X_train = np.reshape(X_train_india,(X_train_india.shape[0],X_train_india.shape[1],1))
#y_train = y_train_india.reshape((y_train_india.shape[0], y_train_india.shape[1], 1))
y_train=y_train_india
#X_test, y_test = split_series(euro_test,n_past, 1)
X_test = X_test_india.reshape((X_test_india.shape[0], X_test_india.shape[1],1))
#y_test = y_test_india.reshape((y_test_india.shape[0], y_test_india.shape[1], 1))
y_test=y_test_india

import tensorflow as tf
from tensorflow.keras.layers import Dense, LSTM, Conv1D, MaxPooling1D, TimeDistributed, Flatten, Dropout, RepeatVector

multivariate_cnn_lstm = tf.keras.models.Sequential([
    LSTM(100, return_sequences=True),
    #LSTM(50, return_sequences=True),
    Flatten(),
    Dense(50, activation='relu'),
    Dense(1)
])


optimizer = tf.keras.optimizers.Adam(lr=4e-3, amsgrad=True)

multivariate_cnn_lstm.compile(loss='mean_absolute_error',
                          optimizer=optimizer)

multivariate_cnn_lstm.fit(X_train,y_train,epochs=20, batch_size=16)

from sklearn.metrics import mean_absolute_error as mae

from sklearn.metrics import mean_squared_error as mse
y_pred=multivariate_cnn_lstm.predict(X_test)
mae(y_test.values.reshape(-1,1),y_pred), mse(y_test.values.reshape(-1,1),y_pred)

plt.figure(figsize=(20,6))
plt.plot(y_test.sort_index())
#plt.plot(y_pred)
plt.plot(pd.DataFrame(y_pred).set_index(y_test.sort_index().index))
plt.xlabel('Time')
plt.ylabel('WQI')
plt.title('India WQI Prediction')
#plt.yscale('log')
plt.show()









"""# Model for Data combined"""

euro.rename(columns={'country':'Country'}, inplace=True)

india['Country']='india'
africa['Country']='africa'
euro['Country']='euro'

#india[euro.columns[euro.columns.isin(india.columns)].values]

df_merged=pd.concat([euro[euro.columns[euro.columns.isin(india.columns)].values],
           africa[euro.columns[euro.columns.isin(india.columns)].values],
           india[euro.columns[euro.columns.isin(india.columns)].values]])

df_merged.set_index('Date', inplace=True)

df_merged['Country']=lec.fit_transform(df_merged['Country'])

df_merged.WQI.unique()

new=df_merged.reset_index().groupby(['Date','Country']).mean().reset_index()

#sorted(new.WQI.unique())

from sklearn.preprocessing import StandardScaler as SS 

SS11=SS()
SS12=SS()

new.Country=new.Country.astype('float64')

new.set_index('Date',inplace=True)

new.sort_values('Date', inplace=True)

X_euro=new.drop('WQI', axis=1)
y_euro=new['WQI']

#SS11.fit(X_euro)
#X_norm_euro=SS11.transform(X_euro)
X_norm_euro=X_euro
#SS12.fit(y_euro.values.reshape(-1,1))
#y_norm_euro=SS12.transform(y_euro.values.reshape(-1,1))
y_norm_euro=y_euro



from sklearn.model_selection import train_test_split as tts

X_train_euro, X_test_euro, y_train_euro, y_test_euro=tts(X_norm_euro.values,y_norm_euro,test_size=0.2)

X_train_euro=X_norm_euro[:1300].values
X_test_euro=X_norm_euro[1301:].values
y_train_euro=y_norm_euro[:1300]
y_test_euro=y_norm_euro[1301:]

X_train=np.reshape(X_train_euro,(X_train_euro.shape[0],X_train_euro.shape[1],1))
X_test=np.reshape(X_test_euro,(X_test_euro.shape[0],X_test_euro.shape[1],1))

y_euro.shape

import tensorflow as tf
from tensorflow.keras.layers import Dense, LSTM, Conv1D, MaxPooling1D, TimeDistributed, Flatten, Dropout, RepeatVector

multivariate_cnn_lstm = tf.keras.models.Sequential([

    Conv1D(32, 3, activation='relu'),                                                    
    LSTM(100, return_sequences=True),
    LSTM(50, return_sequences=True),
    LSTM(25),
    Dense(50, activation='relu'),
    Dense(1)
])


optimizer = tf.keras.optimizers.Adam(lr=4e-3, amsgrad=True)

multivariate_cnn_lstm.compile(loss='mean_absolute_error',
                          optimizer=optimizer)

multivariate_cnn_lstm.fit(X_train,y_train_euro,epochs=20, batch_size=8)

from sklearn.metrics import mean_absolute_error as mae
from sklearn.metrics import mean_squared_error as mse

y_pred=multivariate_cnn_lstm.predict(X_test)

mae(y_test_euro.values.reshape(-1,1),y_pred), mse(y_test_euro.values.reshape(-1,1),y_pred)

import matplotlib.pyplot as plt

plt.figure(figsize=(20,6))
plt.plot(y_test_euro.sort_index())
plt.plot(pd.DataFrame(y_pred).set_index(y_test_euro.sort_index().index))
plt.xlabel('Time')
plt.ylabel('WQI')
#plt.yscale('log')
plt.show()

