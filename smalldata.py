import pandas as pd
data = pd.read_csv('nyc_parking_violation/2016.csv',nrows=50000)
data.to_csv('small_data1.csv')
