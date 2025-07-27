import numpy as np 


# Exponential Moving Average --> volatility, significant deviation from the EMA

# Interquartile Range (IQR): Range between 1st and 3rd quartiles --> high deviations in high freq data

# Mahalnobis: distance from the mean --> detect multivariate anomalies in high freq data

# Local Outlier Factor (LOF): measure local deviation with respect to its neighbors

# Isolation Forest: Unsupervised learning algo, random decision tree --> isolate anomalies in the data

# Potential typo in document
# Earth Mover's Distance (EMD): Measure discrepancy between two data distributions and can be implemented using numpy

# Kolmogorov-Smirnov (KS) Distance: Like EMD, measure discrepancy between two data distribtuions

# ICSS Test: Detect changes in variance (Theta) -- relevant in high-freq trading

# Bayesian Structural time Series (BSTS): Indeityf unexpected fluctuations --> local trends, seasonal patterns, and external regressors. 

# Grubbs' Test: detect outliers in a univariate dataset -- applicable to OHLC data --> volatility, identify extreme values

# One Class Support Vector Machine (OCSVM); Unsupervised Learning Algo that constructs a hyperplane to seperate Normal data points 
# --> points outside the hyperplane are potential anomalies