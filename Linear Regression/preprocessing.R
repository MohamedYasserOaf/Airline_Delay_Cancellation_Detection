#Environment
rm(list=ls())
setwd("C:/Users/Mazen/Desktop/big data/Project")
library(data.table)
#2018 PRE-PROCESSING
regressionDataset18 <- read.csv("Dataset/2018.csv")
regressionDataset18 = subset(regressionDataset18, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME, ORIGIN, DEST))
regressionDataset18 <- na.omit(object=regressionDataset18)
#2017 PRE-PROCESSING
regressionDataset17 <- read.csv("Dataset/2017.csv")
regressionDataset17 = subset(regressionDataset17, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset17 <- na.omit(object=regressionDataset17)
#2016 PRE-PROCESSING
regressionDataset16 <- read.csv("Dataset/2016.csv")
regressionDataset16 = subset(regressionDataset16, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset16 <- na.omit(object=regressionDataset16)
#2015 PRE-PROCESSING
regressionDataset15 <- read.csv("Dataset/2015.csv")
regressionDataset15 = subset(regressionDataset15, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset15 <- na.omit(object=regressionDataset15)
#2014 PRE-PROCESSING
regressionDataset14 <- read.csv("Dataset/2014.csv")
regressionDataset14 = subset(regressionDataset14, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset14 <- na.omit(object=regressionDataset14)
#2013 PRE-PROCESSING
regressionDataset13 <- read.csv("Dataset/2013.csv")
regressionDataset13 = subset(regressionDataset13, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset13 <- na.omit(object=regressionDataset13)
#2012 PRE-PROCESSING
regressionDataset12 <- read.csv("Dataset/2012.csv")
regressionDataset12 = subset(regressionDataset12, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset12 <- na.omit(object=regressionDataset12)
#2011 PRE-PROCESSING
regressionDataset11 <- read.csv("Dataset/2011.csv")
regressionDataset11 = subset(regressionDataset11, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset11 <- na.omit(object=regressionDataset11)
#2010 PRE-PROCESSING
regressionDataset10 <- read.csv("Dataset/2010.csv")
regressionDataset10 = subset(regressionDataset10, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset10 <- na.omit(object=regressionDataset10)
#2009 PRE-PROCESSING
regressionDataset9 <- read.csv("Dataset/2009.csv")
regressionDataset9 = subset(regressionDataset9, select=-c(Unnamed..27, CANCELLATION_CODE, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, CANCELLED, DIVERTED, DEP_TIME, ARR_TIME, TAXI_OUT, TAXI_IN, AIR_TIME, CRS_DEP_TIME, WHEELS_OFF, WHEELS_ON, CRS_ARR_TIME, ACTUAL_ELAPSED_TIME))
regressionDataset9 <- na.omit(object=regressionDataset9)
#FULL DATASET (TRANSFORMATIONS AND SCALING)
fullRegressionDataset <- rbind(regressionDataset18)
#fullRegressionDataset <- na.omit(object=fullRegressionDataset)
#fullRegressionDataset <- head(fullRegressionDataset, 100000)
fullRegressionDataset$FL_DATE <- as.Date(fullRegressionDataset$FL_DATE)
fullRegressionDataset <- transform(fullRegressionDataset, FL_DAY = format(FL_DATE, "%d"), FL_MONTH = format(FL_DATE, "%m"), FL_YEAR = format(FL_DATE, "%Y"))
fullRegressionDataset <- subset(fullRegressionDataset, select=-c(FL_DATE, FL_YEAR, FL_MONTH, OP_CARRIER_FL_NUM))
#fullRegressionDataset$FL_YEAR <- as.numeric(fullRegressionDataset$FL_YEAR)
fullRegressionDataset$DISTANCE <- scale(fullRegressionDataset$DISTANCE)
fullRegressionDataset$CRS_ELAPSED_TIME <- scale(fullRegressionDataset$CRS_ELAPSED_TIME)
fullRegressionDataset$DEP_DELAY <- scale(fullRegressionDataset$DEP_DELAY)
#fullRegressionDataset$OP_CARRIER_FL_NUM <- scale(fullRegressionDataset$OP_CARRIER_FL_NUM)
#fullRegressionDataset$FL_YEAR <- scale(fullRegressionDataset$FL_YEAR)
fullRegressionDataset$FL_DAY <- scale(as.numeric(fullRegressionDataset$FL_DAY))
#fullRegressionDataset$FL_MONTH <- scale(as.numeric(fullRegressionDataset$FL_MONTH))
set.seed(123)
split<- sample(c(rep(0, 0.7 * nrow(fullRegressionDataset)), rep(1, 0.3 * nrow(fullRegressionDataset))))
fullRegressionDatasetTrain <- fullRegressionDataset[split==0, ]     
fullRegressionDatasetTest <- fullRegressionDataset[split==1, ]     
write.table(fullRegressionDatasetTrain,"Dataset/regressionDatasetTrain.csv", sep=",", row.names=FALSE, col.names=FALSE)
write.table(fullRegressionDatasetTest,"Dataset/regressionDatasetTest.csv", sep=",", row.names=FALSE, col.names=FALSE)




