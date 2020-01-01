# In this scripting I used a dataset that contains cost and revenue data from 50 drugstores.
# I used the K-Means algorithm to find 3 cluster that group these drugstore by similarity. There are
# drugstores with low and hight costs. The goal is to investigate the determinants that impact
# the hight cost and poor performance. I used the result of K-Means and I caught the two cluster with 
# greater dissimilaty, that is, create a dataset with data from drugstores with good performance and 
# low costs and drugstores with poor performance and hight cost. I finally used under and oversampling
# techniques to balanced the dataset.

#****************** Diagnostic Analysis **********************************#
rm(list = ls())
library(data.table)
library(tidyverse)
library(sparklyr)
library(ROSE)

d <- fread("C::\\Users\\ear\\dataDrugstore.csv", dec = ',') %>% as.data.frame()
d2 <- d %>% as.data.frame()

normalize <- function(x) (x - min(x, na.rm = T))/(max(x, na.rm = T) - min(x, na.rm = T))
desnormalize <- function(x, y) (y*(max(x, na.rm = T) - min(x, na.rm = T)) + min(x, na.rm = T))

for(i in 6:dim(d2)[2]){
  d2[,i] <- normalize(d2[,i])
}
rm(i)

d3 <- d2 %>% select(6:22)

#**** Find K-Means model with spark.Kmeans
#spark_install()
Sys.setenv(SPARK_HOME = 'C:/Users/ear/AppData/Local/spark/spark-2.4.3-bin-hadoop2.7')
sc <- spark_connect(master = 'local')
dt <- copy_to(sc, d3, 'd3_spark', overwrite = 'TRUE')

#class(dt), src_tbls(sc)
nomes <- colnames(dt)
cluster <- dt %>% ml_kmeans(k = 3, max_iter = 100, features = nomes, seed = 123456789)
pred <- ml_predict(cluster, dt) %>% collect()
pred2 <- cbind.data.frame(d[,1:5], pred)

for(i in 6:dim(d)[2]){
  pred2[,i] <- desnormalize(d[,i], pred2[,i])
}
rm(i)

pred2 %>% group_by(PI_cat, prediction) %>% summarise_all(funs(mean)) %>% 
  fwrite('C:\\Users\\ear\\result\\clusterResult1.csv', sep = ';', dec = ',')

ggplot(pred2, aes(x = V1, y = V13, color = prediction)) +
  geom_point() + scale_color_gradient(low = 'blue', high = 'red')

spark_disconnect(sc)#Disconnect from Spark

#************* undersampling and oversampling to balanced the dataset ************************#
dClust <- pred2 %>% filter((PI_cat == 'Adequate' & prediction == 2) |
                             (PI_cat == 'Inadequate' & prediction == 2))
dClustRose <- dClust %>% mutate(PI_cat = as.integer(ifelse(PI_cat == 'Adequate', 0, 1))) %>% 
  select(-UF, -region, -features)

n = dClustRose %>% filter(PI_cat == 1) %>% nrow()

dClustBalanced <- ovun.sample(PI_cat~., data = dClustRose, N = n*3, p = 0.5, seed = 123456789,
                              method = 'both')$data
dClustBalanced <- dClustBalanced %>% mutate(PI_cat = as.factor(ifelse(PI_cat == 0, 'Adequate',
                                                                      'Inadequate')))
dClustBalanced %>% group_by(PI_cat) %>% summarise_all(funs(mean)) %>% 
  fwrite('C:\\Users\\ear\\result\\dtClusterRose.csv', sep = ';', dec = ',')

fwrite(dClustBalanced, 'C:\\Users\\ear\\result\\dtClusterEnd.csv')