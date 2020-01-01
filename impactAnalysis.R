# These dataset is a result of cluster analysis and over/undersampling. I do the analysis of determinants
# using  WOE (Weight of Evidence), IV (Information Value), Boruta and Logistic Regression (inference)
rm(list = ls())
library(data.table)
library(tidyverse)
library(dataPreparation)
library(Information)
library(Boruta)
library(corrplot)
library(logistf)

d <- fread("C:\\Users\\ear\\result\\dtClusterEnd.csv", sep = ';', dec = ',') %>% as.data.frame()

normalize <- function(x) (x - min(x, na.rm = T))/(max(x, na.rm = T) - min(x, na.rm = T))
desnormalize <- function(x, y) (y*(max(x, na.rm = T) - min(x, na.rm = T)) + min(x, na.rm = T))

d2 <- d %>% select(-store, -date, -prediction)

for(i in 2:dim(d2)[2]){
  d2[,i] <- normalize(d2[,i])
}
rm(i)

d2 <- d2 %>% mutate(PI_cat = as.integer(ifelse(PI_cat == 'Adequate', 0, 1)))

d2constantCols <- whichAreConstant(d2)#Check if there is constant variable
d2doubleCols <- whichAreInDouble(d2)#Check if there is duplicate variable
d2bijectionsCols <- whichAreBijection(d2)#Check if there is bijective variable

#*********************** WOE/IV ********************************#
IV <- create_infotables(data = d2, y = 'PI_cat', bins = 10, parallel = TRUE)
IV_Value <- data.frame(IV$Summary)

plot_infotables(IV, IV$Summary$Variable[1:3], same_scale = FALSE)

IV_Value <- IV_Value %>% 
  mutate(criterion = ifelse(IV < 0.02, 'Sem Utilidade para Predição',
                            ifelse(IV >= 0.02 & IV < 0.1, 'Poder Preditivo Fraco',
                                   ifelse(IV >= 0.1 & IV < 0.3, 'Poder Preditivo Médio',
                                          ifelse(IV >= 0.3 & IV < 0.5, 'Poder Preditivo Alto',
                                                 'Poder Preditivo Suspeito')))))

#*********************** Correlation Matrix ********************************#
correlations <- cor(d2[,2:18]) %>% as.data.frame()
corrplot(correlations, method = 'circle')
fwrite(correlations, "C:\\Users\\ear\\result\\CorMatrix.csv", sep = ';', dec = ',', row.names = T)

#*********************** BORUTA ********************************#
borutaTrain <- Boruta(PI_cat~., data = na.omit(d2), doTrace = 2)
boruta_signif <- names(borutaTrain$finalDecision[borutaTrain$finalDecision %in% 
                                                   c('Confirmed', 'Tentative')])
#print(boruta_signif)
#plot(borutaTrain, cex.axis = 0.7, las = 2, xlab = '', main = 'Variable Importance')
View(borutaTrain$ImpHistory)
boruta.df <- attStats(borutaTrain)
fwrite(as.data.frame(boruta.df), "C:\\Users\\ear\\result\\boruta.csv", sep = ';', dec = ',',
       row.names = T)

#*********************** Firth logistic regression ********************************#
# Credit: https://www.r-bloggers.com/example-8-15-firth-logistic-regression/
# In logistic regression, when the outcome has low (or high) prevalence, or when there are several 
# interacted categorical predictors, it can happen that for some combination of the predictors, all the 
# observations have the same event status. A similar event occurs when continuous covariates predict the 
# outcome too perfectly. This phenomenon, known as "separation" (including complete and quasi-complete 
# separation) will cause problems fitting the model. Sometimes the only symptom of separation will be 
# extremely large standard errors, while at other times the software may report an error or a warning.
fit <- logistf(as.factor(PI_cat)~., data = d2, family = 'binomial')
oddsLogis <- as.data.frame(exp(fit$coefficients))
colnames(oddsLogis) <- c('Odds')
fwrite(oddsLogis,"C:\\Users\\ear\\result\\logistic.csv", sep = ';', dec = ',',
       row.names = T)

# Well, now I finish the impact analysis of determinants in poor performance of drogstores #
# Next step is to put the information in dashboard Power BI                                #
