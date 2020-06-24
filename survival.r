# library and set working directory ---------------------------------------
library(RODBC)
library(dplyr)
library(svDialogs)
library(zoo)
setwd(choose.dir())


# Input -------------------------------------------------------------------
###### connect ODBC ######
myconn <-odbcConnect("ICAAP",
                     uid = dlg_input("UID: ", "ICAAP")$res, 
                     pwd = dlg_input("PASSWORD: ")$res, 
                     believeNRows=FALSE)

###### Input confidence interval ######
conf_lvl <- dlg_input(
  "Confidence level: ",
  "0.99"
)$res %>% as.numeric()


###### Import segment ######
td_study <- as.data.frame(sqlQuery(myconn, "select * from td_study"))

td_test <- as.data.frame(sqlQuery(myconn, "select * from td_test"))

###### format runoff ######
names(td_study) <- c("age", "total_bal", "drawdown")
td_study <- td_study[order(td_study$age),]
td_study$drawdown[is.na(td_study$drawdown)] <- 0


names(td_test) <- c("age", "total_bal", "drawdown")
td_test <- td_test[order(td_test$age),]
td_test$drawdown[is.na(td_test$drawdown)] <- 0




# Study Survival ----------------------------------------------------------
##### survival curve calculation
svv_rate_study <- 1-td_study$drawdown[2:nrow(td_study)]/td_study$total_bal[1:(nrow(td_study)-1)]
svv_rate_study <- sapply(1:length(svv_rate_study), function(x){
  prod(svv_rate_study[1:x])
})


### rate_tbl table
rate_tbl <- cbind.data.frame(td_study$age[2:nrow(td_study)], svv_rate_study)
names(rate_tbl) <- c("age", "svv_rate_study")

##### confidence interval calculation
rate_tbl$conf_int <- td_study$drawdown[2:nrow(td_study)]/
  td_study$total_bal[1:(nrow(td_study)-1)]/(
    td_study$total_bal[1:(nrow(td_study)-1)]-td_study$drawdown[2:nrow(td_study)]
  )
rate_tbl$conf_int <- sapply(1:nrow(rate_tbl), function(x){
  sum(rate_tbl$conf_int[1:x])
})

rate_tbl$conf_int <- qnorm(conf_lvl)*rate_tbl$svv_rate_study*sqrt(rate_tbl$conf_int)

##### upper + lower curve
rate_tbl$svv_rate_up_study <- rate_tbl$svv_rate_study + rate_tbl$conf_int
rate_tbl$svv_rate_low_study <- rate_tbl$svv_rate_study - rate_tbl$conf_int

##### CDR
rate_tbl$CDR_study <- 
  td_study$drawdown[2:nrow(td_study)]/td_study$total_bal[1:(nrow(td_study)-1)]






# Test Survival -----------------------------------------------------------

##### survival curve calculation
svv_rate_test <- 1-td_test$drawdown[2:nrow(td_test)]/td_test$total_bal[1:(nrow(td_test)-1)]
svv_rate_test <- sapply(1:length(svv_rate_test), function(x){
  prod(svv_rate_test[1:x])
})

##### rate_tbl_test table
rate_tbl_test <- cbind.data.frame(td_test$age[2:nrow(td_test)], svv_rate_test)
names(rate_tbl_test) <- c("age", "svv_rate_test")

##### confidence interval calculation
rate_tbl_test$conf_int <- td_test$drawdown[2:nrow(td_test)]/
  td_test$total_bal[1:(nrow(td_test)-1)]/(
    td_test$total_bal[1:(nrow(td_test)-1)]-td_test$drawdown[2:nrow(td_test)]
  )
rate_tbl_test$conf_int <- sapply(1:nrow(rate_tbl_test), function(x){
  sum(rate_tbl_test$conf_int[1:x])
})
rate_tbl_test$conf_int <- qnorm(conf_lvl)*rate_tbl_test$svv_rate_test*sqrt(rate_tbl_test$conf_int)

##### upper + lower curve
rate_tbl_test$svv_rate_up_test <- rate_tbl_test$svv_rate_test + rate_tbl_test$conf_int
rate_tbl_test$svv_rate_low_test <- rate_tbl_test$svv_rate_test - rate_tbl_test$conf_int

##### CDR
rate_tbl_test$CDR_test <- 
  td_test$drawdown[2:nrow(td_test)]/td_test$total_bal[1:(nrow(td_test)-1)]



# Final Table -------------------------------------------------------------

##### join 2 table study + test
final <- merge(rate_tbl[-3], rate_tbl_test[-3], by = 1, all = T)

##### forward fill missing value
final[-c(5,9)] <- sapply(final[-c(5,9)], function(x){
  na.locf(x)
})%>%as.data.frame()

##### missing value of CDR = 0
final[ which(is.na(final[5])) ,5] <- 0
final[ which(is.na(final[9])) ,9] <- 0


##### backtest
final$backtest <- final$svv_rate_low_study <= final$svv_rate_up_test

##### calibration
final$CDR_calib <- final$CDR_study*final$backtest + final$CDR_test*(1-final$backtest)


final$svv_calib <- cumprod(1-final$CDR_calib)

write.csv(final, choose.files())






