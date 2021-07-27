
# add_transit_day -------------------------------------------------------------

#' Add a new column with transit_day 
#'
#'@param x An object coercable to a data.table
#'

add_transit_day <- function(x,cutoff_time){
  dt <- as.data.table()
  
  Transit_Day_Cutoff <- as.ITime("03:30:00")
  
  

  
  dt[
    , DateTest := fifelse(data.table::as.ITime(Time) < Transit_Day_Cutoff
                           , 1
                           , 0
                           ) #end fifelse()
    ][
      , Transit_Day := fifelse(DateTest == 1
                               ,data.table::as.IDate(Time)-1
                               ,data.table::as.IDate(Time)
                               )
      ]
  
}
