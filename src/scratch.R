# 2021/02/23 MPO ----------------------------------------------------------

library(dplyr)
library(data.table)
library(ggplot2)
library(keyring)

# Import ------------------------------------------------------------------


#add Stop Ridership Col Names
Stop_Ridership_col_names <- c("Stop Number", "Stop Name", "Total Boardings", "Average Daily Boardings","Total Alightings","Average Daily Alightings","Days")


# Import ------------------------------------------------------------------


# db connection

keyring_unlock("con_dw")
con_dw <- DBI::dbConnect(odbc::odbc()
                         , Driver = key_get("Driver", keyring = "con_dw")
                         , Server = key_get("Server", keyring = "con_dw")
                         , Database = key_get("Database", keyring = "con_dw")
                         , Port = key_get("Port", keyring = "con_dw")
                         )
keyring_lock("con_dw")

# get applicable dates

DimServiceLevel <- tbl(con_dw,"DimServiceLevel") %>% 
  collect() %>% 
  data.table(key = "ServiceLevelKey")

DimDate_start <- tbl(con_dw, "DimDate") %>%
  filter(CalendarDateChar == "08/06/2021") %>%
  collect() %>% 
  setDT(key = "DateKey")

DimDate_end <- tbl(con_dw, "DimDate") %>%
  filter(CalendarDateChar == "08/07/2021") %>%
  collect() %>% 
  setDT(key = "DateKey")


DimDate_full <- tbl(con_dw, "DimDate") %>%
  collect() %>% 
  setDT(key = "DateKey")

# get stops
DimStop <- tbl(con_dw,"DimStop") %>% 
  collect() %>% 
  setDT(key = "StopKey")

# get farekey

DimFare <- tbl(con_dw, "DimFare") %>%
  collect() %>%
  setDT(key = "FareKey")

# get Routes
DimRoute <- tbl(con_dw,"DimRoute") %>%
  filter(RouteReportLabel == "8") %>%
  collect() %>% 
  setDT(key = "RouteKey")

# get Vehicles
DimVehicle <- tbl(con_dw, "DimVehicle") %>% 
  collect() %>% 
  setDT(key = "VehicleKey")

# get Trips
DimTrip <- tbl(con_dw, "DimTrip") %>% 
  collect() %>% 
  setDT(key = "TripKey")

# get FF boarding and alighting

FactFare_raw <- tbl(con_dw, "FactFare") %>%
  filter(
    FareKey %in% c(1001, 1002, 1003)
    , DateKey >= local(DimDate_start$DateKey)
    , DateKey <= local(DimDate_end$DateKey)
    , RouteKey %in% local(DimRoute$RouteKey)
    ) %>% #end filter
  collect() %>% 
  setDT()


#join servicelevel and a bit of stop info and dimfare while we're here

#mget for memory efficiency, keeps location the same
FactFare_raw[
  #skinny stops first
  DimStop[,.(StopKey,StopID,StopDesc,Latitude,Longitude)]
  ,on = "StopKey"
  ,names(DimStop[,.(StopKey,StopID,StopDesc,Latitude,Longitude)]) := 
    mget(paste0("i.",names(DimStop[,.(StopKey,StopID,StopDesc,Latitude,Longitude)])))
  ][
    #then service level skinny
    DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]
    ,on = "ServiceLevelKey"
    ,names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)]) := 
      mget(paste0("i.",names(DimServiceLevel[,.(ServiceLevelKey,ServiceLevelReportLabel)])))
    ][
      #then dimfareskinny
      DimFare[,.(FareKey,FareReportLabel)]
      , on = "FareKey"
      ,names(DimFare[,.(FareKey,FareReportLabel)]) := mget(paste0("i.",names(DimFare[,.(FareKey,FareReportLabel)])))
      ][
        #then dimroute
        DimRoute[,.(RouteKey,RouteReportLabel)]
        , on = "RouteKey"
        ,names(DimRoute[,.(RouteKey,RouteReportLabel)]) := mget(paste0("i.",names(DimRoute[,.(RouteKey,RouteReportLabel)])))
        ][,Service_Type := sub(".*-","",ServiceLevelReportLabel)
          ][
            DimDate_full
            , on = "DateKey"
            ,names(DimDate_full[,.(DateKey,CalendarDate)]) := mget(paste0("i.",names(DimDate_full[,.(DateKey,CalendarDate)])))
            ][
              DimVehicle[,.(VehicleKey,VehicleReportLabel)]
              , on = "VehicleKey"
              ,names(DimVehicle[,.(VehicleKey,VehicleReportLabel)]) := 
                mget(paste0("i.",names(DimVehicle[,.(VehicleKey,VehicleReportLabel)])))
              ][
                DimTrip[,.(TripKey,TripFareboxID,StartTime24Desc,EndTime24Desc)]
                , on = "TripKey"
                ,names(DimTrip[,.(TripKey,TripFareboxID,StartTime24Desc,EndTime24Desc)]) := 
                  mget(paste0("i.",names(DimTrip[,.(TripKey,TripFareboxID,StartTime24Desc,EndTime24Desc)])))
                ]



# date tests --------------------------------------------------------------


calendar <- seq.Date(as.IDate(DimDate_start[,CalendarDate])
                     ,as.IDate(DimDate_end[,CalendarDate])
                     ,"day")

all(
  calendar ==
    sort(
      unique(
        #throw your dataframe$datecolumn or whatever in here
        FactFare_raw[,unique(CalendarDate)]
        
      )#end unique
    )#end sort
)#end all

fsetdiff(data.table(calendar)
         , FactFare_raw[,.(calendar = lubridate::as_date(CalendarDate))]
         )


# transit day -------------------------------------------------------------

FactFare_raw[
  , c("ClockTime","Date") := list(as.ITime(stringr::str_sub(ServiceDateTime
                                                            , 12
                                                            , 19
                                                            )
                                           )
                                  , as.IDate(stringr::str_sub(ServiceDateTime
                                                              , 1
                                                              , 10
                                                              )
                                             )
                                  )
  ][
    #label prev day or current
    , DateTest := ifelse(ClockTime<as.ITime("03:30:00"),1,0)
    ][
      , Transit_Day := fifelse(
        DateTest ==1
        ,lubridate::as_date(Date)-1
        ,lubridate::as_date(Date)
        )#end fifelse
      ][
        ,Transit_Day := lubridate::as_date("1970-01-01")+
          lubridate::days(Transit_Day)
        ]


FactFare_raw[Transit_Day == "2021-08-06"]

FactFare_raw[FareReportLabel == "On Board"][,.N,FareCount][order(FareCount)]
FactFare_raw[FareReportLabel == "On Board"
             ][FareCount > 30
               ][,.(FareCount
                    , ServiceDateTime
                    , FareCount
                    , StopID
                    , StopDesc
                    , Direction
                    , VehicleReportLabel 
                    , StartTime24Desc
                    , EndTime24Desc
                    , Latitude
                    , Longitude)
                 ][,.N,.(StopID,StopDesc)] 


x <- dcast(
  FactFare_raw[Transit_Day == "2021-08-06"]
  , ServiceDateTime + 
    StartTime24Desc + 
    EndTime24Desc + 
    StopID + 
    StopDesc + 
    Direction + 
    VehicleReportLabel + 
    Latitude + 
    Longitude +
    TripFareboxID
    ~ FareReportLabel
  , value.var = "FareCount"
  )[order(VehicleReportLabel,Direction,ServiceDateTime)]


x[,.(Boardings = sum(Boarding)
     , Alightings = sum(Alighting)
     )
  ,TripFareboxID
  ][,diff := Boardings - Alightings][]



