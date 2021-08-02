# okay couple of steps here
# step 1. get stops from spreadsheet
# step 2. get all routes to those stops during time period
# step 3. read in boardings for every route for all stops
# step 4. get transit_day, route, stop_id, n, uniquen trips
# 



library(magrittr)
library(data.table)


# import ------------------------------------------------------------------
con_rep <- DBI::dbConnect(odbc::odbc()
                          , Driver = "SQL Server"
                          , Server = "REPSQLP01VW"
                          , Database = "TransitAuthority_IndyGo_Reporting"
                          , Port = 1433
                          )
# set dates
startdate <- as.IDate("2019-09-01")
enddate <- as.IDate("2020-03-14")

vmh_startdate <- stringr::str_remove_all(startdate, "-")
vmh_enddate <- stringr::str_remove_all(enddate, "-")

# read stops from spreadsheet

stops_reference <- data.table::fread("data//raw//202106_Stop_Removal_Stops.csv")
        
# get all routes to those stops

vmh_stops <- dplyr::tbl(
  con_rep
  ,dplyr::sql(
    paste0("select distinct a.Route
           , a.Stop_Id
           
           from avl.Vehicle_Message_History a (nolock)
           
           where a.Time > '", vmh_startdate, "'
           and a.Time < DATEADD(day, 1, '", vmh_enddate,"')
           and a.Stop_Id in ("
           , stops_reference[,paste0(stop_id, collapse = ",")],
           ")
           
           group by a.Route
           , a.Stop_Id
           "
           )
    )
  ) %>%
  dplyr::collect() %>%
  data.table::data.table()

# get all data for those routes

vmh_raw <- dplyr::tbl(
  con_rep,dplyr::sql(
    paste0(
    "select a.Time
    ,a.Route
    ,Boards
    ,Alights
    ,Trip
    ,Vehicle_ID
    ,Stop_Name
    ,Stop_Id
    ,Inbound_Outbound
    ,Departure_Time
    ,Latitude
    ,Longitude
    ,GPSStatus
    ,CommStatus
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Route in (",vmh_stops[,paste0(Route, collapse = ",")],")
    and a.Time > '20190901'
    and a.Time < DATEADD(day,1,'20200314')"
    )
    )
  ) %>%
  dplyr::collect() %>%
  data.table::data.table()

# do transit_day

transit_day_cutoff <- as.ITime("03:30:00")

vmh_raw[
  , DateTest := fifelse(data.table::as.ITime(Time) < transit_day_cutoff
                        , 1
                        , 0
                        ) #end fifelse()
  ][
    , Transit_Day := fifelse(DateTest == 1
                             ,data.table::as.IDate(Time)-1
                             ,data.table::as.IDate(Time)
                             )
    ]

# do service type
# get holidays names
holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

# get holidays dates set sat sun
holidays_sunday <- timeDate::holiday(2000:2050, holidays_sunday_service)
holidays_saturday <- timeDate::holiday(2000:2050, holidays_saturday_service)


# set service type column
vmh_raw[
  , Service_Type := fcase(Transit_Day %in% as.IDate(holidays_saturday@Data)
                          , "Saturday"
                          , Transit_Day %in% as.IDate(holidays_sunday@Data)
                          , "Sunday"
                          , weekdays(Transit_Day) %in% c("Monday"
                                                         , "Tuesday"
                                                         , "Wednesday"
                                                         , "Thursday"
                                                         , "Friday"
                                                         )
                          , "Weekday"
                          , weekdays(Transit_Day) == "Saturday"
                          , "Saturday"
                          , weekdays(Transit_Day) == "Sunday"
                          , "Sunday"
                          )#end fcase 
  ]

# lets clean out the zero days?

con_sb <- DBI::dbConnect(odbc::odbc()
                         , Driver = "SQL Server"
                         , Server = "REPSQLP01VW"
                         , Database = "StrategicPlanningSandbox"
                         , Port = 1433
                         )

dplyr::tbl(con_sb,"Zero_B_A_Transit_Day") %>%
  dplyr::filter(Transit_Day  >= vmh_startdate
                , Transit_Day < vmh_enddate) %>%
  dplyr::collect() %>%
  data.table() %>%
  




# okay, i think the first thing we do is get averages and see what, uh,
# what that looks like...

# need days appearing by stop and route

unique_days_per_stoproute <- vmh_raw[, uniqueN(Transit_Day)
                                     , .(Route
                                         , Stop_Id
                                         )
                                     ]


vmh_raw[, .N, .(Transit_Day, Route, Stop_Id)
        ][
          order(Route, Stop_Id, Transit_Day)
          ][, `:=` (days_to_next = shift(Transit_Day,-1)-Transit_Day
                    , N_Difference = shift(N,-1)-N
                    
                    )
            , .(Route, Stop_Id)
            ]
