# okay couple of steps here
# step 1. get stops from spreadsheet
# step 2. get all routes to those stops during time period
# step 3. read in boardings for every route for all stops
# step 4. get transit_day, route, stop_id, n, uniquen trips
# 



library(magrittr)
library(data.table)


con_rep <- DBI::dbConnect(odbc::odbc()
                          , Driver = "SQL Server"
                          , Server = "REPSQLP01VW"
                          , Database = "TransitAuthority_IndyGo_Reporting"
                          , Port = 1433
                          )





VMH_Raw <- dplyr::tbl(
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
    where a.Route in (19,901,4,28,31)
    and a.Time > '20190901'
    and a.Time < DATEADD(day,1,'20200314')"
    )
    )
  ) %>%
  dplyr::collect() %>%
  data.table()

VMH_Raw[
  #add clocktime
  , c("ClockTime", "Date") := list(stringr::str_sub(Time, 12, 19)
                                  , stringr::str_sub(Time, 1, 10)
                                  )
][
  
  , DateTest := ifelse(ClockTime < "03:30:00"
                       , 1
                       , 0
                       )
][, Transit_Day := ifelse(DateTest == 1
                        , lubridate::as_date(Date) - 1
                        , lubridate::as_date(Date))
][
  , Transit_Day := lubridate::as_date("1970-01-01") +
    lubridate::days(Transit_Day)
]

#okay so this will get raw records
VMH_Raw[, .N, .(Transit_Day, Route, Stop_Id)
        ][
          order(Route, Stop_Id, Transit_Day)
          ][]

VMH_Raw[, .(Boards = sum(Boards)
            , Alights = sum(Alights)
            )
        , Stop_Id
        ]

VMH_Raw[, .N, .(Transit_Day, Route, Stop_Id)
        ][
          order(Route, Stop_Id, Transit_Day)
          ][, `:=` (days_to_next = shift(Transit_Day,-1)-Transit_Day
                    , N_Difference = shift(N,-1)-N
                    
                    )
            , .(Route, Stop_Id)
            ][Stop_Id != 0
              ] %>% View()


[,.N,days_to_next
                ]



[Stop_Id != 0
              ][!is.na(days_to_next) & 
                  days_to_next != 1
                , .N
                , .(days_to_next,Stop_Id)
                ]






data.table(grp = rep(c("A", "B", "C", "A", "B")
                     , c(2, 2, 3, 1, 2)
                     )
           , value = 1:10)[
,rleid := rleid(grp)
][]

rleid(DT$grp)


#okay so we need rleid 