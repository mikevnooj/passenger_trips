# Red Line Conversion to DT

#Feb 9 is when the switch happened and 901/902 had their own trips

library(data.table)
library(leaflet)
library(dplyr)
library(magrittr)
library(stringr)
library(timeDate)
library(lubridate)
library(ggplot2)

# Database Connections ----------------------------------------------------

con_rep <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "REPSQLP01VW\\REPSQLP01", 
                          Database = "TransitAuthority_IndyGo_Reporting", 
                          Port = 1433)

# Time --------------------------------------------------------------------

this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date(lubridate::floor_date(Sys.Date()
                                                                 ,unit = "month"
                                                                 ) - 1 #end floor_date
                                           ,unit = "month"
                                           )#end floor_date

#month <- "2021-04-01"

#this_month_Avail <- lubridate::floor_date(x = lubridate::as_date(month)
#                                         , unit = "month"
#                                         )

#last_month_Avail <- lubridate::floor_date(lubridate::floor_date(lubridate::as_date(month)
#                                                               , unit = "month"
#                                                               ) - 1 #end lubridate::floor_date
#                                         ,unit = "month"
#                                         )#end lubridate::floor_date







# this_month_Avail_GPS_search <- as.POSIXct(this_month_Avail) + 111600
# 
# last_month_Avail_GPS_search <- as.POSIXct(last_month_Avail) - 234000

VMH_StartTime <- str_remove_all(last_month_Avail,"-")

VMH_EndTime <- str_remove_all(this_month_Avail,"-")



#paste0 the query
VMH_Raw <- tbl(
  con_rep,sql(
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
    ,a.Avl_History_Id
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Route like '90%'
    and a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
    
    UNION
    
    select a.Time
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
    ,a.Avl_History_Id
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
    and Vehicle_ID > 1950
    and Vehicle_ID < 2000
    
    UNION
    
      select a.Time
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
      ,a.Avl_History_Id
      from avl.Vehicle_Message_History a (nolock)
      left join avl.Vehicle_Avl_History b
      on a.Avl_History_Id = b.Avl_History_Id
      where a.Time > '",VMH_StartTime,"'
      and a.Time < DATEADD(day,1,'",VMH_EndTime,"')
      and Vehicle_ID = 1899"
      )#end paste
  )#endsql
) %>% #end tbl
  collect() %>%
  data.table()


# Cleaning ----------------------------------------------------------------
Transit_Day_Cutoff <- as.ITime("03:30:00")

VMH_Raw[
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





#get 90 and remove garage boardings
VMH_Raw_90 <- VMH_Raw[Latitude > 39.709468 & Latitude < 39.877512
                      ][Longitude > -86.173321
                        ][Route == 90
                          ]

#do transit day


VMH_Raw_90[
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

VMH_Raw_90 <- VMH_Raw_90[Transit_Day >= last_month_Avail & Transit_Day < this_month_Avail]

#VMH_Raw_90[Vehicle_ID ==  1984,.(Boards = sum(Boards)),.(Day = Transit_Day)][order(Day)]

#add seconds since midnight for later

VMH_Raw_90[
  ,seconds_since_midnight := fifelse(Transit_Day == as.IDate(Time)
          ,as.numeric(as.ITime(Time) - as.ITime(Transit_Day))
          ,as.numeric(as.ITime(Time))+24*60*60)
][
  ,Clock_Hour := data.table::hour(Time)
]


#consider adding error catching here for times

#confirm dates
VMH_Raw_90[,.N,Transit_Day][order(Transit_Day)]
#error catch here as well

#set service type
holidays_sunday_service <- c("USNewYearsDay", "USMemorialDay",
                             "USIndependenceDay", "USLaborDay",
                             "USThanksgivingDay", "USChristmasDay")

holidays_saturday_service <- c("USMLKingsBirthday")

#set sat sun
holidays_sunday <- holiday(2000:2050, holidays_sunday_service)
holidays_saturday <- holiday(2000:2025, holidays_saturday_service)

#set service type column
VMH_Raw_90[
  ,Service_Type := fcase(Transit_Day %in% as.IDate(holidays_saturday@Data)
                         , "Saturday"
                         , Transit_Day %in% as.IDate(holidays_sunday@Data)
                         , "Sunday"
                         , weekdays(Transit_Day) %in% c("Monday"
                                                        , "Tuesday"
                                                        , "Wednesday"
                                                        , "Thursday"
                                                        , "Friday"
                                                        )#end c
                         , "Weekday"
                         , weekdays(Transit_Day) == "Saturday"
                         , "Saturday"
                         ,weekdays(Transit_Day) == "Sunday"
                         , "Sunday"
                         )#end fcase 
]


VMH_Raw_90[
  ,AdHocTripNumber := str_c(
   Inbound_Outbound
   ,str_remove_all(Transit_Day,"-")
   ,Trip
   ,Vehicle_ID
  )
]

zero_b_a_vehicles[,.N,Vehicle_ID]

zero_b_a_vehicles <- VMH_Raw[, .(Boards = sum(Boards)
                                 , Alights = sum(Alights)
                                 )
                             , .(Transit_Day
                                 , Vehicle_ID
                                 )
                             ][
                               Boards == 0 | Alights == 0 
                               ][
                                 order(Transit_Day)
                                 ]




VMH_Raw_90_no_zero <- VMH_Raw_90[ 
  !zero_b_a_vehicles
  , on = c("Transit_Day", "Vehicle_ID")
]


#get outliers
VMH_Raw_90_no_zero[
  , .(Boards = sum(Boards)
     ,Alights = sum(Alights))
  , .(Transit_Day,Vehicle_ID)
] %>% 
  ggplot(aes(x=Boards)) +
  geom_histogram(binwidth = 1)# +
  stat_bin(binwidth = 1, geom = "text", aes(label = ..x..), vjust = -1.5)


  
#adjust this!!!!!!!!!!!!
obvious_outlier <- 1250

outlier_apc_vehicles <- VMH_Raw_90_no_zero[
  ,.(Boards = sum(Boards)
     ,Alights = sum(Alights))
  ,.(Transit_Day,Vehicle_ID)
][
  Boards > obvious_outlier
]

#get three standard deviations from the mean
three_deeves <- VMH_Raw_90_no_zero[
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
][
  ,.(
    sum(Boards)
    ,sum(Alights)
  )
  ,.(Transit_Day,Vehicle_ID)
  
][
  ,sd(V1)*3
]+
  VMH_Raw_90_no_zero[
    !outlier_apc_vehicles
    ,on = c("Transit_Day","Vehicle_ID")
  ][
    ,.(
      sum(Boards)
      ,sum(Alights)
      )
    ,.(Transit_Day,Vehicle_ID)
    ][,mean(V1)]

three_sd_or_pct_vmh <- VMH_Raw_90_no_zero[
  #remove outlier apc vehicles
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
][
  ,.(
    Boardings = sum(Boards)
    ,Alightings = sum(Alights)
  )
  ,.(Transit_Day,Vehicle_ID)
][
  ,pct_diff := (Boardings - Alightings)/ ((Boardings + Alightings)/2)
][
  Boardings > three_deeves & pct_diff > 0.1 |
  Boardings > three_deeves & pct_diff < -0.1
]

VMH_Raw_90_no_zero[
  #remove outlier apc vehicles
  !outlier_apc_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
  ][
    ,.(
      Boardings = sum(Boards)
      ,Alightings = sum(Alights)
    )
    ,.(Transit_Day,Vehicle_ID)
    ][
      ,pct_diff := (Boardings - Alightings)/ ((Boardings + Alightings)/2)
      ][]


#get clean vmh
VMH_90_clean <- VMH_Raw_90[
  !zero_b_a_vehicles
  ,on = c("Transit_Day","Vehicle_ID")
  ][
    !three_sd_or_pct_vmh
    ,on = c("Transit_Day","Vehicle_ID")
    ][
      , nstops := uniqueN(Stop_Id)
      , AdHocTripNumber
      ][nstops >= 28 | nstops == 21
        ]

VMH_Raw_90[
  , nstops := uniqueN(Stop_Id)
  , AdHocTripNumber
][
  ,.N
  ,nstops
  ]

VMH_Raw_90[
  , nstops := uniqueN(Stop_Id)
  , AdHocTripNumber
  ][
    ,uniqueN(AdHocTripNumber)
    ,nstops
    ][order(nstops)]



#get invalid vmh
VMH_90_invalid <- fsetdiff(VMH_Raw_90,VMH_90_clean)


# expansion method --------------------------------------------------------
valid_dt <- VMH_90_clean[, trip_start_hour := hour(min(Time))
                         , AdHocTripNumber
                         ][
                           , .(VBoard = sum(Boards)
                               , VTrip = uniqueN(AdHocTripNumber))
                           , .(Inbound_Outbound
                               , trip_start_hour
                               , Service_Type
                               )
                           ]

valid_dt_xuehao <- VMH_90_clean[,.(Boards = sum(Boards))
                                ,.(Transit_Day
                                   , AdHocTripNumber
                                   , Inbound_Outbound
                                   , trip_start_hour
                                   , Service_Type
                                   )
                                ]


invalid_dt <- VMH_90_invalid[, trip_start_hour := hour(min(Time))
                             , AdHocTripNumber
                             ][
                               , .(IVBoard = sum(Boards)
                                   , IVTrip = uniqueN(AdHocTripNumber))
                               , .(Inbound_Outbound
                                   , trip_start_hour
                                   , Service_Type
                                   )
                               ]


invalid_dt_xuehao <- VMH_90_invalid[,.(Boards = sum(Boards))
                                    ,.(AdHocTripNumber
                                       , Transit_Day
                                       , Inbound_Outbound
                                       , trip_start_hour
                                       , Service_Type
                                       )
                                    ]




fwrite(valid_dt_xuehao
       ,paste0("data//processed//VMH_90_Valid_Invalid//"
               , str_sub(VMH_StartTime,0,6)
               , "_valid_trips.csv"
               )
       )

fwrite(invalid_dt_xuehao
       ,paste0("data//processed//VMH_90_Valid_Invalid//"
               , str_sub(VMH_StartTime,0,6)
               , "_invalid_trips.csv"
               )
       )


# # combine all the csvs for stat support -----------------------------------

files <- paste0("data//processed//VMH_90_Valid_Invalid//"
                , list.files("data//processed//VMH_90_Valid_Invalid"
                             , pattern = "trips.csv"
                )
)



read_plus <- function(file){
  fread(file) %>%
    mutate(valid_invalid = strsplit(file,"_")[[1]][5])
}

apc_trips_data <- files %>%
  purrr::map_df(~read_plus(.))
# 
# 
# 
# #fwrite(apc_trips_data[Transit_Day < "2021-01-01"
# #                  ][order(Transit_Day)]
#      , "data//processed//FY2020_apc_trips_data.csv"
#       )


# get trips operated ------------------------------------------------------

# paramaterize with dates
# 
# 
# need to pull all the operated trips for a year

month_end <- as.IDate(last_month_Avail) + days_in_month(last_month_Avail) - 1

year <- format(month_end,"%Y")

prev_month_end <- month_end - lubridate::days_in_month(month_end)

#last_month_for_filepath <- format(last_month_Avail,"%Y%m")

#month_before_last_for_filepath <- format(prev_month_end,"%Y%m")

trips_operated_files <- paste0("data//raw//90_Trips_Operated//"
                               , list.files("data//raw//90_Trips_Operated"
                                            , pattern = year
                                            )
                               )

trips_operated_raw <- purrr::map_df(trips_operated_files,fread)



# trips_op_raw_month_before_last <- fread(paste0("data//raw//90_Trips_Operated_"
#                                                ,month_before_last_for_filepath
#                                                ,".csv"
#                                                )
#                                         )
# 
# trips_op_raw_last_month <- fread(paste0("data//raw//90_Trips_Operated_"
#                                         , last_month_for_filepath
#                                         , ".csv"
#                                         )
#                                  )

# trips_operated_raw <- rbind(trips_op_raw_month_before_last
#                             , trips_op_raw_last_month
#                             )

trips_operated <- trips_operated_raw[`In-S` == "TRUE" & Cancelled == "FALSE"]



trips_operated[,From_To := paste0(From,To)]

trips_operated[From != To
               , .N
               , From_To
               ]


bad_combos <- c("96CCOLL66"
                ,"MAEUOI"
                ,"GPMAE"
                ,"GPUOI"
                ,"UOIGP"
                ,"MAEGP"
                ,"COLL6696C")

trips_operated_90 <- trips_operated[From != To & !From_To %in% bad_combos]

trips_operated_90[,.N,From_To]

northbound <- c("M38COLL66"
                , "DTC-GCOLL66"
                , "UOICOLL66"
                , "GPCOLL66"
                , "UOIDTC-G"
                , "UOIM38"
                , "GPM38"
                , "GPDTC-G"
                , "UOI96C"
                , "MAECOLL66"
                , "DTC-GM38"
                , "DTC-G96C"
                , "M3896C"
                , "UOICOLL64"
                , "DTC-GCOLL64"
                , "M38COLL64"
                )

trips_operated_90[, Inbound_Outbound := fifelse(From_To %in% northbound
                                                , 0
                                                , 1
                                                )
                  ]


# good check, 0 north
trips_operated_90[
  , .N
  , .(Inbound_Outbound
      , From_To
      )
  ][order(Inbound_Outbound)
    ]


trips_operated_90[, c("time"
                      , "aorp"
                      ) := tstrsplit(trips_operated_90$Start
                                     , "[a,p,x,;]"
                                     )
                  ]

trips_operated_90[, aorp := fifelse(Start %like% "p"
                                    , "p"
                                    , "a"
                                    )
                  ][, time := as.integer(time)
                    ][, time := fifelse(Start %like% "p" & 
                                          time < 1200 
                                        | Start %like% "x"
                                        , time + 1200
                                        , time
                                        , 0
                                        )
                      ][, time := fifelse(time > 2359
                                          , time - 2400
                                          , time
                                          )
                        ][, time := as.ITime(strptime(sprintf('%04d'
                                                              , time
                                                              )
                                                      , format='%H%M'
                                                      )
                                             )
                          ][, aorp := NULL
                            ]

trips_operated_90[
  , service_type := fcase(as.IDate(mdy(Date)) %in% as.IDate(holidays_saturday@Data)
                          , "Saturday"
                          , as.IDate(mdy(Date)) %in% as.IDate(holidays_sunday@Data)
                          , "Sunday"
                          , weekdays(as.IDate(mdy(Date))) %in% c("Monday"
                                                                 , "Tuesday"
                                                                 , "Wednesday"
                                                                 , "Thursday"
                                                                 , "Friday"
                                                                 )#end c
                          , "Weekday"
                          , weekdays(as.IDate(mdy(Date))) == "Saturday"
                          , "Saturday"
                          , weekdays(as.IDate(mdy(Date))) == "Sunday"
                          , "Sunday"
                          )#end fcase 
  ]

trips_operated_90[
  , AdHocTripNumber := str_c(Inbound_Outbound
                             , str_remove_all(as.IDate(mdy(Date)),"-")
                             , `Internal trp number`
                             )
  ]

trips_operated_90[, trip_start_hour := data.table::hour(time)
                  ][, Transit_Day := as.IDate(mdy(Date))
                    ][, trip_start_hour := fifelse(trip_start_hour == 4
                                                   , 5
                                                   , trip_start_hour
                                                   )
                      ]

trips_operated_90[,.N,Transit_Day][order(Transit_Day)] %>% View()

#gotta remove dupes

trips_operated_90 <- unique(trips_operated_90
                            , by = "AdHocTripNumber"
                            )



# begin the expansion -----------------------------------------------------

# first get averages by type and hour and direction


apc_trips_valid <- 
  apc_trips_data[!apc_trips_data[Transit_Day <= "2020-01-31" & 
                                   Boards > 757 | 
                                   Transit_Day %between% c("2020-02-01"
                                                           , "2020-02-29"
                                                           ) & 
                                   Boards > 1000                                                   
                                 ]
                 , on = c("AdHocTripNumber")
                 ][Transit_Day >= lubridate::floor_date(month_end,"year") #this needs to be a year
                   ][Inbound_Outbound != 9 &
                       Inbound_Outbound != 14
                     ][, trip_start_hour := fifelse(trip_start_hour == 4
                                                    , 5
                                                    , trip_start_hour
                                                    )
                       ]

apc_boardings_by_stratum <- apc_trips_valid[valid_invalid == "valid"
                                             ,.(boards = sum(Boards)
                                                , trips = .N
                                             )
                                             ,.(trip_start_hour
                                                , Service_Type
                                                , Inbound_Outbound              
                                             )
                                             ][,avg_boardings := boards/trips
                                               ][order(Inbound_Outbound
                                                       , -Service_Type
                                                       , trip_start_hour
                                                       )
                                                 ]


trips_operated_per_stratum <- trips_operated_90[
  , .(trips_operated = .N) 
  , .(trip_start_hour
      , service_type
      , Inbound_Outbound
  )
  ][order(Inbound_Outbound
          , -service_type
          , trip_start_hour
          )
    ]

expanded_operated_trips <- merge.data.table(trips_operated_per_stratum
                                            , apc_boardings_by_stratum
                                            , by.x = c("service_type"
                                                       , "trip_start_hour"
                                                       , "Inbound_Outbound"
                                                       )
                                            , by.y = c("Service_Type"
                                                       , "trip_start_hour"
                                                       , "Inbound_Outbound"
                                                       )
                                            , all = TRUE
                                            )[, final_boardings := trips_operated * avg_boardings
                                              ][order(Inbound_Outbound
                                                      , -service_type
                                                      , trip_start_hour
                                                      )
                                                ][,sum(final_boardings,na.rm = TRUE)][]


# function development ----------------------------------------------------


month_end <- as.IDate("2021-06-30")

month_number <- month(month_end)

#month_number == 1

apc_boardings_by_stratum <- apc_trips_valid[valid_invalid == "valid" & Transit_Day <= month_end
                                            , .(boards = sum(Boards)
                                                , trips = .N
                                                )
                                            , .(trip_start_hour
                                                , Service_Type
                                                , Inbound_Outbound              
                                                )
                                            ][, avg_boardings := boards/trips
                                              ][order(Inbound_Outbound
                                                      , -Service_Type
                                                      , trip_start_hour
                                                      )
                                                ]


trips_operated_per_stratum <- trips_operated_90[Transit_Day <= month_end
  , .(trips_operated = .N) 
  , .(trip_start_hour
      , service_type
      , Inbound_Outbound
      )
  ][order(Inbound_Outbound
          , -service_type
          , trip_start_hour
          )
    ]

expanded_operated_trips <-merge.data.table(trips_operated_per_stratum
                 , apc_boardings_by_stratum
                 , by.x = c("service_type"
                            , "trip_start_hour"
                            , "Inbound_Outbound"
                            )
                 , by.y = c("Service_Type"
                            , "trip_start_hour"
                            , "Inbound_Outbound"
                            )
                 , all = TRUE
                 )[, final_boardings := trips_operated * avg_boardings
                   ][order(Inbound_Outbound
                           , -service_type
                           , trip_start_hour
                           )
                     ][,trip_start_hour := fifelse(is.na(trips_operated)
                                                   ,trip_start_hour + 1
                                                   ,trip_start_hour
                                                   )
                       ][,`:=`(trips_operated = sum(trips_operated,na.rm = TRUE)
                               , boards = sum(boards,na.rm = TRUE)
                               , trips = sum(trips,na.rm = TRUE)
                               )
                         ,.(service_type
                            , trip_start_hour
                            , Inbound_Outbound
                            ,trip_start_hour
                            )
                         ][,avg_boardings := boards/trips
                           ][,final_boardings := trips_operated * avg_boardings]


#get service table
service_table <- trips_operated_90[Transit_Day <= month_end][,.(service_days_in_month = uniqueN(Transit_Day)),service_type]

month_summary <- expanded_operated_trips[, .(boardings = sum(final_boardings,na.rm = TRUE))
                                         , service_type
                                         ][service_table,on = "service_type"
                                           ][,`:=` (boardings = round(boardings)
                                                    , average_daily_boardings = round(boardings/service_days_in_month)
                                                    )
                                             ][]

fwrite(month_summary
       , paste0("data//output//Route_90_Expanded//"
                , format(month_end
                         , "%Y%m"
                         )
                ,"_summary.csv"
                )
       )

ytd_month_summary <- expanded_operated_trips[, .(boardings = sum(final_boardings,na.rm = TRUE))
                                             , service_type
                                             ][service_table,on = "service_type"
                                               ][,`:=` (boardings = boardings
                                                        , average_daily_boardings = boardings/service_days_in_month
                                               )
                                               ][]

fwrite(ytd_month_summary
       , paste0("data//processed//Route_90_Expanded//"
                , format(month_end
                         , "%Y%m"
                         )
                ,"_ytd_month_summary.csv"
                )
       )


# second section function dev ---------------------------------------------





apc_boardings_by_stratum <- apc_trips_valid[valid_invalid == "valid" & Transit_Day <= month_end
                                            , .(boards = sum(Boards)
                                                , trips = .N
                                            )
                                            , .(trip_start_hour
                                                , Service_Type
                                                , Inbound_Outbound              
                                            )
                                            ][, avg_boardings := boards/trips
                                              ][order(Inbound_Outbound
                                                      , -Service_Type
                                                      , trip_start_hour
                                              )
                                              ]

trips_operated_per_stratum <- trips_operated_90[Transit_Day <= month_end
                                                , .(trips_operated = .N) 
                                                , .(trip_start_hour
                                                    , service_type
                                                    , Inbound_Outbound
                                                )
                                                ][order(Inbound_Outbound
                                                        , -service_type
                                                        , trip_start_hour
                                                )
                                                ]

expanded_operated_trips <- merge.data.table(trips_operated_per_stratum
                                            , apc_boardings_by_stratum
                                            , by.x = c("service_type"
                                                       , "trip_start_hour"
                                                       , "Inbound_Outbound"
                                                       )
                                            , by.y = c("Service_Type"
                                                       , "trip_start_hour"
                                                       , "Inbound_Outbound"
                                                       )
                                            , all = TRUE
                                            )[, final_boardings := trips_operated * avg_boardings
                                              ][order(Inbound_Outbound
                                                      , -service_type
                                                      , trip_start_hour
                                                      )
                                                ][,trip_start_hour := fifelse(is.na(trips_operated)
                                                                              ,trip_start_hour + 1
                                                                              ,trip_start_hour
                                                                              )
                                                  ][,`:=`(trips_operated = sum(trips_operated,na.rm = TRUE)
                                                          , boards = sum(boards,na.rm = TRUE)
                                                          , trips = sum(trips,na.rm = TRUE)
                                                          )
                                                    ,.(service_type
                                                       , trip_start_hour
                                                       , Inbound_Outbound
                                                       ,trip_start_hour
                                                       )
                                                    ][,avg_boardings := boards/trips
                                                      ][,final_boardings := trips_operated * avg_boardings]


#get service table
service_table <- trips_operated_90[Transit_Day <= month_end 
                                   & Transit_Day > prev_month_end][,.(service_days_in_month = uniqueN(Transit_Day)),service_type]



ytd_month_summary <- expanded_operated_trips[, .(boardings = sum(final_boardings))
                                         , service_type
                                         ]

prev_ytd_summary <- fread(paste0("data//processed//Route_90_Expanded//"
                                 , format(prev_month_end
                                          , "%Y%m"
                                          )
                                 , "_ytd_month_summary.csv"
                                 )
                          )

month_summary <- prev_ytd_summary[ytd_month_summary,on = "service_type"
                                  ][, month_boardings := i.boardings - boardings
                                    ][ ,.(service_type
                                          , boardings = month_boardings
                                          )
                                       ][service_table,on = "service_type"
                                         ][,`:=` (boardings = round(boardings)
                                                  , average_daily_boardings = round(boardings/service_days_in_month)
                                                  )
                                           ][]

fwrite(month_summary
       , paste0("data//output//Route_90_Expanded//"
                , format(month_end
                         , "%Y%m"
                         )
                ,"_summary.csv"
                )
       )

fwrite(ytd_month_summary
       , paste0("data//processed//Route_90_Expanded//"
                , format(month_end
                         , "%Y%m"       
                         )
                , "_ytd_month_summary.csv"
                )       
       )


# trip comparo
# 
con_dw <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", 
                         Server = "AVAILDWHP01VW", Database = "DW_IndyGo", Port = 1433)

DimTrip <- tbl(con_dw
               ,"DimTrip"
) %>%
  collect() %>%
  data.table()


trips_operated_90[,.(unique(`Internal trp number`))][V1 %in% DimTrip$PermanentTripNumber][V1 %in% trips_operated_90[,.(unique(`Internal trp number`))][V1 %in% DimTrip$TripInternalNumber]$V1]

trips_operated_90[,.(unique(`Internal trp number`))][V1 %in% DimTrip$TripInternalNumber]$V1

DimTrip[!is.na(TripInternalNumber)]
