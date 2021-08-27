# Tommie wants to know boardings by block for the month of July so we can
# target where we send our extra board operators
# 
# we should look also at % coverage and variability

# packages ----------------------------------------------------------------
library(data.table)
library(magrittr)
library(timeDate)


# Inputs ------------------------------------------------------------------
this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date(lubridate::floor_date(Sys.Date()
                                                                 , unit = "m"
                                                                 ) - 1
                                           , unit = "m"
                                           )#end floor_date

VMH_StartTime <- stringr::str_remove_all(last_month_Avail, "-")

VMH_EndTime <- stringr::str_remove_all(this_month_Avail, "-")


# import -------------------------------------------------------- 
con_rep <- DBI::dbConnect(odbc::odbc()
                         , Driver = keyring::key_get("Driver", keyring = "con_rep")
                         , Server = keyring::key_get("Server", keyring = "con_rep")
                         , Database = keyring::key_get("Database", keyring = "con_rep")
                         , Port = keyring::key_get("Port", keyring = "con_rep")
)

keyring::keyring_lock("con_rep")


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
    ,Block_Farebox_Id
    ,GPSStatus
    ,CommStatus
    from avl.Vehicle_Message_History a (nolock)
    left join avl.Vehicle_Avl_History b
    on a.Avl_History_Id = b.Avl_History_Id
    where a.Time > '",VMH_StartTime,"'
    and a.Time < DATEADD(day,1,'",VMH_EndTime,"')"
    )
  )
) %>%
  dplyr::collect() %>%
  data.table::data.table()

hastus_trips <- fread("data\\raw\\Trips_Operated_202107.csv")



# Clean VMH ---------------------------------------------------------------

Transit_Day_Cutoff <- data.table::as.ITime("03:30:00")

vmh_raw[
  , DateTest := data.table::fifelse(
    data.table::as.ITime(Time) < Transit_Day_Cutoff
    , 1
    , 0
    )
  ][
    , Transit_Day := data.table::fifelse(DateTest == 1
                                         , data.table::as.IDate(Time)-1
                                         , data.table::as.IDate(Time)
                                         )
    ]

vmh_one_month <- vmh_raw[Transit_Day >= last_month_Avail &
                           Transit_Day < this_month_Avail
                         ][Route != "999" &
                             Route != 901
                           ]

vmh_one_month[, clock_hour := data.table::hour(Time)]

vmh_one_month[, .N, Transit_Day][order(Transit_Day)]

#set service type
holidays_sunday_service <- c("USNewYearsDay"
                             , "USMemorialDay"
                             , "USIndependenceDay"
                             , "USLaborDay"
                             , "USThanksgivingDay"
                             , "USChristmasDay"
)

holidays_saturday_service <- c("USMLKingsBirthday")

#set sat sun
holidays_sunday <- timeDate::holiday(2000:2050
                                     , holidays_sunday_service
)

holidays_saturday <- timeDate::holiday(2000:2050
                                       , holidays_saturday_service
)

#set service type column
vmh_one_month[
  , Service_Type := data.table::fcase(
    Transit_Day %in% data.table::as.IDate(holidays_saturday@Data)
    , "Saturday"
    , Transit_Day %in% data.table::as.IDate(holidays_sunday@Data)
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
    , weekdays(Transit_Day) == "Sunday"
    , "Sunday"
    )#end fcase 
  ]

vmh_one_month[
  , AdHocTripNumber := stringr::str_c(
    Inbound_Outbound
    , stringr::str_remove_all(Transit_Day,"-")
    , Trip
    , Vehicle_ID
    )
  ]


# Clean Hastus ------------------------------------------------------------

hastus_trips_operated <- hastus_trips[
  `In-S` == "TRUE" & Cancelled == "FALSE"
  ][,Date := data.table::as.IDate(lubridate::mdy(Date))
    ][From != To
      ][Date >= last_month_Avail &
          Date < this_month_Avail]


#fix time
hastus_trips_operated[, c("time"
                          , "aorp"
                          ) := data.table::tstrsplit(hastus_trips_operated$Start
                                                     , "[a,p,x,;]"
                                                     )
                      ]

hastus_trips_operated[, aorp := data.table::fifelse(Start %like% "p"
                                                    , "p"
                                                    , "a"
                                                    )
                      ][, time := as.integer(time)
                        ][, time := data.table::fifelse(Start %like% "p" & 
                                                          time < 1200 
                                                        | Start %like% "x"
                                                        , time + 1200
                                                        , time
                                                        , 0
                                                        )
                          ][, time := data.table::fifelse(time > 2359
                                                          , time - 2400
                                                          , time
                                                          )
                            ][, time := data.table::as.ITime(
                              strptime(
                                sprintf('%04d'
                                        , time
                                        )
                                , format='%H%M'
                                )
                              )
                              ][, aorp := NULL
                                ]

hastus_trips_operated[
  , service_type := data.table::fcase(
    Date %in% data.table::as.IDate(holidays_saturday@Data)
    , "Saturday"
    , Date %in% data.table::as.IDate(holidays_sunday@Data)
    , "Sunday"
    , weekdays(Date) %in% c("Monday"
                            , "Tuesday"
                            , "Wednesday"
                            , "Thursday"
                            , "Friday"
                            )#end c
    , "Weekday"
    , weekdays(Date) == "Saturday"
    , "Saturday"
    , weekdays(Date) == "Sunday"
    , "Sunday"
    )#end fcase 
  ]

hastus_trips_operated[,.N,.(Date,service_type)]

# investigate ---------------------------------------------------------------
# okay let's try to get trip, by block, then sequence within block as well

#can blockfareboxids share routes?
vmh_one_month[,.N,.(Block_Farebox_Id, Route, Trip, Transit_Day)][order(Block_Farebox_Id)]
#yes. fuck.

#okay lets start with hastus blocks
x <- hastus_trips_operated[,.(hastus_block = unique(Block))]

y <- vmh_one_month[!is.na(Block_Farebox_Id),.(avail_block = unique(Block_Farebox_Id))]

x[hastus_block %in% y$avail_block]
y[!avail_block %in% x$hastus_block]

vmh_one_month[Block_Farebox_Id != 1100]
