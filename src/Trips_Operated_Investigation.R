library(data.table)

# Inputs ------------------------------------------------------------------

this_month_Avail <- lubridate::floor_date(Sys.Date(), unit = "month")

last_month_Avail  <- lubridate::floor_date(lubridate::floor_date(Sys.Date()
                                                                 , unit = "m"
                                                                 ) - 1
                                           , unit = "m"
                                           )#end floor_date

month_end <- data.table::as.IDate(last_month_Avail) + 
  lubridate::days_in_month(last_month_Avail) - 1

year <- format(month_end, "%Y")

prev_month_end <- month_end - lubridate::days_in_month(month_end)

trips_operated_file <- "data//raw//Trips_Operated_Test.csv"



# Import and Clean --------------------------------------------------------

trips_operated_raw <- purrr::map_df(trips_operated_file, fread)

trips_operated_90_dirty <- trips_operated_raw[`In-S` == "TRUE" & 
                                                Cancelled == "FALSE" &
                                                `Rte stat` == 90
                                              ]

trips_operated_90_dirty[, From_To := paste0(From, To)]

trips_operated_90_dirty[From != To
                        , .N
                        , From_To
                        ]


bad_combos <- c("96CCOLL66"
                , "MAEUOI"
                , "GPMAE"
                , "GPUOI"
                , "UOIGP"
                , "MAEGP"
                , "COLL6696C")

trips_operated_90 <- trips_operated_90_dirty[From != To & 
                                               !From_To %in% bad_combos
                                             ]

trips_operated_90[, .N, From_To]

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
trips_operated_90[, .N
                  , .(Inbound_Outbound
                      , From_To
                  )
                  ][order(Inbound_Outbound)
                    ]


trips_operated_90[, c("time"
                      , "aorp"
                      ) := data.table::tstrsplit(trips_operated_90$Start
                                                 , "[a,p,x,;]"
                                                 )
                  ]

trips_operated_90[, aorp := data.table::fifelse(Start %like% "p"
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


trips_operated_90[
  , AdHocTripNumber := stringr::str_c(
    Inbound_Outbound
    , stringr::str_remove_all(data.table::as.IDate(lubridate::mdy(Date))
                              , "-"
    )
    , `Internal trp number`
  )
  ]

trips_operated_90[,.(Group
                     ,`Rte stat`
                     , SchType
                     , `Internal trp number`
                     , `Date Stamp`
                     , Day
                     , Start
                     , From
                     , To
                     )
                  ]


trips_operated_90[,.N,hour(time)]

hastus <- trips_operated_90[, .(hastus_trips = uniqueN(AdHocTripNumber))
                            , .(trip_start_hour = hour(time)
                                , Inbound_Outbound
                                , SchType
                                )
                            ][order(SchType
                                    , Inbound_Outbound
                                    , trip_start_hour
                                    )
                              ]

avail <- valid_dt[order(Service_Type
                        , Inbound_Outbound
                        , trip_start_hour
                        )
                  ]


merged_avail_hastus <- merge.data.table(hastus
                                        , avail
                                        , by.x = c("trip_start_hour"
                                                   , "Inbound_Outbound"
                                                   , "SchType"
                                                   )
                                        , by.y = c("trip_start_hour"
                                                   , "Inbound_Outbound"
                                                   , "Service_Type"
                                                   )
                                         , all = TRUE
                                         )

merged_avail_hastus[,diff := VTrip - hastus_trips
                    ][order(SchType
                            , Inbound_Outbound
                            , trip_start_hour
                            )
                      ] %>% View()



fwrite(trips_operated_90[,.(Group
                            ,`Rte stat`
                            , SchType
                            , `Internal trp number`
                            , `Date Stamp`
                            , Day
                            , Start
                            , From
                            , To
                            )
                         ]
       , "trips_to_send_to_avail.csv"
       )

fread("trips_to_send_to_avail.csv")
