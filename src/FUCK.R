VMH_Raw[,uniqueN(Avl_History_Id),.(Vehicle_ID,Time)][V1 > 1]

x <- VMH_Raw[, .(routes = uniqueN(Route)
                 , avls = uniqueN(Avl_History_Id)
                 )
             , .(Vehicle_ID, Time)
             ]

x[, .N, routes]




x[, .N, avls]
x[routes > 1 & avls > 1]

VMH_Raw[Time == as.POSIXct("2021-07-29 05:25:31")]

VMH_Raw[Transit_Day == "2021-07-29" & Vehicle_ID == 1973] %>% View()

x[routes == 3]

VMH_Raw[Time == as.POSIXct("2021-07-01 00:45:34",tz = "UTC")] %>%
  leaflet::leaflet() %>%
  leaflet::addCircles() %>%
  leaflet::addTiles()


VMH_Raw[, `:=` (routes = uniqueN(Route)
                , avls = uniqueN(Avl_History_Id)
                )
        , .(Vehicle_ID, Time)
        ]

VMH_Raw[
  VMH_Raw[avls == 2 & routes == 2
          , unique(Route)
          , .(Time
              , Vehicle_ID
          )
          ][order(Vehicle_ID,Time)
            ][,test := shift(V1,-1),.(Vehicle_ID,Time)
              ][,pastyboi := paste0(V1,test)
                ][!is.na(test)]
  , on = c(Vehicle_ID = "Vehicle_ID",Time = "Time")
][pastyboi %in% c("90902"
                  , "90290"
                  )
  ]%>%
  leaflet::leaflet() %>%
  leaflet::addCircles() %>%
  leaflet::addTiles()



VMH_Raw[Time == as.POSIXct("2021-07-16 08:37:37", tz = "UTC")] %>%
  leaflet::leaflet() %>%
  leaflet::addCircles() %>%
  leaflet::addTiles()
