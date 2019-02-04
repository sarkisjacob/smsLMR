
#' Author: Sarkis Jacob
#' CRC/OTTAWA - Canada
#'
#' January 29, 2019 --
#' This program filters a dataset within a bounary-box centered at coordinates (latitude and longitude) and with side-length (box_length_km) specified by the user. 
#'
#' @param latitude Defines the latitude coordinate for the center of the boundary-box containing the dataset.
#' @param longitude Defines the longitude coordinate for the center of the boundary-box containing the dataset.
#' @param box_length_km Defines the side-length of the boundary-box containing the dataset.
#' @return Spark dataframe
#' @export
#' @examples
#' gps_coordinates_within_box(latitude,longitude, box_length_km)
 


## select data set using coordinates inside a box with side length of box_length_km specified by the user
gps_coordinates_within_box <- function(latitude,longitude, box_length_km){
  
  lat <- "latitude_WGS84"
  long <- "longitude_WGS84"
  
  dburl <- "ssm-spectrumdw-sql.database.windows.net"
  dbname <- "ssmspectrumdw"
  dbtable <- "lmr_spectrum_license"
  schema <- "sms"
  
  latitude <- latitude*pi/180.0 
  longitude <- longitude*pi/180.0
  length_km <-  formatC(box_length_km/2, digits = 2, format = "f")
  
  query <- paste0("SELECT * 
                  FROM ",schema,".",dbtable," , 
                  (SELECT lat_d_north,
                  (",longitude," + atn2(sin(radians(90.0))*sin(",length_km,"/6371)*cos((",latitude,")), cos(",length_km,"/6371)-sin((",latitude,"))*sin(radians(lon_d_east))))*180.0/PI() AS lon_d_east,
                  lat_d_south,
                  (",longitude," + atn2(sin(radians(270.0))*sin(",length_km,"/6371)*cos((",latitude,")), cos(",length_km,"/6371)-sin((",latitude,"))*sin(radians(lon_d_west))))*180.0/PI() AS lon_d_west
                  FROM ( 
                  SELECT asin( sin((",latitude,"))*cos(",length_km,"/6371) + cos((",latitude,"))*sin(",length_km,"/6371)*cos(radians(0.0)))*180/PI() AS lat_d_north,
                  asin( sin((",latitude,"))*cos(",length_km,"/6371) + cos((",latitude,"))*sin(",length_km,"/6371)*cos(radians(90.0)))*180/PI() AS lon_d_east,  
                  asin( sin((",latitude,"))*cos(",length_km,"/6371) + cos((",latitude,"))*sin(",length_km,"/6371)*cos(radians(180.0)))*180/PI() AS lat_d_south,
                  asin( sin((",latitude,"))*cos(",length_km,"/6371) + cos((",latitude,"))*sin(",length_km,"/6371)*cos(radians(270.0)))*180/PI() AS lon_d_west
                  ) latitude_limit_table) AS boundry_box_table 
                  WHERE ",lat," <= boundry_box_table.lat_d_north AND ",lat," >= boundry_box_table.lat_d_south AND 
                  ",long," <= boundry_box_table.lon_d_east AND ",long," >= boundry_box_table.lon_d_west")
  
  db <- spark_read_jdbc(sc = spark_conn, name = dbtable, 
                        options = list(url = paste0("jdbc:sqlserver://",dburl,""), user = username, password = passwd, databaseName = dbname, dbtable  = paste0("(",query, ") as_query")), 
                        driver= "com.microsoft.sqlserver.jdbc.SQLServerDriver", memory = FALSE)
  db <- select(db,-lat_d_north,-lat_d_south,-lon_d_east,-lon_d_west)
}
