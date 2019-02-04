
#' Author: Sarkis Jacob
#' CRC/OTTAWA - Canada
#'
#' January 29, 2019 --
#' This program filters dataset within a range of frequencies defined by the user. 
#'
#' @param centre_freq_mhz_i Defines the initial value of the centre frequency range.
#' @param centre_freq_mhz_f Defines the final value of the centre frequency range.
#' @return Spark dataframe
#' @export
#' @examples
#' centre_freq_mhz_range(centre_freq_mhz_i,centre_freq_mhz_f)



## select data within a range of frequencies
centre_freq_mhz_range <- function(centre_freq_mhz_i,centre_freq_mhz_f){
  
  tryCatch(
    {
      #if(!is.null(centre_freq_mhz_i) | !is.null(centre_freq_mhz_f))
      dburl <- "ssm-spectrumdw-sql.database.windows.net"
      dbname <- "ssmspectrumdw"
      dbtable <- "lmr_spectrum_license"
      schema <- "sms"
      
      query <- paste0("SELECT * FROM ",schema,".",dbtable,"  WHERE centre_freq_mhz >= ",centre_freq_mhz_i," and centre_freq_mhz <= ",centre_freq_mhz_f,"")
      
      db <- spark_read_jdbc(sc = spark_conn, name = dbtable,
                            options = list(url = paste0("jdbc:sqlserver://",dburl,""), user = username, password = passwd, databaseName = dbname, dbtable  = paste0("(",query, ") as_query")),
                            driver= "com.microsoft.sqlserver.jdbc.SQLServerDriver", memory = FALSE)
      #db
    },
    error=function(e)
    {
      # print(paste("Error! Invalid or missing centre_freq_mhz value(s)"))
      print(e)
    }
  )
}
