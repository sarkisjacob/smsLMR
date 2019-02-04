
#' Author: Sarkis Jacob
#' CRC/OTTAWA - Canada
#'
#' January 29, 2019 --
#' This program will disconnect spark 
#'
#' @param none
#' @return Disconnects spark
#' @export
#' @examples
#' 




## disconnect spark
disconnect_spark <- function(){
  spark_disconnect(spark_conn)
}

