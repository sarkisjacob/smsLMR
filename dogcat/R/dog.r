#' A Cat Function
#'
#' This function allows you to express your love of dogs.
#' @param love Do you love dogs? Defaults to TRUE.
#' @keywords dog
#' @export
#' @examples
#' dog_function()


dog_function <- function(love=TRUE){
  if(love==TRUE){
    print("Dogs RULE!!")
  }
  else {
    print("I dont have a dog.")
  }
}


