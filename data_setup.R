# Data Setup for Restaurant Analysis
check_data_files <- function() {
  required_files <- c(
    "data/yelp_academic_dataset_review.json",
    "data/yelp_academic_dataset_business.json", 
    "data/yelp_academic_dataset_user.json"
  )
  
  missing_files <- required_files[!file.exists(required_files)]
  
  if (length(missing_files) > 0) {
    cat("Missing data files:\n")
    cat(paste("-", missing_files, collapse = "\n"))
    cat("\nPlease download from: https://www.yelp.com/dataset\n")
    cat("Extract to 'data/' directory\n")
    return(FALSE)
  }
  
  cat("All data files found! ✓\n")
  return(TRUE)
}

# Run check
check_data_files()
