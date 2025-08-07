# Load required libraries
library(jsonlite)
library(readr)
library(dplyr)

# Function to aggressively flatten nested columns
flatten_business_data <- function(data) {
  cat("Flattening nested columns...\n")
  
  for (col_name in names(data)) {
    col_data <- data[[col_name]]
    
    # Check if column contains lists/nested data
    if (is.list(col_data) && !is.data.frame(col_data)) {
      cat("  Processing column:", col_name, "\n")
      
      data[[col_name]] <- sapply(col_data, function(x) {
        if (is.null(x) || length(x) == 0) {
          return(NA_character_)
        } else if (is.list(x) || is.vector(x)) {
          # For business data, common nested columns are:
          # - attributes: convert to JSON string
          # - categories: convert to comma-separated
          # - hours: convert to JSON string
          if (col_name == "categories" && is.vector(x)) {
            return(paste(x, collapse = ", "))
          } else {
            return(toJSON(x, auto_unbox = TRUE, na = "null"))
          }
        } else {
          return(as.character(x))
        }
      })
    }
  }
  
  return(data)
}

# Convert business dataset specifically
convert_business_dataset <- function() {
  json_file <- "yelp_academic_dataset_business.json"
  csv_file <- "yelp_academic_dataset_business.csv"
  
  if (!file.exists(json_file)) {
    cat("Error: File", json_file, "not found in current directory.\n")
    cat("Available JSON files:\n")
    json_files <- list.files(".", pattern = "\\.json$")
    for(file in json_files) {
      cat("  -", file, "\n")
    }
    return(FALSE)
  }
  
  cat("Converting business dataset...\n")
  cat("File:", json_file, "\n")
  
  tryCatch({
    # Read JSON file with flattening
    cat("Reading JSON data...\n")
    data <- stream_in(file(json_file), verbose = FALSE, flatten = TRUE)
    
    cat("Data loaded:", nrow(data), "businesses\n")
    cat("Columns:", ncol(data), "\n")
    
    # Check for problematic columns
    list_cols <- sapply(data, function(x) is.list(x) && !is.data.frame(x))
    if (any(list_cols)) {
      cat("Found nested columns:", names(data)[list_cols], "\n")
      data <- flatten_business_data(data)
    }
    
    # Final check - convert any remaining lists to characters
    for (i in 1:ncol(data)) {
      if (is.list(data[[i]])) {
        cat("Converting remaining list column:", names(data)[i], "\n")
        data[[i]] <- sapply(data[[i]], function(x) {
          if (is.null(x)) return(NA_character_)
          return(paste(as.character(x), collapse = ", "))
        })
      }
    }
    
    # Write to CSV
    cat("Writing to CSV...\n")
    write_csv(data, csv_file)
    
    # Show results
    file_size_mb <- round(file.size(csv_file) / (1024^2), 2)
    cat("✓ SUCCESS!\n")
    cat("Created:", csv_file, "\n")
    cat("Records:", nrow(data), "\n")
    cat("File size:", file_size_mb, "MB\n")
    
    # Show column names for reference
    cat("\nColumns in the dataset:\n")
    for (i in 1:length(names(data))) {
      cat(sprintf("%2d. %s\n", i, names(data)[i]))
    }
    
    return(TRUE)
    
  }, error = function(e) {
    cat("✗ Error:", e$message, "\n")
    
    # Try alternative method if the main one fails
    cat("\nTrying alternative method...\n")
    return(convert_business_alternative())
  })
}

# Alternative method with line-by-line processing
convert_business_alternative <- function() {
  json_file <- "yelp_academic_dataset_business.json"
  csv_file <- "yelp_academic_dataset_business.csv"
  
  cat("Using line-by-line processing...\n")
  
  con <- file(json_file, "r")
  businesses <- list()
  line_count <- 0
  
  tryCatch({
    while (length(line <- readLines(con, n = 1)) > 0) {
      line_count <- line_count + 1
      
      if (line_count %% 10000 == 0) {
        cat("Processed", line_count, "businesses...\n")
      }
      
      tryCatch({
        business <- fromJSON(line, flatten = TRUE)
        
        # Manually flatten problematic fields
        if ("attributes" %in% names(business) && is.list(business$attributes)) {
          business$attributes <- toJSON(business$attributes, auto_unbox = TRUE)
        }
        if ("categories" %in% names(business) && is.list(business$categories)) {
          business$categories <- paste(unlist(business$categories), collapse = ", ")
        }
        if ("hours" %in% names(business) && is.list(business$hours)) {
          business$hours <- toJSON(business$hours, auto_unbox = TRUE)
        }
        
        businesses[[length(businesses) + 1]] <- business
        
      }, error = function(e) {
        cat("Error on line", line_count, ":", e$message, "\n")
      })
    }
    
    close(con)
    
    # Convert to data frame
    cat("Converting to data frame...\n")
    df <- bind_rows(businesses)
    
    # Write to CSV
    write_csv(df, csv_file)
    
    file_size_mb <- round(file.size(csv_file) / (1024^2), 2)
    cat("✓ Alternative method SUCCESS!\n")
    cat("Created:", csv_file, "\n")
    cat("Records:", nrow(df), "\n")
    cat("File size:", file_size_mb, "MB\n")
    
    return(TRUE)
    
  }, error = function(e) {
    close(con)
    cat("✗ Alternative method failed:", e$message, "\n")
    return(FALSE)
  })
}

# Run the conversion
cat("=== BUSINESS DATASET CONVERSION ===\n")
convert_business_dataset()

# After conversion, show how to load the data
cat("\n=== HOW TO LOAD THE DATA ===\n")
cat("business <- read_csv('yelp_academic_dataset_business.csv')\n")
cat("head(business)\n")
cat("glimpse(business)\n")