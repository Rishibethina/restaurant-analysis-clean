# Load required libraries
library(jsonlite)
library(readr)
library(dplyr)

# Check what JSON files you have in your project
json_files <- list.files(".", pattern = "\\.json$")
cat("Found JSON files in your project:\n")
for(file in json_files) {
  cat("  -", file, "\n")
}

# Function to flatten nested columns
flatten_nested_columns <- function(data) {
  for (col_name in names(data)) {
    col_data <- data[[col_name]]
    
    # Check if column contains lists
    if (is.list(col_data) && !is.data.frame(col_data)) {
      # Convert lists to JSON strings or comma-separated values
      data[[col_name]] <- sapply(col_data, function(x) {
        if (is.null(x) || length(x) == 0) {
          return(NA_character_)
        } else if (is.list(x)) {
          # Convert complex nested structures to JSON strings
          return(toJSON(x, auto_unbox = TRUE))
        } else {
          # Convert simple vectors to comma-separated strings
          return(paste(x, collapse = ", "))
        }
      })
    }
  }
  return(data)
}

# Function to convert JSON to CSV (with nested data handling)
convert_json_to_csv <- function(json_filename) {
  csv_filename <- gsub("\\.json$", ".csv", json_filename)
  
  cat("\nConverting", json_filename, "to", csv_filename, "...\n")
  
  tryCatch({
    # Read JSON lines file with flattening enabled
    data <- stream_in(file(json_filename), verbose = FALSE, flatten = TRUE)
    
    # Additional flattening for remaining nested columns
    data <- flatten_nested_columns(data)
    
    # Write to CSV
    write_csv(data, csv_filename)
    
    # Show file info
    file_size_mb <- round(file.size(csv_filename) / (1024^2), 2)
    cat("✓ Success:", csv_filename, "(", nrow(data), "records,", file_size_mb, "MB)\n")
    
    return(TRUE)
    
  }, error = function(e) {
    cat("✗ Error converting", json_filename, ":", e$message, "\n")
    return(FALSE)
  })
}

# Fast batch processing function for large files
convert_large_json_to_csv_fast <- function(json_filename, batch_size = 100000) {
  csv_filename <- gsub("\\.json$", ".csv", json_filename)
  
  cat("\nConverting LARGE file", json_filename, "with batch processing...\n")
  cat("Batch size:", batch_size, "records\n")
  
  # Remove existing CSV file if it exists
  if (file.exists(csv_filename)) {
    file.remove(csv_filename)
  }
  
  # Open connection
  con <- file(json_filename, "r")
  
  batch_count <- 0
  total_records <- 0
  first_batch <- TRUE
  
  tryCatch({
    repeat {
      # Read batch of lines
      lines <- readLines(con, n = batch_size)
      
      if (length(lines) == 0) break  # End of file
      
      batch_count <- batch_count + 1
      cat("Processing batch", batch_count, "- reading", length(lines), "records...")
      
      # Parse JSON lines in batch
      batch_data <- lapply(lines, function(line) {
        tryCatch({
          fromJSON(line, flatten = TRUE)
        }, error = function(e) {
          return(NULL)
        })
      })
      
      # Remove NULL entries (failed parses)
      batch_data <- batch_data[!sapply(batch_data, is.null)]
      
      if (length(batch_data) > 0) {
        # Convert to data frame
        df_batch <- bind_rows(batch_data)
        
        # Flatten any remaining nested columns
        df_batch <- flatten_nested_columns(df_batch)
        
        # Write to CSV
        if (first_batch) {
          write_csv(df_batch, csv_filename)
          first_batch <- FALSE
        } else {
          write_csv(df_batch, csv_filename, append = TRUE)
        }
        
        total_records <- total_records + nrow(df_batch)
        cat(" ✓ Written", nrow(df_batch), "records (Total:", total_records, ")\n")
      } else {
        cat(" ⚠ No valid records in this batch\n")
      }
    }
    
    close(con)
    
    # Show final results
    file_size_mb <- round(file.size(csv_filename) / (1024^2), 2)
    cat("✓ SUCCESS:", csv_filename, "(", total_records, "records,", file_size_mb, "MB)\n")
    
    return(TRUE)
    
  }, error = function(e) {
    close(con)
    cat("✗ Error:", e$message, "\n")
    return(FALSE)
  })
}

# Convert files with smart processing
cat("\n=== STARTING CONVERSION ===\n")

for(json_file in json_files) {
  file_size_mb <- round(file.size(json_file) / (1024^2), 2)
  
  # Use batch processing for large files (>100MB)
  if (file_size_mb > 100) {
    cat("\nDetected large file:", json_file, "(", file_size_mb, "MB)")
    convert_large_json_to_csv_fast(json_file, batch_size = 50000)  # Smaller batches for speed
  } else {
    convert_json_to_csv(json_file)
  }
}

# Show final summary
cat("\n=== CONVERSION COMPLETE ===\n")
csv_files <- list.files(".", pattern = "\\.csv$")
cat("CSV files created:\n")
for(csv_file in csv_files) {
  file_size_mb <- round(file.size(csv_file) / (1024^2), 2)
  cat("  -", csv_file, "(", file_size_mb, "MB)\n")
}

cat("\nYou can now load your data with:\n")
cat("business <- read_csv('yelp_academic_dataset_business.csv')\n")
cat("reviews <- read_csv('yelp_academic_dataset_review.csv')\n")
cat("users <- read_csv('yelp_academic_dataset_user.csv')\n")
cat("checkins <- read_csv('yelp_academic_dataset_checkin.csv')\n")
cat("tips <- read_csv('yelp_academic_dataset_tip.csv')\n")