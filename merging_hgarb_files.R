library(dplyr)
library(readxl) 
library(rio)
library(here)
library(janitor)
library(tidyr)
library(stringr)
source("reason_fieldnames_bydocuments.R")
source("ppd.R")

##########Plan collected each year##########
year_2021 <- list.files("data/2023_May7", pattern = "_2021.xlsx") %>% str_remove("_2021.xlsx")
year_2022 <- list.files("data/2023_May7", pattern = "_2022.xlsx") %>% str_remove("_2022.xlsx")
year_2023 <- list.files("data/2023_May7", pattern = "_2023.xlsx") %>% str_remove("_2023.xlsx")

# check plan collected in one year but not in other year
setdiff(year_2021, year_2022)
setdiff(year_2022, year_2021)
setdiff(year_2023, year_2022)

########Function to read and process data#############

import_and_slice <- function(filename) {
  read_and_process_sheet <- function(sheet) {
    data <- read_excel(filename, sheet = sheet, skip = 1) %>%
      clean_names() %>%
      mutate(fye = as.numeric(fye)) %>%
      filter(fye >= 2000 & fye <= 2023)
    
    max_year <- max(data$fye, na.rm = TRUE)
    end_index <- which(data$fye == max_year)[1]
    
    sliced_data <- data[1:end_index, ]
    
    return(sliced_data)
  }
  
  ## sheet 1
  gasb = read_and_process_sheet("GASB 68") %>% 
    # Differentiate some cols from sheet gasb & acfr
    setnames(
      old = c("adec_amt", "adec_paid_amt", "adec_missed"),
      new = c(paste0(c("adec_amt", "adec_paid_amt", "adec_missed"), "_from_gasb")))
  
  
  ### sheet 2
  valuation = read_and_process_sheet("Actuarial Valuation") %>% 
    # sheet 2 vs sheet 3 
    setnames(
      old =  c("actuarial_return", "market_return","ava" , "aal","ual", "funded_ratio_old", "payroll"), 
      new = c(paste0(c("actuarial_return", "market_return","ava" , "aal", "ual", "funded_ratio_old", "payroll"), "_from_valuation")))
  
  ## sheet 3
  acfr <- read_and_process_sheet("CAFR") %>% 
  
  # valuation vs sheet acfrs 
  setnames(old = c("actuarial_return", "market_return", "ava", "aal", "ual", "funded_ratio_old","payroll"),
           new = c(paste0(c("actuarial_return", "market_return", "ava", "aal", "ual", "funded_ratio_old",
                            "payroll"), "_from_acfr"))) %>% 
    #gasb vs. sheet acfr
  setnames(
      old = c("adec_amt", "adec_paid_amt", "adec_missed"),
      new = c(paste0(c("adec_amt", "adec_paid_amt", "adec_missed"), "_from_acfr")))
  
  # extract plan_id
  plan_id <- import(filename, sheet = "Database Input") %>% 
    select(2) %>% colnames()
  
  result <- gasb %>% 
    full_join(valuation, by = c("full_name", "fye")) %>% 
    full_join(acfr, by = c("full_name", "fye")) %>% 
    arrange(fye) %>% 
    #adding plan_id  
    mutate(plan_id = plan_id)
  
  return(result)
}

# test the function
filename <- "data/2023_May7/Texas_TRS_updated_2023.xlsx"
result <- import_and_slice(filename)


####### Loop thorough year 2023#######
filelist_2023 <- list.files("data/2023_May7", pattern = "_2023.xlsx") 
df_2023 <- data.frame()
df_exception_23 <- data.frame(filename = character(), error_message = character(), 
                           stringsAsFactors = FALSE)

for (filename in filelist_2023) {
  filename = paste0("data/2023_May7/", filename)
  result <- tryCatch({
    plan <- import_and_slice(filename)
    df_2023 <- rbind(df_2023, plan) %>% distinct()
    NULL  # No error occurred
  }, error = function(e) {
    list(filename = filename, error_message = e$message)
  })
  
  # If an error occurred, add to df_exception
  if (!is.null(result)) {
    df_exception_23 <- rbind(df_exception_23, result)
  }

}


#########Loop through 2022##########
# plans already have 2023 update
intersection22_23 <- Reduce(intersect, list(year_2023, year_2022))

# filter out plans ends _2022 but already have 2023 update
filelist_2022 <- setdiff(list.files("data/2023_May7", pattern = "_2022.xlsx"), 
                         paste0(intersection22_23, "_2022.xlsx")) # these already have 2023 update
                                     
df_2022 <- data.frame()
df_exception_22 <- data.frame(filename = character(), error_message = character(), 
                           stringsAsFactors = FALSE)
for (filename in filelist_2022) {
  filename = paste0("data/2023_May7/", filename)
  result <- tryCatch({
    plan <- import_and_slice(filename)
    df_2022 <- rbind(df_2022, plan) %>% distinct()
    NULL  # No error occurred
  }, error = function(e) {
    list(filename = filename, error_message = e$message)
  })
  
  # If an error occurred, add to df_exception
  if (!is.null(result)) {
    df_exception_22 <- rbind(df_exception_22, result)
  }
  
}


# test the function with previously error file
filename <- "data/2023_May7/Arizona_PSPRS_Tier 3_updated_2023.xlsx"
result <- import_and_slice(filename)

######Exceptions############
# Delaware has extra column -x5
df_exception_23
delaware <- import_and_slice("data/2023_May7/Delaware_PERS_NSPPP_updated_2023.xlsx") %>% 
  select(-x5)

#Michigan_JRS_updated_2022.xlsx has this: "...12" and doesn't have this: "Tot_Total_Amt" 
michigan <- import_and_slice("data/2023_May7/Michigan_JRS_updated_2023.xlsx") %>% 
  rename(tot_total_amt = x12)

exceptions <- rbind(delaware, michigan)

#dealing with other errors 
df_2023_correction <- df_2023 %>% 
  mutate(full_name = ifelse(plan_id == "6060", "Arizona - Public Safety Personnel Retirement System Tier 3", full_name))  


#######Result#########
all_plans_2023 <- rbind(df_2022, df_2023_correction, exceptions) %>% 
  mutate(plan_id = str_remove(plan_id, "\\.0"))

#write.csv(all_plans_2023, "output/all_plans_2023_hgarb.csv")

