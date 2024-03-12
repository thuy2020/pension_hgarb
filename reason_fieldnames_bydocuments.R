
library(dplyr)
library(rio)

# Reason data field fullname & shortname by document source

#gasb
gasb_fullname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "GASB 68") %>% colnames()

gasb_shortname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "GASB 68", skip = 1) %>% clean_names() %>% 
  setnames(
    old = c("adec_amt", "adec_paid_amt", "adec_missed"),
    new = c(paste0(c("adec_amt", "adec_paid_amt", "adec_missed"), "_from_gasb"))) %>% 
  colnames() 

#valuation
valuation_fullname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "Actuarial Valuation") %>% colnames()

valuation_shortname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "Actuarial Valuation", skip = 1) %>% clean_names() %>% 
  # Differentiate some cols from sheet 2 & sheet 3 
  setnames(
    old =  c("actuarial_return", "market_return","ava" , "aal","ual", "funded_ratio_old", "payroll"), 
    new = c(paste0(c("actuarial_return", "market_return","ava" , "aal", "ual", "funded_ratio_old", "payroll"), "_from_valuation"))) %>% 
  colnames()

#acfr
acfr_fullname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "CAFR") %>% colnames()

acfr_shortname <- import("data/HGarb_Updates_2022/Texas_ESRS_updated_2022.xlsx", sheet = "CAFR", skip = 1) %>% clean_names() %>%  
  # Differentiate some cols from sheet 2 & sheet 3 
  setnames(old = c("actuarial_return", "market_return", "ava", "aal", "ual", "funded_ratio_old",
                   "payroll"),
           new = c(paste0(c("actuarial_return", "market_return", "ava", "aal", "ual", "funded_ratio_old",
                            "payroll"), "_from_acfr"))) %>% 
  
  # Differentiate sheet 1 & sheet 3
  setnames(
    old = c("adec_amt", "adec_paid_amt", "adec_missed"),
    new = c(paste0(c("adec_amt", "adec_paid_amt", "adec_missed"), "_from_acfr"))) %>% 
  colnames()

data.frame(field_name_full = c(gasb_fullname, valuation_fullname, acfr_fullname),
           field_name_short = c(gasb_shortname, valuation_shortname, acfr_shortname)
)

reason_field_name_full_short_source <- (
  # collect all names in gasb
  data.frame(field_name_full = gasb_fullname,
             field_name_short = gasb_shortname) %>% 
    mutate(source = "gasb")) %>% 
  
  # bind with valuation
  rbind(data.frame(field_name_full = valuation_fullname, 
                   field_name_short = valuation_shortname) %>% 
          mutate(source = "valuation")) %>% 
  slice(-c(1:2)) %>% #remove full_name and fye
  
  # bind with acfr
  rbind(data.frame(field_name_full = acfr_fullname, 
                   field_name_short = acfr_shortname) %>% 
          mutate(source = "acfr")) %>% 
  slice(-c(1:2))  #remove full_name and fye

write.csv(reason_field_name_full_short_source, "output/reason_field_name_by_document.csv")


# it seems like Reason has slightly different ppd colnames compare to ppd colnames downloaded from ppd website:
ppd_colnames_created_by_reason <- import("data/marc_swaroop_handover_files/ppd-out.xlsx") %>%  
  colnames() %>% sort()


#downloaded from ppd site 
ppd_colnames <- import(here("data/ppd-data-latest.csv")) %>% colnames() %>% tolower() %>% sort()
