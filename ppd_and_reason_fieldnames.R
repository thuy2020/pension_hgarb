
rm(list=ls())

library(dplyr)

plan_attribute_master_df <- read.csv("data/plan_attribute_master.csv") %>% 
  select(-id)

# plan_attribute_df covers data from ppd and reason (data_source_id = 2 or 3)
plan_attribute_df <- read.csv("data/plan_attribute.csv") %>% 
  select(id, name, attribute_column_name, data_source_id)

ppd_df <- plan_attribute_df %>% 
  filter(data_source_id == 2) %>% 
  select(-data_source_id) %>% 
  rename(
    ppd_id = id,
    ppd_name = name,
    ppd_attribute_colunm_name = attribute_column_name) %>% 
  left_join(plan_attribute_master_df, by = c('ppd_id' = 'plan_attribute_id')) %>% 
  filter(!is.na(master_attribute_id))

reason_df <- plan_attribute_df %>% 
  filter(data_source_id == 3) %>% 
  select(-data_source_id) %>% 
  rename(
    reason_id = id,
    reason_name = name,
    reason_attribute_colunm_name = attribute_column_name
    ) %>% 
  left_join(plan_attribute_master_df, by = c('reason_id' = 'plan_attribute_id')) %>% 
  filter(!is.na(master_attribute_id))
  

inner_joined_ppd_reason_df <- ppd_df %>% 
  inner_join(reason_df, by = "master_attribute_id")


anti_joined_ppd_reason_df <- ppd_df %>% 
  anti_join(reason_df, by = "master_attribute_id")


# Notes
# Total contribution, Employer Projected Actuarial Required 
#Contribution Rate: Each gets assigned to two master attribute (156 and 442)
