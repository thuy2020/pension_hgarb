library(dplyr)
library(stringr)
library(janitor)
###############
# Equable teacher plans (Truong emailed April 18, 2024)
teachers_plans_equable <- import("data/List of Teacher and Public School Plans for Reason.xlsx") %>% 
  filter((`Who it Covers?` == "Teachers") & (`Municipal Plan` != "Yes")) %>% 
  select(System_Name, Plan_Shorthand) %>% 
  
  mutate(Plan_Shorthand = str_squish(Plan_Shorthand),
         Plan_Shorthand = str_trim(Plan_Shorthand)) %>% 
  rename(Plan_Shorthand_equable = Plan_Shorthand) %>% arrange(System_Name)

#############
# PPD plan names 
ppd_plans_name <- import("data/ppd-data-latest.csv") %>% select(ppd_id, PlanName, PlanFullName) %>% 
  distinct() %>%
  rename(short_hand_ppd = PlanName) %>% 
  mutate(ppd_id = as.character(ppd_id))

# Reason plan names
all_plans_2021_2022_all_ids <- import("output/all_plans_2021_2022_updated April 18.csv") %>% 
  select(-c(V1, plan_name_legacydatabase)) %>% 
  
  # joind with ppd to get the short_hand name
  left_join(ppd_plans_name, by = "ppd_id") %>% select(-c(ppd_id)) %>% distinct()


reason_plans_covering_teachers <- all_plans_2021_2022_all_ids  %>% 
  
  # Regex to filter all plans that cover teacher - apply for reason data
  # Find out these pattern by examining the teachers_plans_equable list above
  
  filter(str_detect(plan_name_hgarb, "(?i)(teacher)|(teachers)|(school)|(TRS)|(Education)") | 
           str_detect(short_hand_ppd, "(?i)teacher|(Virginia RS)|(Kansas PERS)")) %>% arrange(plan_name_hgarb) %>% distinct() 


############
#Export result

write.csv(reason_plans_covering_teachers, "output/plans_covering_teachers.csv")

# the REASON list contains 44 distinct plans while equable has 40
reason_plans_covering_teachers %>% select(plan_name_hgarb, short_hand_ppd) %>% 
  distinct() %>% arrange(plan_name_hgarb) %>% write.csv("reason_teacherplan_list.csv")

teachers_plans_equable %>% select(System_Name, Plan_Shorthand_equable) %>% 
  distinct() %>% write.csv("equable_teacherplan_list.csv")
