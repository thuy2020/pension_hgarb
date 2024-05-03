

ppd <- import(here("data/ppd-data-latest.csv"))

ppd_plans_name <- ppd %>% 
  select(ppd_id, PlanName, system_id, PlanFullName) %>% 
  distinct() %>% 
  rename_with(~ paste0("ppd_",.), .cols = -ppd_id)
 
