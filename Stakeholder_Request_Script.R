# START SCRIPT HERE in order to execute it all automatically
# check your connection to TUNNEL
{
#==== Libraries and connections ====
{
#* get libraries and establish connection to DB ====
path_fun_and_pkg <- paste0(getwd(),ifelse(Sys.info()["sysname"]=="Darwin","/","\\"), "critical_lists_fun_and_pkg.R")
source(path_fun_and_pkg)
db_config <- config::get("dataconnection")
con <- dbConnect(PostgreSQL(), dbname = db_config$dbname, 
                 host=db_config$dbhost, port=db_config$dbport,
                 user=db_config$dbuser, password=db_config$dbpwd)
path_sc_properties <- paste0(getwd(),ifelse(Sys.info()["sysname"]=="Darwin","/","\\"), "sc_properties.csv")
sc_properties <- as.data.frame(fread(path_sc_properties)) %>% 
  filter(sales_channel_id %in% c("1","2","101","305","841","842","844","850"))
}
#==== USER Inputs ====
{
in_gmail <- dlgInput(message = "Check if your email address to access gDrive/gSheets is correct!", 
                     default = paste0(Sys.info()["user"],"@kfzteile24.de"))$res
gs4_auth(in_gmail) # automatically select one of pre-authorized emails
dir_out <- paste0(selectDirectory(caption = "Select Output Directory to write export files",label = "Select Output Directory",path = getActiveProject()),
                  ifelse(Sys.info()["sysname"]=="Darwin","/","\\"))

#in_export <- showQuestion(title = "UPLOAD",
#                          message = "Do you need FULL/ALL price upload file 
#                      (if YES selected - an upload with ALL prices will be generated, 
#                      if NO selected - upload file with ONLY PRICE UPDATES will be generated)?",
#                          ok = "FULL", cancel = "UPDATES only")
}
#==== other INPUT ====
{
forex_dkk <- 7.46
forex_vat_dkk <- forex_dkk/1.19*1.25
}
#==== SCRIPT ====
{
# LOAD data ====
#* get the list of Stakeholder request articles from gSheets ====
data_stakeholder_request_original <- read_sheet("https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit#gid=0",sheet = "Request", col_types = "cnncDDcccccnc")
data_stakeholder_request_original <- data_stakeholder_request_original %>% 
  mutate(min_price = round(min_price, 2)) %>%
  mutate(max_price = round(max_price, 2))
#* remove inactive requests ====
data_stakeholder_request <- data_stakeholder_request_original %>% filter(valid_from <= today(), valid_to >=today() | is.na(valid_to))
#* get stakeholder requests already in production
data_stakeholder_last <- read_sheet("https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit?pli=1#gid=1624728521",sheet = "in production", col_types = "cnncDDcccccccccnnD")
data_stakeholder_last <- data_stakeholder_last %>% 
  mutate(min_price = round(min_price, 2)) %>%
  mutate(max_price = round(max_price, 2)) %>% 
  mutate(code=paste0(product_number,market,min_price,max_price))
#* get the min/max and fixed prices articles ====
data_pricing_min_max <- getMinMaxPricesReason(sc_properties = sc_properties, product_numbers = data_stakeholder_request_original$product_number)
data_pricing_min_max <- data_pricing_min_max %>% 
  mutate(min_price = round(min_price, 2)) %>%
  mutate(max_price = round(max_price, 2))                                      
data_pricing_fixed <- getFixPricesReason(sc_properties = sc_properties,product_numbers = data_stakeholder_request$product_number)
#* get the archive from the stakeholder requests file
data_stakeholder_request_archive <- read_sheet("https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit#gid=515728761",sheet = "archive", col_types = "cnncDDcccccnc")
data_stakeholder_request_archive <- data_stakeholder_request_archive %>% 
  mutate(min_price = round(min_price, 2)) %>%
  mutate(max_price = round(max_price, 2))

#==== CHECKS ====
# perform checks and trigger alerts
#* check DOUBLE entries - if articles are listed several times ====
alert_double_entries <- data_stakeholder_request %>% 
  group_by(product_number,market) %>% 
  summarise(active_entries=n()) %>% 
  filter(active_entries>1)
#* check NEW requests ====
check_new_requests <- data_stakeholder_request %>% mutate(code=paste0(product_number,market,min_price,max_price)) %>% 
  filter(!(code %in% data_stakeholder_last$code))

#** [technical] add codes to stakeholder request data (to ease the comparison) ====
codes <- as.character()
i=1
while (i<= nrow(data_stakeholder_request)) {
  if (data_stakeholder_request[i,c("market")] == "ALL") {
    codes <- cbind(codes,paste(data_stakeholder_request[i,"product_number"],"DE-ALL (B2C, online, offline)"),
                   paste(data_stakeholder_request[i,"product_number"],"MP-amazon"),
                   paste(data_stakeholder_request[i,"product_number"],"MP-ebay"),
                   paste(data_stakeholder_request[i,"product_number"],"INT-DK"),
                   paste(data_stakeholder_request[i,"product_number"],"INT-AT"),
                   paste(data_stakeholder_request[i,"product_number"],"INT-FR"),
                   paste(data_stakeholder_request[i,"product_number"],"INT-NL"),
                   paste(data_stakeholder_request[i,"product_number"],"BPlus"))
  } else if (data_stakeholder_request[i,c("market")] == "MP-ALL") {
      codes <- cbind(codes,paste(data_stakeholder_request[i,"product_number"],"MP-amazon"),
                     paste(data_stakeholder_request[i,"product_number"],"MP-ebay"))
  } else if (data_stakeholder_request[i,c("market")] == "INT") {
      codes <- cbind(codes,paste(data_stakeholder_request[i,"product_number"],"INT-DK"),
                     paste(data_stakeholder_request[i,"product_number"],"INT-AT"),
                     paste(data_stakeholder_request[i,"product_number"],"INT-FR"),
                     paste(data_stakeholder_request[i,"product_number"],"INT-NL"))
  } else {
    codes <- cbind(codes,paste(data_stakeholder_request[i,"product_number"],data_stakeholder_request[i,"market"]))
  }
  i=i+1
}
#* check DELETE prices ====
check_delete_prices <- data_pricing_min_max %>% 
  mutate(market = ifelse(sales_channel_id %in% c("1"),"DE-ALL (B2C, online, offline)",
                          ifelse(sales_channel_id %in% c("850"),"BPlus",
                          ifelse(sales_channel_id %in% c("842"),"MP-ebay",
                          ifelse(sales_channel_id %in% c("844"),"MP-amazon",
                          ifelse(sales_channel_id %in% c("2"),"INT-AT",
                          ifelse(sales_channel_id %in% c("101"),"INT-NL",
                          ifelse(sales_channel_id %in% c("305"),"INT-FR","INT-DK"))))))),
         code=paste(product_number,market)) %>% 
  filter(reason %in% c("Stakeholder Request"
                       , "Stock Clearance"
                       , "Marketing Campaign"
                       , "Spray Can for stores don´t change"
                       , "Physical Store new fixed")
         , !(code %in% codes))

#only currently active requests are left in min/max data for further work
data_pricing_min_max <- data_pricing_min_max %>% filter(product_number %in% data_stakeholder_request$product_number)
#* check OVERLAP MIN/MAX prices + ignore 7L priced articles ====
check_overlap_min_max <- data_pricing_min_max %>% 
  mutate(market = ifelse(sales_channel_id %in% c("1"),"DE-ALL (B2C, online, offline)",
                  ifelse(sales_channel_id %in% c("850"),"BPlus",
                  ifelse(sales_channel_id %in% c("842"),"MP-ebay",
                  ifelse(sales_channel_id %in% c("844"),"MP-amazon",
                  ifelse(sales_channel_id %in% c("2"),"INT-AT",
                  ifelse(sales_channel_id %in% c("101"),"INT-NL",
                  ifelse(sales_channel_id %in% c("305"),"INT-FR","INT-DK"))))))),
         code=paste(product_number,market)) %>% 
  filter(code %in% codes, !(reason %in% c("Stakeholder Request"
                                          ,"7Learnings AB Test"
                                          , "Stock Clearance"
                                          , "Marketing Campaign"
                                          , "Spray Can for stores don´t change"
                                          , "Physical Store new fixed"))) %>% 
  select(-code)

#* check OVERLAP FIXED prices + ignore 7L priced articles ====
check_overlap_fixed <- data_pricing_fixed %>% 
  mutate(market = ifelse(sales_channel_id %in% c("1"),"DE-ALL (B2C, online, offline)",
                  ifelse(sales_channel_id %in% c("850"),"BPlus",
                  ifelse(sales_channel_id %in% c("842"),"MP-ebay",
                  ifelse(sales_channel_id %in% c("844"),"MP-amazon",
                  ifelse(sales_channel_id %in% c("2"),"INT-AT",
                  ifelse(sales_channel_id %in% c("101"),"INT-NL",
                  ifelse(sales_channel_id %in% c("305"),"INT-FR","INT-DK"))))))),
         code=paste(product_number,market)) %>% 
  filter(code %in% codes, !(reason %in% c("Stakeholder Request"
                                          ,"7Learnings AB Test"
                                          , "Stock Clearance"
                                          , "Marketing Campaign"
                                          , "Spray Can for stores don´t change"
                                          , "Physical Store new fixed"))) %>% 
  select(-code)

#* check OVERLAP MIN/MAX prices with 7L priced articles ====
check_7L_overlap_min_max <- data_pricing_min_max %>% 
  mutate(market = ifelse(sales_channel_id %in% c("1"),"DE-ALL (B2C, online, offline)",
                  ifelse(sales_channel_id %in% c("850"),"BPlus",
                  ifelse(sales_channel_id %in% c("842"),"MP-ebay",
                  ifelse(sales_channel_id %in% c("844"),"MP-amazon",
                  ifelse(sales_channel_id %in% c("2"),"INT-AT",
                  ifelse(sales_channel_id %in% c("101"),"INT-NL",
                  ifelse(sales_channel_id %in% c("305"),"INT-FR","INT-DK"))))))),
         code=paste(product_number,market)) %>% 
  filter(code %in% codes, (reason %in% c("7Learnings AB Test"))) %>% 
  select(-code)

if (nrow(check_7L_overlap_min_max) > 0) {
check_7L_overlap_min_max <- check_7L_overlap_min_max %>%
  inner_join(data_stakeholder_request, by = c("product_number","market"), suffix = c("_7L", "_request")) %>%
  filter(!(min_price_7L >= min_price_request | is.na(min_price_request)) & !(max_price_7L <= max_price_request | is.na(max_price_request)))
} else {}

#* check OVERLAP FIXED prices with 7L priced articles ====
check_7L_overlap_fixed <- data_pricing_fixed %>% 
  mutate(market = ifelse(sales_channel_id %in% c("1"),"DE-ALL (B2C, online, offline)",
                  ifelse(sales_channel_id %in% c("850"),"BPlus",
                  ifelse(sales_channel_id %in% c("842"),"MP-ebay",
                  ifelse(sales_channel_id %in% c("844"),"MP-amazon",
                  ifelse(sales_channel_id %in% c("2"),"INT-AT",
                  ifelse(sales_channel_id %in% c("101"),"INT-NL",
                  ifelse(sales_channel_id %in% c("305"),"INT-FR","INT-DK"))))))),
         code=paste(product_number,market)) %>% 
  filter(code %in% codes, (reason %in% c("7Learnings AB Test"))) %>% 
  select(-code)

if (nrow(check_7L_overlap_fixed) > 0) {
check_7L_overlap_fixed <- check_7L_overlap_fixed %>%
  inner_join(data_stakeholder_request, by = c("product_number","market"), suffix = c("_7L", "_request")) %>%
  filter(!(min_price_7L >= min_price_request | is.na(min_price_request)) & !(max_price_7L <= max_price_request | is.na(max_price_request)))
} else {}

#* check LOGIC ====
#check if min_price<=max_price
check_logic_01 <- data_stakeholder_request %>% filter(min_price>max_price)
#check if entries/markets are not overlapping
check_logic_02 <- data_stakeholder_request %>% 
  group_by(product_number,market) %>% 
  summarise(count_id=n()) %>% 
  pivot_wider(names_from = market, values_from = count_id) %>% 
  replace(is.na(.),0) %>% 
  mutate(check = round((`DE-ALL (B2C, online, offline)`+`MP-ALL`+`INT`+`BPlus`)/4 + `ALL`,1)) %>% 
  filter(check >1)

#check if entries/markets are not overlapping for MPs
check_logic_03 <- data_stakeholder_request %>% 
  group_by(product_number,market) %>%
  filter(market %in% c("MP-ebay", "MP-amazon", "MP-ALL")) %>%
  summarise(count_id=n()) %>% 
  pivot_wider(names_from = market, values_from = count_id) %>% 
  replace(is.na(.),0) %>% 
  mutate(check = round((`MP-ebay`+`MP-amazon`)/2 + `MP-ALL`,1)) %>% 
  filter(check >1)

#check_logic_04 <- data_stakeholder_request %>% 
#  group_by(product_number,market) %>% 
#  summarise(count_id=n()) %>% 
#  pivot_wider(names_from = market, values_from = count_id) %>% 
#  replace(is.na(.),0) %>% 
#  mutate(check = round((`DE-ALL (B2C, online, offline)`+`MP-ALL`+`MP-ebay`+`MP-amazon`+`INT`+`INT-AT`+`INT-FR`+`INT-NL`+`INT-DK`+`BPlus`)/10 + `ALL`,1)) %>% 
#  filter(check >1)

#* check BLACKLIST ====
sql_blacklist <- str_replace_all(
  paste0(
    "select * from pricing_data.sim_param_product_blacklist 
    where product_number in ('", gsub(", ","','",toString(data_stakeholder_request$product_number)),"') "),"\n", " ")
check_blacklist <- dbFetch(dbSendQuery(conn = con, sql_blacklist))

#* check CM1 ====
sim_comp<- str_replace_all(str_replace_all(
  paste0("SELECT max(id) as id
            FROM pricing_data.sim_operation where successful is true"),"\n", " "),"\t"," ")
operation_id <- dbGetQuery(conn = con, sim_comp)$id

sql_products <- str_replace_all(
  paste0(
    "select 
    product_number 
    ,brand_name
    ,main_category_name 
    ,category_name 
    ,subcategory_name
    ,product_name
    ,gross_purchase_price 
    from pricing_data.sim_price_", operation_id, "_www_k24_de 
    where product_number in ('", gsub(", ","','",toString(data_stakeholder_request$product_number)),"') "),"\n", " ")
data_products <- dbFetch(dbSendQuery(conn = con, sql_products))

#* create export file with latest uploaded price information
data_export <- merge(x=data_stakeholder_request,y=data_products, on="product_number",all.x = T) %>%
  mutate(cm1_check=ifelse(is.na(gross_purchase_price),NA,
                          ifelse(!(is.na(min_price)),round((min_price-gross_purchase_price)*100/min_price,2),round((max_price-gross_purchase_price)*100/max_price,2))),
         last_update = today()) %>%
  select(-sku_market_help, -date_help, -concat_help)

check_cm1 <- data_export %>% filter(cm1_check < 10)
#==== ERROR check and BREAK script ====
if (nrow(check_overlap_fixed)!=0 | nrow(check_overlap_min_max)!=0 | nrow(alert_double_entries)!=0 | nrow(check_logic_02)!=0) {
  showDialog(title = "ALERT", 
             message = paste0(
               if(nrow(check_overlap_fixed)!=0) {"Check overlap with fixed prices (table check_overlap_fixed)! "},
               if(nrow(check_overlap_min_max)!=0) {"Check overlap with other min/max prices (table check_overlap_min_max)! "},
               if(nrow(alert_double_entries)!=0) {"Check the double entries in the Stakeholder request file (table alert_double_entries)! "},
               if(nrow(check_logic_01)!=0) {"Adjust min_price being lower than max_price in the Stakeholder request file (table check_logic_01)! "},
               if(nrow(check_logic_02)!=0) {"Check overlap in the markets in the Stakeholder request file (table check_logic_02)! "},
               if(nrow(check_logic_03)!=0) {"Check for overlaps in the MPs in the Stakeholder request file (table check_logic_03)! "},
               "RUN SCRIPT AGAIN once the mismatches/errors have been fixed/aligned in the pricing_tool or Stakeholder request file!")
             )
  stop('\r Script is FINISHED')
}

#==== ERROR check and DO NOT BREAK script ====
if (nrow(check_7L_overlap_min_max)!=0 | nrow(check_7L_overlap_fixed)!=0) {
  showDialog(title = "ALERT", 
             message = paste0(
               if(nrow(check_7L_overlap_min_max)!=0) {"Stakeholder Requests Script: Check overlap with 7L prices after SUPERSCRIPT finishes! (view table: check_7L_overlap_min_max)"},
               if(nrow(check_7L_overlap_fixed)!=0) {"Stakeholder Requests Script: Check overlap with other 7L prices after SUPERSCRIPT finishes! (view table: check_7L_overlap_fixed)"})
  )
}
#==== PREPARE PRICE UPDATES ====
# DELETE prices ====
if (nrow(check_delete_prices)>0){
  generateDeleteFile(sc_properties = sc_properties,check_delete_prices,
                     paste0(dir_out,"Stakeholder_Request_DELETE_",today(),".csv"))
  showDialog(title = "DONE", 
             message = paste0("PLease manually upload the delete prices file to the pricing tool. The DELETE prices upload file has been exported to ",dir_out))
}
#==== FULL price update file ====

data_price_updates <- data.frame()
i<-1
while (i <=nrow(data_stakeholder_request)) {
  if (data_stakeholder_request[i,c("market")] == "DE-ALL (B2C, online, offline)") {
    sales_channel_ids <- c("1")
  } else if (data_stakeholder_request[i,c("market")] == "MP-ALL") {
    sales_channel_ids <- c("842","844")
  } else if (data_stakeholder_request[i,c("market")] == "MP-ebay") {
    sales_channel_ids <- c("842")
  } else if (data_stakeholder_request[i,c("market")] == "MP-amazon") {
    sales_channel_ids <- c("844")
  } else if (data_stakeholder_request[i,c("market")] == "ALL") {
    sales_channel_ids <- c("1","2","101","305","841","842","844","850")
  } else if (data_stakeholder_request[i,c("market")] == "INT") {
    sales_channel_ids <- c("2","101","305","841")
  } else if (data_stakeholder_request[i,c("market")] == "INT-AT") {
    sales_channel_ids <- c("2")
  } else if (data_stakeholder_request[i,c("market")] == "INT-FR") {
    sales_channel_ids <- c("305")
  } else if (data_stakeholder_request[i,c("market")] == "INT-NL") {
    sales_channel_ids <- c("101")
  } else if (data_stakeholder_request[i,c("market")] == "INT-DK") {
    sales_channel_ids <- c("841")
  } else if (data_stakeholder_request[i,c("market")] == "BPlus") {
    sales_channel_ids <- c("850")
  } 
  
  for (sales_channel_id in sales_channel_ids) {
#    for (reason_id in reason_ids) {
    data_price_update_new <- data_stakeholder_request[i,] %>% 
      mutate(sales_channel_id=sales_channel_id,
#             reason_id=reason_id, 
             comment = paste0("Requested: ",requested_by," // comment: ",request_comment," // comment pricing: ",pricing_comment)) %>% 
      select(product_number,sales_channel_id,min_price,max_price,reason,comment)
    data_price_updates <- rbind(data_price_updates,data_price_update_new)
#    }
  }
  i <- i+1
  rm(data_price_update_new)
}

#* stakeholder price updates to overwrite 7L min/max prices (Lines 312-348) ====
#data_price_updates_to_overwrite_7L <- data_price_updates %>% 
#  filter((product_number %in% data_pricing_min_max[data_pricing_min_max$reason == "7Learnings AB Test",]$product_number & 
#             sales_channel_id == 1)) %>%
#  mutate(min_price = round(min_price, 2),
#         max_price = round(max_price, 2))
#
#data_price_updates_to_overwrite_7L <- data_price_updates_to_overwrite_7L %>% 
#  mutate(min_price = ifelse(sales_channel_id == "841", round(min_price*forex_vat_dkk),min_price),
#         max_price = ifelse(sales_channel_id == "841", round(max_price*forex_vat_dkk),max_price))
#
#sql_codes_7L <- str_replace_all(paste0("SELECT id as sales_channel_id, code as sales_channel_code FROM pricing_data.sim_param_sales_channel where enable_calculation=TRUE"),"\n", " ")
#data_sc_codes_7L <- dbFetch(dbSendQuery(conn = con, sql_codes_7L))
#data_price_updates_to_overwrite_7L <- merge(x=data_price_updates_to_overwrite_7L, y=data_sc_codes_7L, on="sales_channel_id", all.x = T)
#data_price_updates_to_overwrite_7L[is.na(data_price_updates_to_overwrite_7L)] <- ""
#colnames(data_price_updates_to_overwrite_7L) <- c("sales_channel_id","product_number","minPrice","maxPrice","reason_id","comment","sales_channel_code")
#
#* creating Manual Upload files for overwriting 7L min/max prices
#if(nrow(data_price_updates_to_overwrite_7L)>0) {
#  data_price_updates_to_overwrite_7L <- merge(x=data_price_updates_to_overwrite_7L,y=data_sc_codes_7L, on="sales_channel_code",all.x = T)
#  reason_ids_7L <- unique(data_price_updates_to_overwrite_7L$reason_id)
#  
#  for (reason_id in reason_ids_7L) {
#    filtered_data_7L <- data_price_updates_to_overwrite_7L %>% filter(reason_id == !!reason_id)
#    file_name_7L <- paste0(dir_out, "Stakeholder_Request_script_overwrite_7L_", reason_id, "_reason_UPLOAD_", today(), ".csv")
#    generateMinMaxImportFile(sc_properties,
#                             filtered_data_7L %>% 
#                               select(product_number, sales_channel_id, min_price = minPrice, max_price = maxPrice),
#                             file_name_7L)
#    
#  }
#  showDialog(title = "DONE", 
#             message = paste0("Manually upload the price update files (for overwriting 7L prices) to the pricing tool with the correct reason (the reason is stated in CAPITAL LETTERS in the file name). All price update files have been exported to ",dir_out))
#} else {
#  showDialog(title = "DONE", 
#             message = paste0("No new prices or price updates needed"))
#}

#* remove 7L listings from the update ====
data_price_updates <- data_price_updates %>% 
  filter(!(product_number %in% data_pricing_min_max[data_pricing_min_max$reason == "7Learnings AB Test",]$product_number & 
         sales_channel_id == 1)) %>%
  mutate(min_price = round(min_price, 2),
    max_price = round(max_price, 2))

data_price_updates <- data_price_updates %>% 
  mutate(min_price = ifelse(sales_channel_id == "841", round(min_price*forex_vat_dkk),min_price),
         max_price = ifelse(sales_channel_id == "841", round(max_price*forex_vat_dkk),max_price))

#if(in_export==T) {
#  generateMinMaxImportFile(sc_properties = sc_properties,data_price_updates,paste0(dir_out,"Stakeholder_Requests_",today(),"_price_upload_FULL.csv"))
#  showDialog(title = "DONE", 
#             message = paste0("FULL price upload file has been exported to ",dir_out))
#}

sql_codes <- str_replace_all(paste0("SELECT id as sales_channel_id, code as sales_channel_code FROM pricing_data.sim_param_sales_channel where enable_calculation=TRUE"),"\n", " ")
data_sc_codes <- dbFetch(dbSendQuery(conn = con, sql_codes))
data_price_updates <- merge(x=data_price_updates, y=data_sc_codes, on="sales_channel_id", all.x = T)
data_price_updates[is.na(data_price_updates)] <- ""
#data_price_updates[,c("min_price","max_price")] <- sapply(data_price_updates[,c("min_price","max_price")],as.numeric)
colnames(data_price_updates) <- c("sales_channel_id","product_number","minPrice","maxPrice","reason_id","comment","sales_channel_code")

#* check for NOT NEEDED updates and remove from FULL price update list  ====
data_update_skip <- data_pricing_min_max[,c("sales_channel_code","product_number","min_price","max_price")]
colnames(data_update_skip) <- c("sales_channel_code","product_number","min_price_current","max_price_current")
data_update_skip <- merge(x=data_update_skip, y=data_price_updates, on=c("sales_channel_code","product_number"),all.y = T)
data_update_skip[is.na(data_update_skip)] <- ""
#data_update_skip[,c("min_price_current","max_price_current","minPrice","maxPrice")] <- sapply(data_update_skip[,c("min_price_current","max_price_current","minPrice","maxPrice")],as.numeric)
data_price_updates <- data_update_skip %>% filter(min_price_current != minPrice | max_price_current != maxPrice, !(product_number %in% check_blacklist$product_number)) %>% 
  select(-min_price_current, -max_price_current)

#* creating Manual Upload files
if(nrow(data_price_updates)>0) {
  data_price_updates <- merge(x=data_price_updates,y=data_sc_codes, on="sales_channel_code",all.x = T)
  reason_ids <- unique(data_price_updates$reason_id)
  
  for (reason_id in reason_ids) {
    filtered_data <- data_price_updates %>% filter(reason_id == !!reason_id)
    file_name <- paste0(dir_out, "Stakeholder_Request_script_", reason_id, "_reason_UPLOAD_", today(), ".csv")
    generateMinMaxImportFile(sc_properties,
                             filtered_data %>% 
                               select(product_number, sales_channel_id, min_price = minPrice, max_price = maxPrice),
                               file_name)
    
  }
  showDialog(title = "DONE", 
             message = paste0("Manually upload the price update files to the pricing tool with the correct reason (the reason is stated in CAPITAL LETTERS in the file name). All price update files have been exported to ",dir_out))
} else {
  showDialog(title = "DONE", 
             message = paste0("No new prices or price updates needed"))
}

#* selecting invalid requests from the requests sheet in the google file ====
selecting_invalid_requests_to_append <- data_stakeholder_request_original %>%
  filter(date_help == 0)

#* Adding old and invalid requests to the archive sheet in the google file ====
if (dim(selecting_invalid_requests_to_append)[1] != 0){
  sheet_append(ss = "https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit?pli=1#gid=515728761", selecting_invalid_requests_to_append, sheet = "archive")
} else {
  print("There are no requests to append to the archive")
}

#* Deleting the invalid requests from the "Requests" sheet in the google file and updating the Requests sheet ====
selecting_invalid_requests_to_delete <- which(grepl(0, data_stakeholder_request_original$date_help)) + 1

if (length(selecting_invalid_requests_to_delete) > 0) {
  # Delete each row individually, starting from the last to avoid row shifting issues
  for (row in rev(selecting_invalid_requests_to_delete)) {
    range_delete(
      ss = "https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit?pli=1#gid=0", 
      sheet = "Request", 
      range = cell_rows(row), 
      shift = "up"
    )
  }
} else {
  print("There are no requests to delete after moving them to the archive")
}

#* EXPORT relevant data + update the Stakeholder request file ====
sheet_write(data_export, "https://docs.google.com/spreadsheets/d/1iGUZd_WgkPNp7EjfZyUTuAe_oBiMJ6N2uC2y2qGM-sU/edit?pli=1#gid=1624728521", sheet = "in production")
}
}
