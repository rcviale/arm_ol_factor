library(tidyverse)
library(RSQLite)
library(dbplyr)
library(RPostgres)
library(frenchdata)
library(slider)

source("R/my_login.R")
my_login()
rm(my_login)

#### Login for Replication #####
# my_login <- function(){
#   login <<- c("YOUR_USERNAME", "YOUR_PASSWORD")
# }

start_date <- lubridate::ymd("1962-01-01")
end_date <- lubridate::ymd("2023-12-31")

#### WRDS Connect #### 
wrds <- dbConnect(
  Postgres(),
  host = "wrds-pgdata.wharton.upenn.edu",
  dbname = "wrds",
  port = 9737,
  sslmode = "require",
  user = login[1],
  password = login[2]
)

#### CRSP Download Data ####
msf_db <- tbl(wrds, I("crsp.msf_v2"))

stksecurityinfohist_db <- tbl(wrds, I("crsp.stksecurityinfohist"))

msf_db |>
  filter(mthcaldt >= start_date & mthcaldt <= end_date) |>
  select(-c(siccd, primaryexch, conditionaltype, tradingstatusflg)) |> 
  inner_join(
    stksecurityinfohist_db |>
      filter(sharetype == "NS" & 
               securitytype == "EQTY" & 
               securitysubtype == "COM" & 
               usincflg == "Y" & 
               issuertype %in% c("ACOR", "CORP") & 
               primaryexch %in% c("N", "A", "Q") &
               conditionaltype %in% c("RW", "NW") &
               tradingstatusflg == "A") |> 
      select(permno, secinfostartdt, secinfoenddt,
             primaryexch, siccd),
    join_by(permno)
  ) |> 
  filter(mthcaldt >= secinfostartdt & mthcaldt <= secinfoenddt) |>
  mutate(month = floor_date(mthcaldt, "month")) |>
  select(
    permno, # Security identifier
    date = mthcaldt, # Date of the observation
    month, # Month of the observation
    ret = mthret, # Return
    shrout, # Shares outstanding (in thousands)
    prc = mthprc, # Last traded price in a month
    primaryexch, # Primary exchange code
    siccd # Industry code
  ) |>
  collect() |>
  mutate(
    month = ymd(month),
    shrout = shrout * 1000
  ) |> 
  write_rds("Data/raw_crsp.rds")

rm(stksecurityinfohist_db, msf_db)

crsp_monthly <- read_rds("Data/raw_crsp.rds") |> 
  mutate(
    mktcap = shrout * prc / 10^6,
    mktcap = na_if(mktcap, 0)
  ) |> 
  filter(siccd <= 5999 | siccd >= 7000) |> # Filter out financial firms
  mutate(industry = case_when(
    siccd >= 1 & siccd <= 999 ~ "Agriculture",
    siccd >= 1000 & siccd <= 1499 ~ "Mining",
    siccd >= 1500 & siccd <= 1799 ~ "Construction",
    siccd >= 2000 & siccd <= 3999 ~ "Manufacturing",
    siccd >= 4000 & siccd <= 4899 ~ "Transportation",
    siccd >= 4900 & siccd <= 4999 ~ "Utilities",
    siccd >= 5000 & siccd <= 5199 ~ "Wholesale",
    siccd >= 5200 & siccd <= 5999 ~ "Retail",
    # siccd >= 6000 & siccd <= 6799 ~ "Finance",
    siccd >= 7000 & siccd <= 8999 ~ "Services",
    siccd >= 9000 & siccd <= 9999 ~ "Public",
    .default = "Missing"
  ))

temp <- crsp_monthly |>
  mutate(month = month %m+% months(1)) |>
  select(permno, month, mktcap_lag = mktcap)

ff <- download_french_data("Fama/French 3 Factors")$subsets[[2]][[1]] |> 
  mutate(date = make_date(year = substr(date, 1, 4), 
                          month = substr(date, 5, 6)) |> 
           ceiling_date(unit = "month") - 1) # Standardize all dates

ff |> 
  write_rds("Data/ff.rds")

read_csv("http://global-q.org/uploads/1/2/2/6/122679606/q5_factors_monthly_2022.csv") |>
  mutate(date = make_date(year = year, month = month, day = 1) |> 
           ceiling_date(unit = "month") - 1) |> 
  select(-R_F, -year, -month) |>
  rename_with(~ str_replace(., "R_", "q_")) |> 
  write_rds("Data/q5.rds")

crsp_monthly |> 
  left_join(temp, join_by(permno, month)) |> 
  mutate(date = ceiling_date(date, unit = "month") - 1) |> # Standardize all dates
  left_join(ff, join_by(date)) |> 
  mutate(ex_ret = ret - RF / 100) |> 
  drop_na(ex_ret, mktcap_lag) |> 
  write_rds("Data/crsp.rds")

rm(temp, crsp_monthly, q5, ff)

#### Compustat Download Data ####
funda_db <- tbl(wrds, I("comp.funda"))

funda_db |>
  filter(
    indfmt == "INDL" &
      datafmt == "STD" & 
      consol == "C" &
      datadate >= start_date & datadate <= end_date
  ) |>
  select( # OL = (COGS + XSGA) / AT
    gvkey, # Firm identifier
    datadate, # Date of the accounting data
    seq, # Stockholders' equity
    ceq, # Total common/ordinary equity
    at, # Total assets
    cogs, # Costs of goods sold
    xint, # Interest expense
    xsga # Selling, general, and administrative expenses
  ) |>
  collect() |> 
  write_rds("Data/raw_compustat.rds")

compustat <- read_rds("Data/raw_compustat.rds") |> 
  mutate(year = year(datadate)) |>
  group_by(gvkey, year) |>
  filter(datadate == max(datadate)) |>
  ungroup()

compustat |> 
  left_join(
    compustat |> 
      select(gvkey, year, at_lag = at) |> 
      mutate(year = year + 1), 
    join_by(gvkey, year)
  ) |> 
  mutate(date = datadate %m+% months(6) |> 
           ceiling_date(unit = "month") - 1) |> # Add 6 months to avoid look ahead bias and standardize dates
  write_rds("Data/compustat.rds")

rm(funda_db, compustat)

#### Merge CRSP and CS ####
crsp_monthly <- read_rds("Data/crsp.rds")
compustat <- read_rds("Data/compustat.rds")

ccmxpf_linktable_db <- tbl(wrds, I("crsp.ccmxpf_linktable"))

ccmxpf_linktable <- ccmxpf_linktable_db |>
  filter(linktype %in% c("LU", "LC") &
           linkprim %in% c("P", "C") &
           usedflag == 1) |>
  select(permno = lpermno, gvkey, linkdt, linkenddt) |>
  collect() |>
  mutate(linkenddt = replace_na(linkenddt, today()))

ccm_links <- crsp_monthly |>
  inner_join(ccmxpf_linktable, 
             join_by(permno), relationship = "many-to-many") |>
  filter(!is.na(gvkey) & 
           (date >= linkdt & date <= linkenddt)) |>
  select(permno, gvkey, date) 

crsp_monthly |>
  inner_join(ccmxpf_linktable, 
             join_by(permno), relationship = "many-to-many") |>
  filter(!is.na(gvkey) & 
           (date >= linkdt & date <= linkenddt)) |>
  select(permno, gvkey, date) |> 
  right_join(crsp_monthly, join_by(permno, date)) |> 
  write_rds("Data/crsp.rds")

read_rds("Data/compustat.rds") |> 
  mutate(ol   = (cogs + xsga) / at) |> 
  right_join(read_rds("Data/crsp.rds"),
             join_by(gvkey, date)) |>
  arrange(date, gvkey) |> 
  select(date, datadate, month, gvkey, everything()) |> 
  write_rds("Data/merged_db.rds")

rm(list = ls())

#### OL Portfolios ####
library(tidyverse)
library(RSQLite)
library(dbplyr)
library(RPostgres)
library(frenchdata)
library(slider)

temp <- read_rds("Data/merged_db.rds") |>
  select(date, datadate, gvkey, ol, ex_ret, prc, mktcap, mktcap_lag, primaryexch) |> 
  group_by(gvkey) |> 
  mutate(dummy = slider::slide_lgl(.x      = ol,
                                   .f      = ~sum(!is.na(.x)) >= 1, # We want to fill down the ol column, however, no longer
                                   # than 12 months, as this would mean no new data was made available, and we would be using
                                   # data that is longer that 12 months old
                                   .before = 11)) |> 
  filter(dummy == TRUE, date >= ymd("1963-01-01")) |> # Paper starts in January 1963
  fill(ol, datadate, .direction = "down") |> # With this fill, I'm pushing the last available OL 11 rows down
  drop_na(ol) |> # LTM information was filtered with the dummy, so we can drop NA based on OL
  ungroup()

breakpoints <- temp |> 
  filter(primaryexch == "N") |> # Consider only NYSE breakpoints
  group_by(date) |> 
  mutate(breaks  = case_when(
    year(date) == 1963 & month(date) == 1 | month(date) == 7 
    ~ list(quantile(ol, probs = seq(0, 1, length.out = 6), na.rm = T, names = F)[2:5]),
    .default = NA
  )) |> # For every July we are computing the quintiles, which are then dragged down
  ungroup() |> 
  fill(breaks, .direction = "down") |> # This fill will drag down the quantiles for 11 (extra) months
  select(date, breaks) |> 
  distinct()

temp |> 
  left_join(
    breakpoints,
    join_by(date)
  ) |> 
  mutate(port_id = case_when(
    ol <= (breaks[[1]][1]) ~ "p1",
    ol <= (breaks[[1]][2]) ~ "p2",
    ol <= (breaks[[1]][3]) ~ "p3",
    ol <= (breaks[[1]][4]) ~ "p4",
    .default = "p5"
  )) |> 
  write_rds("Data/portfolios.rds")

rm(temp, breakpoints)

#### Equally Weighted Portfolio ####
read_rds("Data/portfolios.rds") |> 
  filter(port_id == "p1" | port_id == "p5") |> 
  select(date, port_id, ex_ret) |> 
  group_by(date, port_id) |>
  mutate(w = 1 / length(ex_ret)) |>
  summarise(ret_port = sum(w * ex_ret)) |> 
  ungroup() |> 
  pivot_wider(names_from = port_id,
              values_from = ret_port,
              names_prefix = "ret_") |>
  mutate(cum_p5 = cumprod(1 + ret_p1),
         cum_p1 = cumprod(1 + ret_p5),
         dport  = ret_p5 - ret_p1,
         cum_dp = cumprod(1 + dport)) |> 
  write_rds("Data/equal_wgt_rets.rds")

#### Value Weighted Portfolio ####
read_rds("Data/portfolios.rds") |> 
  filter(port_id == "p1" | port_id == "p5") |> 
  select(date, port_id, ex_ret, mktcap_lag) |> 
  group_by(date, port_id) |>
  mutate(w = mktcap_lag / sum(mktcap_lag)) |>
  summarise(ret_port = sum(w * ex_ret)) |> 
  ungroup() |> 
  pivot_wider(names_from = port_id,
              values_from = ret_port,
              names_prefix = "ret_") |>
  mutate(cum_p5 = cumprod(1 + ret_p1),
         cum_p1 = cumprod(1 + ret_p5),
         dport  = ret_p5 - ret_p1,
         cum_dp = cumprod(1 + dport)) |> 
  write_rds("Data/value_wgt_rets.rds")

#### Merge All Results ####
all_data <- read_rds("Data/ff.rds") |> 
  left_join(
    read_rds("Data/q5.rds"),
    join_by(date)
  ) |> 
  right_join(
    read_rds("Data/equal_wgt_rets.rds") |>
      select(date, equal = dport),
    join_by(date)
  ) |> 
  right_join(
    read_rds("Data/value_wgt_rets.rds") |> 
      select(date, value = dport),
    join_by(date)
  )

all_data |> 
  write_rds("Data/all_data.rds")

#### Plot ####
read_rds("Data/all_data.rds") |> 
  pivot_longer(equal:value,
               names_to = "port",
               values_to = "ex_ret") |> 
  group_by(port) |>
  mutate(cum_ex_ret = cumprod(1 + ex_ret) - 1) |> 
  ggplot2::ggplot() +
  ggplot2::geom_line(ggplot2::aes(x = date, y = cum_ex_ret, col = port)) + # Plot cumulative returns
  ggplot2::theme_bw() +
  ggplot2::labs(title = "Cumulative Excess Return Differentials",
                subtitle = "Portfolios sorted on operating leverage",
                x = "Date",
                y = "Cumulative excess returns",
                col = "Weighting") +
  ggplot2::theme(text = ggplot2::element_text(size = 15), 
                 legend.position = "bottom") +
  ggplot2::scale_color_manual(values = c("red", "blue"), 
                              labels = c("Equally weighted", "Value weighted"))

#### Regressions ####
# RHS of the regressions
forms <- c(" ~ `Mkt-RF`", 
           " ~ `Mkt-RF`+ SMB + HML", 
           " ~ q_MKT + q_ME + q_IA + q_ROE + q_EG") 

# Do several regressions with nest and map
regs <- all_data |> 
  pivot_longer(equal:value,
               names_to = "port",
               values_to = "ex_ret") |> 
  nest(data = -port) |> 
  mutate(forms = list(forms)) |> 
  unnest(forms) |> 
  mutate(reg = map2(
    .x = data,
    .y = forms,
    .f = ~{
      formula <- as.formula(paste0("ex_ret", .y))
      lm(formula, data = .x)
    }
  ),  
  tidy = map(
    .x = reg,
    .f = broom::tidy
  ),
  rsq = map_dbl(
    .x = reg,
    .f = ~summary(.x)$r.squared
  ),
  a_rsq = map_dbl(
    .x = reg,
    .f = ~summary(.x)$adj.r.squared
  )) |> 
  unnest(tidy) 

regs

# Tables
stargazer::stargazer(regs |> 
                       filter(port == "equal") |> 
                       pull(reg) |> 
                       unique())

stargazer::stargazer(regs |> 
                       filter(port == "value") |> 
                       pull(reg) |> 
                       unique())
