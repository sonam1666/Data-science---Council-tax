
library(haven) 
library(dplyr) 
library(odbc)
library(RSQLite)
library(DBI)




#----------------------house_price_data------------------------
house_price_data <- read.csv("D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/HousePrice.csv")

#----------------------broadband-------------------------------
broadband <- read.csv("D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/BroadBand.csv")

#----------------------council_tax-----------------------------
council_tax <- read.csv("D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/CouncilTax.csv")

#----------------------district-----------------------------
district <- read.csv("D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/District.csv")

#----------------------ward-----------------------------
ward <- read.csv("D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/Ward.csv")




#------------ Database Connection -------------
connection <- dbConnect(RSQLite::SQLite(), "D:/Study Material/assignment data/Daatabase -SQL/SQL_sonam/SQL project/database.db")


# Insert Ward
dbGetQuery(connection, "SELECT count(*) FROM WARD")
dbWriteTable(connection, name="WARD", value=ward, append=TRUE)

# Insert District
dbGetQuery(connection, "SELECT count(*) FROM DISTRICT")

# Remove duplicate 
district <- district %>% distinct(District_Code, .keep_all = TRUE)

dbWriteTable(connection, name="DISTRICT", value=district, append=TRUE)

# Insert house_price_data
dbGetQuery(connection, "SELECT count(*) FROM MEDIAN_HOUSE_PRICES")
dbWriteTable(connection, name="MEDIAN_HOUSE_PRICES", value=house_price_data, append=TRUE)

# Insert Broadband
dbGetQuery(connection, "SELECT count(*) FROM BROADBAND")
dbWriteTable(connection, name="BROADBAND", value=broadband, append=TRUE)

# Insert Council Tax
dbGetQuery(connection, "SELECT count(*) FROM COUNCIL_TAX")
dbWriteTable(connection, name="COUNCIL_TAX", value=council_tax, append=TRUE)

--------------------------------------------------------------

# Question 3: The average price of houses in two years, e.g., average of prices for 2020 and 2021

dbGetQuery(connection, 
           "SELECT a.Ward_Code, b.Ward_Name, c.District_Name, 
           (a.Year_ending_Mar_2020 + a.Year_ending_Jun_2020 + a.Year_ending_Sep_2020 + a.Year_ending_Dec_2020) / 4 as \"Avg_of_2020\", 
           (a.Year_ending_Mar_2021 + a.Year_ending_Jun_2021 + a.Year_ending_Sep_2021 + a.Year_ending_Dec_2021) / 4 as \"Avg_of_2021\" 
           FROM MEDIAN_HOUSE_PRICES a, WARD b, DISTRICT c
           WHERE a.Ward_Code = b.Ward_Code 
           AND  b.District_Code = c.District_Code 
           AND b.Ward_Name = 'Wootton';"
)

# Question 4: The average increase (or decrease) in prices (in percent) between two years, 2019 and 2020 in Deddington ward
dbGetQuery(connection, 
           "
SELECT  
  a.Ward_Code, 
  b.Ward_Name, 
  c.District_Name, 
  (a.Year_ending_Mar_2020 + a.Year_ending_Jun_2020 + a.Year_ending_Sep_2020 + a.Year_ending_Dec_2020) / 4 as Average_of_prices_for_2020,
  (a.Year_ending_Mar_2021 + a.Year_ending_Jun_2021 + a.Year_ending_Sep_2021 + a.Year_ending_Dec_2021) / 4 as Average_of_prices_for_2021,
  ((a.Year_ending_Mar_2021 + a.Year_ending_Jun_2021 + a.Year_ending_Sep_2021 + a.Year_ending_Dec_2021) / 4) - 
  ((a.Year_ending_Mar_2020 + a.Year_ending_Jun_2020 + a.Year_ending_Sep_2020 + a.Year_ending_Dec_2020) / 4) as Price_Change,
  ((((a.Year_ending_Mar_2021 + a.Year_ending_Jun_2021 + a.Year_ending_Sep_2021 + a.Year_ending_Dec_2021) / 4) - 
  ((a.Year_ending_Mar_2020 + a.Year_ending_Jun_2020 + a.Year_ending_Sep_2020 + a.Year_ending_Dec_2020) / 4)) * 100 / 
  ((a.Year_ending_Mar_2020 + a.Year_ending_Jun_2020 + a.Year_ending_Sep_2020 + a.Year_ending_Dec_2020) / 4)) as  \"(%)Percentage_Change \"
FROM 
  MEDIAN_HOUSE_PRICES a
  JOIN WARD b ON a.Ward_Code = b.Ward_Code 
  JOIN DISTRICT c ON b.District_Code = c.District_Code 
WHERE 
  b.Ward_Name = 'Deddington';
"
)

# Question 5: Find a ward which has the highest house price in Mar 2021
dbGetQuery(connection,"
           SELECT a.Ward_Code, b.Ward_Name, c.District_Name, 
          max(a.Year_ending_Mar_2021) \"Highest house price for Mar 2021\"
          FROM MEDIAN_HOUSE_PRICES a, WARD b, DISTRICT c 
          WHERE a.Ward_Code = b.Ward_Code  
          AND b.District_Code = c.District_Code
           ")

# Question 6: find a ward which has the lowest house price in Dec 2019.
dbGetQuery(connection,"
           SELECT a.Ward_Code, b.Ward_Name, c.District_Name,
a.Average_Download_Speed,
a.Superfast_Availability
FROM BROADBAND a, WARD b, DISTRICT c
WHERE a.Ward_Code = b.Ward_Code 
AND b.District_Code = c.District_Code
AND b.Ward_Name = 'Leafield';
           ")

#Question 7: 
dbGetQuery(connection,"
         SELECT
          AVG(CAST(REPLACE(Receiving_under_10_Mbps, '%', '') AS DECIMAL)) AS Average_Receiving_under_10_Mbps
        FROM BROADBAND a
        JOIN WARD b ON a.Ward_Code = b.Ward_Code
        JOIN DISTRICT c ON b.District_Code = c.District_Code
        WHERE c.District_Name = 'West Oxfordshire';

")

#Question 8: Calculate average council tax charge for a particular town in a particular district for any three bands of properties
dbGetQuery(connection,"
           SELECT a.Ward_Code, b.Ward_Name, c.District_Name,
            a.Band_A, a.Band_B, a.Band_C,
            ((a.Band_A + a.Band_B + a.Band_C) / 3) as \"Average Council tax Charge\"
            FROM COUNCIL_TAX a 
            JOIN WARD b 
            ON a.Ward_Code = b.Ward_Code 
            JOIN DISTRICT c 
            ON b.District_Code = c.District_Code
            WHERE b.Ward_Name = 'Leafield';
   ")

#Question 9 : calculates the difference between council tax charges of same bands but of two different towns of the same district
dbGetQuery(connection,"
          SELECT  DISTINCT(select c.Band_C from COUNCIL_TAX c, WARD a WHERE c.Ward_Code = a.Ward_Code and a.Ward_Name = 'Churchill') -
(select c.Band_C from COUNCIL_TAX c, WARD a WHERE c.Ward_Code = a.Ward_Code and a.Ward_Name = 'North Leigh') as Difference
FROM COUNCIL_TAX c, WARD a

   ")

#Question 10 :find a town which has the lowest council tax charges for Band B properties
dbGetQuery(connection,"
           SELECT a.Ward_Code, b.Ward_Name, c.District_Name, 
          min(a.Band_F) as \"Minimun Council tax charges\"
          FROM COUNCIL_TAX a 
          JOIN WARD b 
          ON a.Ward_Code = b.Ward_Code 
          JOIN DISTRICT c 
          ON b.District_Code = c.District_Code
          WHERE c.District_Name = 'West Oxfordshire';
")


-------------- Disconnect -------------------

dbDisconnect(connection)