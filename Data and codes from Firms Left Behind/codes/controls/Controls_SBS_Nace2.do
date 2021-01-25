***** Controls: Structural Business Statistics *****
**** NACE2 data 2005-2017 ****

clear
set more off

*** set directory
global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$temp"

*** download SBS data from Eurostat dirs: 
foreach file in sbs_na_sca_r2 {
copy "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F`file'.tsv.gz" "`file'.tsv.gz", replace
}

// unzip the file 
foreach file in sbs_na_sca_r2  {
shell "C:\Program Files\7-Zip\7zG.exe" e -y `file'.tsv.gz   // this line calls in the 7-Zip using the Stata shell command. As part of 7-zip syntax, e stands for extract and -y for replace file.
}

*** reformat and clean SBS data:
import delimited "sbs_na_sca_r2.tsv", varnames(1) clear

// Need to split the first variable (contains categories)
split nace, p(,) gen(var)
order var*
rename (var1 var2 var3) (nace indicator ctry)
drop nace_r2indic_sbgeotime


// Rename v2-v14 to contain years (recorded in a label)
foreach var of varlist v* {
   local x : var label `var' // saves the label of a variable to a local
   rename `var' time`x' // uses this local to rename the variable
}


// clean values
foreach var of varlist time* {
foreach x in i b p e : r d a { // check which non-numeric flags appear
cap replace `var' = substr(`var', 1, strpos(`var', "`x'")-1) if ///
strpos(`var', "`x'") > 0 
}
destring `var', replace
}
sum time* // check


// reshape to long format - to have year as another column
reshape long time, i(ctry indicator nace) j(year)
reshape wide time, i(ctry year nace) j(indicator) string

renvars, subst("time" "")
replace ctry = "GB" if ctry == "UK"
replace ctry = "GR" if ctry == "EL"


// Restrict to NACE R2 indicators of interest
keep ctry nace year V11110 V12110 V12150 V13310 V13320 V15110 V16130 V91110 V91120

// Rename indicator variables to NACE R2 categories
rename (V11110 V12110 V12150 V13310 V13320 V15110 V16130 V91110 V91120) (num_enter turnover value_add pers_costs wages invest num_empl lab_prod_app lab_prod_wage)
label variable num_enter "Number Enterprises"
label variable turnover "Turnover MEUR" 
label variable value_add "Value Added at Factor Cost MEUR" 
label variable pers_costs "Personnel Costs MEUR" 
label variable wages "Wages and Salaries MEUR" 
label variable invest "Gross Investment MEUR"
label variable num_empl "Number Employees" 
label variable lab_prod_app "Apparent Labor Productivity TEUR" 
label variable lab_prod_wage "Wage Adjusted Labor Productivity"


// Rename nace variable to NACE2 
rename nace nace2

replace nace2 = substr(nace2,2,.)
tab nace2
destring nace2, replace force
tab  nace2
drop if mi(nace2)

duplicates drop
ren ctry iso316612

gen sam = "N2"

append using "sbs_nace1_1990-2008"

// check for repetitions in 2008 to avoid double count
bys year iso nace2: gen n = _N
tab n

// for years < 2008: keep from Nace1 (only SE and LU have Nace2 data before 2008)
drop if n == 2 & year < 2008 & sam == "N2"
// for 2008: keep from Nace2
drop if n == 2 & year == 2008 & sam == ""
drop n sam

// aggregated because other datasets (skill shortages) are at such level.
replace nace2 = 43 if inrange(nace2, 41,43)
replace nace2 = 88 if inrange(nace2, 87, 88)
replace nace2 = 99 if inrange(nace2, 98, 99)


collapse (sum) num_enter-lab_prod_wage, by(nace2 year iso316612)

compress, nocoalesce

save "..\cleaned_data\sbs_nace_1990-2017", replace

// clean-up temp folder
cd "$temp"
local filelist: dir . files "*"
foreach file in `filelist' {
  erase "`file'"
}



