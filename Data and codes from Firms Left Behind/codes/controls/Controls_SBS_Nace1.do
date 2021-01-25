***** Controls: Structural Business Statistics *****
**** NACE1 data until 2008 ****

clear
set more off

*** set directory
global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$temp"

*** download SBS data from Eurostat dirs: 
foreach file in sbs_na_2a_mi sbs_na_2a_dade sbs_na_2a_dfdn sbs_na_2a_el sbs_na_4a_co sbs_na_3b_tr sbs_na_1a_se {
copy "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F`file'.tsv.gz" "`file'.tsv.gz", replace
}
// NACE datasets: C, DA-DE, DF-DN, E, F, G, H-K

// unzip the files 
foreach file in sbs_na_2a_mi sbs_na_2a_dade sbs_na_2a_dfdn sbs_na_2a_el sbs_na_4a_co sbs_na_3b_tr sbs_na_1a_se {
shell "C:\Program Files\7-Zip\7zG.exe" e -y `file'.tsv.gz   // this line calls in the 7-Zip using the Stata shell command. As part of 7-zip syntax, e stands for extract and -y for replace file.
}

*** combine all SBS datasets:
// save as dta
foreach file in sbs_na_2a_mi sbs_na_2a_dade sbs_na_2a_dfdn sbs_na_2a_el sbs_na_4a_co sbs_na_3b_tr sbs_na_1a_se {
import delimited  "`file'.tsv", varnames(1) clear 
save "`file'", replace
}

// append all datasets into one
foreach file in sbs_na_2a_mi sbs_na_2a_dade sbs_na_2a_dfdn sbs_na_2a_el sbs_na_4a_co sbs_na_3b_tr  {
append using  "`file'.dta" 
}

*** reformat and clean SBS data:
// Need to split the first variable (contains categories)
split nace, p(,) gen(var)
order var*
rename (var1 var2 var3) (nace indicator ctry)
drop nace_r1indic_sbgeotime

// Rename v2-v20 to contain years (recorded in a label)
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

// destring remaining observations
destring time1998 time1999 time2005 time2006 time2007, replace force 
sum time* // check

// drop duplicates
bys ctry indicator nace: gen n=_N
tab n
drop n
duplicates drop
bys ctry indicator nace: gen n=_N
tab n
drop n

// reshape to long format - to have year as another column
reshape long time, i(ctry indicator nace) j(year)
reshape wide time, i(ctry year nace) j(indicator) string

renvars, subst("time" "")
replace ctry = "GB" if ctry == "UK"
replace ctry = "GR" if ctry == "EL"

// Restrict to NACE R1 indicators of interest
keep ctry nace year V11110 V12110 V12150 V13310 V13320 V15110 V16130 V91110 V91120

// Rename indicator variables to NACE R1 categories
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

tab nace
keep if strlen(nace) == 3

// Rename nace variable to NACE1 
gen nace2_r1 = substr(nace,2,.)
tab nace2
destring nace2, replace 
tab nace2
drop if mi(nace2)
ren ctry iso316612
merge m:1 iso316612 nace2_r1 using "$help/nace1to2_convert.dta"
tab iso if _merge == 1 // not the relevant countries
drop if _merge == 2
drop _merge

reshape long nace2_r2_, i(iso nace2_r1 year) j(nace2)
drop if mi(nace2_r2_)

foreach x of varlist num_enter - lab_prod_wage {
replace `x' = `x'*nace2_r2
}

collapse (sum) num_enter-lab_prod_wage, by(year nace2 iso)
foreach x of varlist num_enter-lab_prod_wage {
replace `x' = . if `x' == 0 // compressing puts missing to zeros
}


compress, nocoalesce

save "sbs_nace1_1990-2008", replace
