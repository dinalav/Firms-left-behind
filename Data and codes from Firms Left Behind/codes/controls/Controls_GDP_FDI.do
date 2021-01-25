***** Controls: GDP and FDI data *****
set more off

*** set directory
global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$data"

*** download GDP data from Eurostat dirs: 
foreach file in sdg_08_10 {
copy "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F`file'.tsv.gz" "`file'.tsv.gz", replace
}
// data is Real GDP per capita
// GDP at market prices
// chain linked volume (2010), euro per capita
// all EFTA countries
// time period: 2000-2019

// unzip the file 
foreach file in sdg_08_10  {
shell "C:\Program Files\7-Zip\7zG.exe" e -y `file'.tsv.gz   // this line calls in the 7-Zip using the Stata shell command. As part of 7-zip syntax, e stands for extract and -y for replace file.
}


*** download FDI data from World Bank directly through Stata tool:
// ssc install wbopendata
wbopendata, indicator(BX.KLT.DINV.CD.WD) clear long
// Foreign direct investment, net inflows (BoP, current US$)
// all countries worldwide
// time period: 1960-2019
// data already in long format
save "fdi_data_wb", replace


*** reformat and clean GDP data:
import delimited "sdg_08_10.tsv", varnames(1) clear

// Need to split the first variable (contains categories)
split unit, p(,) gen(var)
order var*
rename (var1 var2 var3) (unit B1 ctry)
drop unitna B1

// Rename v2-v35 to contain years (recorded in a label)
foreach var of varlist v* {
   local x : var label `var' // saves the label of a variable to a local
   rename `var' time`x' // uses this local to rename the variable
}

*another useful command to rename variables in a fast way
*findit renvars if not yet installed
renvars, subst("time" "gdp") 

// clean values
foreach var of varlist gdp* {
foreach x in i b p e : r d a { // check which non-numeric flags appear
cap replace `var' = substr(`var', 1, strpos(`var', "`x'")-1) if ///
strpos(`var', "`x'") > 0 
}
destring `var', replace
}
sum gdp* // check

// reshape to long format - to have year as another column
reshape long gdp, i(ctry unit) j(year)
replace unit = "_abs" if unit == "CLV10_EUR_HAB"
replace unit = "_gr" if unit == "CLV_PCH_PRE_HAB"
reshape wide gdp, i(ctry year) j(unit) string

replace ctry = "GB" if ctry == "UK"
replace ctry = "GR" if ctry == "EL"
ren ctry origin

drop if mi(origin)
bys origin year: keep if _n == 1
encode origin, gen(cc)
xtset cc year 
replace gdp_abs = L.gdp_abs if mi(gdp_abs)
ren origin ctry
drop cc

save "gdp", replace


*** reformat and clean FDI data:
use "fdi_data_wb", replace

// rename FDI variable and drop if year < 2000
ren bx_klt_dinv_cd_wd fdi_abs
label variable fdi_abs "FDI Net Inflows"
drop if year < 2000

// drop and fill missing values
drop if mi(countrycode)
bys countrycode year: keep if _n == 1
encode countrycode, gen(cc)
xtset cc year 
replace fdi_abs = L.fdi_abs if mi(fdi_abs)
drop cc

// merge with two-digit country codes
*World Bank uses three-digit country codes, while Eurostat uses two-digit country codes
ren countrycode iso316613
ren countryname ctryname
merge m:m iso316613 using "$help/countries"
// merge based on variable iso316613 (three-digit country codes)
// add the variable iso316612 (two-digit country codes) to the dataset
drop if _merge == 2
drop _merge
ren iso316612 ctry


*** 1-to-1 merge of GDP and FDI data sets 
bys ctry year: gen n=_N
tab n
drop if mi(ctry)
drop n
merge 1:1 ctry year using "gdp"
tab ctry if _merge == 1
drop if _merge == 1
keep ctry year gdp_abs gdp_gr fdi_abs

ren ctry origin
order origin year gdp_abs gdp_gr fdi_abs

ren origin iso316612

compress, nocoalesce

save "..\cleaned_data\controls_gdp_fdi", replace

// clean raw
local filelist: dir . files "*.tsv"
foreach file in `filelist' {
  erase "`file'"
}
local filelist: dir . files "*.gz"
foreach file in `filelist' {
  erase "`file'"
}

// clean temp 
cd "$temp"
local filelist: dir . files "*"
foreach file in `filelist' {
  erase "`file'"
}
