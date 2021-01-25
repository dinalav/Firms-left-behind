// adding trade controls
global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$temp"

// Concordance tables from https://wits.worldbank.org/es/product_concordance.html
* H1 -->  NACE1 --> NACE2 * 

import excel "$data\H1toNace.xlsx", sheet("NACE Rev1 - HS 1996") firstrow clear

ren (HS NACE) (H1 nace1)
keep H1 nace1

destring nace1, ignore(".") replace
destring H1, replace

bys H1: gen n = _n // as it is not 1 to 1, take the mode
bys H1: egen mode = mode(nace1), minmode
keep H1 mode
duplicates drop

save "$temp/H1_nace1", replace

// checking to have one on one correspondence
// source: Eurostat correspondence tables, 4 digit
/*
use "$help/nace1nace2", replace

keep nace4*
ren (nace4_1 nace4) (nace1 nace2)

bys nace1: gen n = _n
tab n
drop n
save, replace
*/

use "$data\Comtrade_MAIN.dta", clear
keep Year Trade_Flow Reporter_ISO Partner_ISO Commodity_Code Trade_Value

ren Commodity_Code H1
merge m:1 H1 using "$temp/H1_nace1"
keep if _merge == 3
drop _merge

tab Reporter_ISO
tab Partner_ISO // for some reason Romania is missing among Reporting countries

ren mode nace1
merge m:1 nace1 using "$help/nace1nace2"
keep if _merge == 3
drop _merge

tostring nace2, replace
replace nace2 = substr(nace2, 1, 2)
destring nace2, replace

collapse (sum) Trade_Value, by(Year Trade_Flow Reporter Partner nace2)

ren Reporter_ISO iso316613 
tab iso316613
replace iso316613 = "ROM" if iso316613 == "ROU"
merge m:1 iso316613 using "$help/countries_short"

drop if _merge == 2
drop _merge

ren iso316612 origin
drop iso316613

ren Partner_ISO iso316613
replace iso316613 = "ROM" if iso316613 == "ROU"
merge m:1 iso316613 using "$help/countries_short"
drop if _merge == 2
drop _merge
ren iso316612 dest
drop iso31

ren Trade_Value trade
ren Year year

reshape wide trade, j(Trade_Flow) i(year origin dest nace) string

save "trade", replace

// the data is symmetric: destination - origins, but does not repeat, e.g. there
// is only data on SE-GB but not on GB-SE

use "$help/cntry_nace_years", clear
drop if origin == dest

keep if inrange(year, 2000, 2018)
merge 1:1 origin dest nace2 year using "trade", keepusing(tradeImport)
drop if _merge == 2
drop _merge

ren (origin dest) (dest origin)
merge 1:1 origin dest nace2 year using "trade", keepusing(tradeExport)
drop if _merge == 2
drop _merge 
ren (dest origin) (origin dest)
replace tradeImport = tradeExport if mi(tradeImport) // using symmetric information

drop tradeExport
merge 1:1 origin dest nace2 year using "trade", keepusing(tradeExport)
drop if _merge == 2
drop _merge

ren tradeImport Import
ren (origin dest) (dest origin)
merge 1:1 origin dest nace2 year using "trade", keepusing(tradeImport)
drop if _merge == 2
drop _merge 
ren (dest origin) (origin dest)
replace tradeExport = tradeImport if mi(tradeExport)
ren tradeExport Export
drop tradeImport

foreach x in dest origin {
replace `x' = "GB" if `x' == "UK"
}

// convert to million EUR // 
// https://sdw.ecb.europa.eu/browseTable.do?org.apache.struts.taglib.html.TOKEN=f9f740bf34d6cbff98f28e3c6d29263e&df=true&MAX_DOWNLOAD_SERIES=500&DATASET=0&org.apache.struts.taglib.html.TOKEN=5ccfed7bec05c36830ab40c9438d547f&node=BASKET572834064&SERIES_MAX_NUM=50&activeTab=EXR&start=01-01-2000&end=31-12-2018&submitOptions.x=0&submitOptions.y=0&trans=AF&q=&type=
merge m:1 year using "$help/usd_eur"
drop _merge

replace Export = Export/usd_eur/1000000
replace Import = Import/usd_eur/1000000
sum Export Import

replace nace2 = 43 if inrange(nace2, 41,43)
replace nace2 = 88 if inrange(nace2, 87,88)
replace nace2 = 99 if inrange(nace2, 98,99)
collapse (sum) Export Import, by(nace year origin dest)

// aggregated because other datasets (skill shortages) are at such level.
save "../cleaned_data/trade_final", replace


// clean-up temp folder
cd "$temp"
local filelist: dir . files "*"
foreach file in `filelist' {
  erase "`file'"
}
