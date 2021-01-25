global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$data"

// Migration data from OECD
// install sdmxuse package to access the data directly
// findit sdmxuse 
// to check the structure of the data
sdmxuse datastructure OECD, clear dataset(MIG)
// save labels for variables
keep if concept == "VAR"
keep code*
ren code VAR
ren code_lbl label
save OECD_labels, replace

// download the dataset 
sdmxuse data OECD, clear dataset(MIG) // takes some time
ren var VAR 
merge m:1 VAR using OECD_labels 
drop _merge

keep if strpos(label, "Stock") | strpos(label, "Inflows")
tab gen
keep if gen == "TOT"
drop gen
ren co2 iso316613 
merge m:1 iso316613 using "$help/countries_short"
tab iso316613 if _merge == 1
replace iso316612 = "RO" if iso316613 == "ROU"
ren iso316612 origin
drop iso316613 _merge

ren cou iso316613
merge m:1 iso316613 using "$help/countries_short"
tab iso316613 if _merge == 1
replace iso316612 = "RO" if iso316613 == "ROU"
ren iso316612 dest
drop iso316613 _merge

do "$codes/help_codes/country_to_region"
tab origin if inrange(origin_r, 2,3)
tab dest if dest_r == 1 | dest_r == 4
*keep if inrange(origin_r, 2,3) & (dest_r == 1 | dest_r == 4)

drop if mi(time)
tab origin
tab dest // all except for LI

cap drop ind
gen ind = ""
foreach x in asylum population workers seasonal {
replace ind = "inflows_`x'" if strpos(label, "`x'") & strpos(label, "flows")
}
tab ind
foreach x in birth nationality {
replace ind = "stocks_`x'" if strpos(label, "`x'") & strpos(label, "Stock")
}
tab ind
drop VAR label
drop if mi(origin, dest)
reshape wide value, i(dest dest_r origin origin_r time) j(ind) string
renvars, subst("value" "")
pwcorr inflows*
pwcorr stock*
sum inflows* stock*

// take population by birth as the default, replace with nationality if missing
egen ch = mean(stocks_birth/stocks_nationality)
sum ch
replace stocks_birth = ch*stocks_nationality if mi(stocks_birth)
drop ch

ren time year
destring year, replace
renvars stocks* inflows*, postfix("_oecd")
compress, nocoalesce

save "../cleaned_data/OECD_data", replace

// fetch eurostat bilateral migration data
cd
foreach file in migr_pop1ctz migr_pop3ctb {
copy "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=data%2F`file'.tsv.gz" "`file'.tsv.gz", replace
}

// find location of 7zip programme on your computer
foreach file in migr_pop1ctz migr_pop3ctb {
shell "C:\Program Files\7-Zip\7zG.exe" e -y `file'.tsv.gz   // this line calls in the 7-Zip using the Stata shell command. As part of 7-zip syntax, e stands for extract and -y for replace file.
}
*tsv - is similar to csv but tab-separated instead of comma-separated, can be opened in excel

import delimited "migr_pop3ctb.tsv", varnames(1) clear

// Need to split the first variable (contains categories)
split c_birth, p(,) gen(var)
order var*
rename (var1 var2 var3 var4 var5) (origin age unit sex dest)
drop c_birth*

// Rename v2-v35 to contain years (recorded in a label)
foreach var of varlist v* {
   local x : var label `var' // saves the label of a variable to a local
   rename `var' time`x' // uses this local to rename the variable
}

renvars, subst("time" "stock") 

// clean values
foreach var of varlist stock* {
foreach x in i b p e : r d a { // check which non-numeric flags appear
cap replace `var' = substr(`var', 1, strpos(`var', "`x'")-1) if ///
strpos(`var', "`x'") > 0 
}
destring `var', replace
}
sum stock* // check

// keep total 
keep if age == "TOTAL"
keep if sex == "T"
drop sex age unit

// reshape to long format - to have year as another column
reshape long stock, i(origin dest) j(year)

// distribution of migrants by destinations in the EU
foreach x in origin dest {
replace `x' = "GR" if `x' == "EL"
replace `x' = "GB" if `x' == "UK"
}
do "$codes/help_codes/country_to_region"
tab dest if dest_region == 1
tab dest if dest_region == 4
tab origin if inrange(origin_r, 2, 3)

sort origin dest year
ren stock population_by_birth_ESTAT

save ESTAT_data_birth, replace

// the same with nationality data
import delimited "migr_pop1ctz.tsv", varnames(1) clear

// Need to split the first variable (contains categories)
split citizen, p(,) gen(var)
order var*
rename (var1 var2 var3 var4 var5) (origin age unit sex dest)
drop citizen*

// Rename v2-v35 to contain years (recorded in a label)
foreach var of varlist v* {
   local x : var label `var' // saves the label of a variable to a local
   rename `var' time`x' // uses this local to rename the variable
}

renvars, subst("time" "stock") 

// clean values
foreach var of varlist stock* {
foreach x in i b p e : r d a { // check which non-numeric flags appear
cap replace `var' = substr(`var', 1, strpos(`var', "`x'")-1) if ///
strpos(`var', "`x'") > 0 
}
destring `var', replace
}
sum stock* // check

// keep total 
keep if age == "TOTAL"
keep if sex == "T"
drop sex age unit

// reshape to long format - to have year as another column
reshape long stock, i(origin dest) j(year)

// distribution of migrants by destinations in the EU
foreach x in origin dest {
replace `x' = "GR" if `x' == "EL"
replace `x' = "GB" if `x' == "UK"
}
do "$codes/help_codes/country_to_region"
tab dest if dest_region == 1
tab dest if dest_region == 4
tab origin if inrange(origin_r, 2, 3)

sort origin dest year
ren stock population_by_nation_ESTAT

save ESTAT_data_nation, replace

merge 1:1 dest origin year using ESTAT_data_birth
drop _merge 

pwcorr population*
cap drop ch
egen ch = mean(population_by_birth/population_by_nation)
tab ch
gen population_total = population_by_birth
replace population_total = ch*population_by_nation if mi(population_by_birth)
drop ch
label var population_total "stocks by birth, missing filled with nation"
save "../cleaned_data/ESTAT_data", replace

// have a list of all the relevant country pairs and years
// for creation: see codes/instrument/distances
use "$help/cntry_nace_years", replace
keep origin dest year 
duplicates drop
keep if inrange(year, 2000, 2018)
do "$codes/help_codes/country_to_region"
tab origin
tab dest

merge 1:1 dest origin year using ../cleaned_data/ESTAT_data
drop _merge

merge 1:1 dest origin year using ../cleaned_data/OECD_data
drop _merge 

pwcorr population*
sum population*

gen immigrants_by_birth = population_total
replace immigrants = stocks_birth_oecd if mi(immigrants)

count
count if mi(immigrants)

egen cc = group(dest origin)
xtset cc year

// linearly interpolate if values are missing in-between 
replace immigrants = . if immigrants == 0
cap drop immigrants_ipol
bys cc: ipolate immigrants year, gen(immigrants_ipol)
assert immigrants_by_birth == immigrants_ipol if !mi(immigrants_by_birth)

// for missing boundary values: extrapolate backward, using average migration growth rates for 
// a given country pair
xtset cc year
gen rate = immigrants_ipol/l.immigrants_ipol
bys cc: egen mrate = mean(rate)

foreach x of numlist 1/14 {
replace immigrants_ipol = f.immigrants_ipol/mrate if mi(immigrants_ipol)
}
count if mi(immigrants_ipol) 
drop cc rate mrate
label var immigrants_ipol "Stock of immigrants, interpolated"
label var immigrants_by_birth "Stock of immigrants"

save ../cleaned_data/bilateral_migration_data, replace

// get the distribution by industries: by year, destination and region of origin
// if for a certain destination such information is missing, will replace with average EU for that year

// Source: custom extraction from Eurostat, through the User Support group
import delimited "employment_ind_origin_ESTAT", clear 
tab year 

destring na112d nace2d, replace force
ren (country countryb na112d nace2d) (dest origin nace2_r1 nace2)

collapse (sum) value, by(dest origin nace2_r1 nace2 year)
replace value = . if value == 0

foreach x in dest {
replace `x' = "GR" if `x' == "EL"
replace `x' = "GB" if `x' == "UK"
}
tab dest 

preserve
drop nace2_r1
drop if mi(nace2)
save "$temp/employment_indNace2", replace
restore

// convert nace rev 1 to rev 2 
drop nace2
drop if mi(nace2_r1)
do "$codes/help_codes/country_to_region"
keep if dest_r == 1 | dest_r == 4
gen iso316612 = dest
tab iso
merge m:1 iso316612 nace2_r1 using "$help/nace1to2_convert.dta"
tab nace2_r1 if _merge == 1 // not relevant
tab iso if _merge == 1 & nace2_r1 > 5
keep if _merge == 3
drop _merge iso

reshape long nace2_r2_, i(dest origin nace2_r1 year) j(nace2)
sum nace2_r2
drop if mi(nace2_r2)

foreach x of varlist value {
replace `x' = `x'*nace2_r2
}
// double check that weights sum to 1 
bys dest origin nace2_r1 year: egen ch = sum(nace2_r2)
tab ch
drop ch
collapse (sum) value, by(year nace2 dest origin)

save "$temp/employment_indNace1", replace 

use "$temp/employment_indNace1", replace
append using "$temp/employment_indNace2"
// aggregate construction and some other service sectors because other datasets only have these industries
replace nace2 = 43 if inrange(nace2, 41,42)
replace nace2 = 88 if inrange(nace2, 87, 88)
replace nace2 = 99 if inrange(nace2, 98, 99)

collapse (sum) value, by(year nace2 dest origin)
foreach x of varlist value {
replace `x' = . if `x' == 0 // compressing puts missing to zeros
}

// keep only EU19 countries
do "$codes/help_codes/country_to_region"
*keep if dest_r == 1 | dest_r == 4
tab dest // again no LI

replace origin_r = 2 if strpos(origin, "2004")
replace origin_r = 3 if strpos(origin, "2007")
tab origin if origin_r == 2
tab year if !mi(value)

// use distribution by year, origin, dest, industry - when available
cap drop tot* share*
drop if nace2 < 10
bys origin dest year: egen tot_employed = sum(value)
bys origin dest year: egen count = count(value) // how many observations available
replace tot_employed = . if tot_employed == 0 | count < 10
gen share_emp = value/tot_employed
replace share_emp = 0 if mi(share_emp) & !mi(tot_employed)
tab share_emp if tot_employed == .
bys origin dest year: egen ch = sum(share_emp)
tab ch // should be either 0 or 1 
drop ch
drop origin 
preserve 
drop if mi(origin_r)
save "$temp/employment_shares", replace
restore

// for fully missing data, replace with average by EU
collapse (sum) value, by(year nace2)
bys year: egen tot_empEU = sum(value)
gen share_empEU = value/tot_empEU
bys year: egen ch = sum(share_empEU)
tab ch
drop ch
save "$temp/employment_sharesEU", replace

// now merge the datasets together
// start with the list of all nace, then expand to 2000-20018, 
// then to origin 2 and 3, then to 18 destinations.
use "$temp/employment_sharesEU", replace
keep nace2
duplicates drop

expand 19
bys nace2: gen year = 1999 + _n
tab year

expand 2
bys nace2 year: gen origin_region = _n + 1 // 2 and 3 for NMS 2004 and 2007/13
tab origin_r

expand 18
gen dest = ""
global EU15 = "AT BE DE DK ES FI FR GB GR IE IT LU NL PT SE"
global EFTA = "CH IS NO"

local j = 1
foreach x in $EU15 $EFTA {
bys nace2 year origin: replace dest = "`x'" if _n == `j'
local j = `j' + 1
}
tab dest
egen cc = group(dest origin nace2)
xtset cc year
drop cc 
merge 1:m origin_r dest nace2 year using "$temp/employment_shares"
// for those where tot_employment is not missing, set share_emp to 0 in non-reported industries
bys origin_r dest year: egen tot_employed1 = sum(value)
cap drop count
bys origin_r dest year: egen count = count(value)
replace tot_employed1 = . if tot_employed1 == 0 | count < 10
replace share_emp = 0 if mi(share_emp) & !mi(tot_employed1) 
drop _merge
bys year origin_r dest: egen ch = sum(share_emp) 
tab ch // either 0 or 1 if missing, replace only if ch == 0
count

merge m:1 nace2 year using "$temp/employment_sharesEU"
drop _merge 
// for missing shares: use average EU18 employment by industry
cap drop share
gen share = share_emp
replace share = share_empEU if ch == 0
cap drop ch
bys year origin_r dest: egen ch = sum(share)
tab ch // should be equal to 1
drop ch

keep year origin_r dest nace2 share
count 
sum share 

// check how the shares sum-up
bys origin_r dest year: egen ch = sum(share)
tab ch
drop ch

save "$temp/shares_by_ind", replace

// merge with bilateral migration data
use "$temp/employment_sharesEU", replace
keep nace2
duplicates drop

expand 19
bys nace2: gen year = 1999 + _n
tab year

expand 11 
gen origin = ""
global EU10 = "CZ EE HU LT LV PL SI SK"
global EU3 = "BG HR RO"

local j = 1
foreach x in $EU10 $EU3 {
bys nace2 year: replace origin = "`x'" if _n == `j'
local j = `j' + 1
}
tab origin

expand 18
gen dest = ""
global EU15 = "AT BE DE DK ES FI FR GB GR IE IT LU NL PT SE"
global EFTA = "CH IS NO"

local j = 1
foreach x in $EU15 $EFTA {
bys nace2 year origin: replace dest = "`x'" if _n == `j'
local j = `j' + 1
}
tab dest
egen cc = group(dest origin nace2)
xtset cc year
drop cc 

do "$codes/help_codes/country_to_region"
tab origin_r
tab dest_r

// merge with bilateral migration data 
merge m:1 dest origin year using "../cleaned_data/bilateral_migration_data
drop if _merge == 2
drop _merge

merge m:1 dest origin_r nace2 year using "$temp/shares_by_ind"
drop _merge
foreach x in by_birth ipol {
gen ifmissing_`x' = immigrants_`x'*share
}
keep dest origin nace2 year ifmissing*
save "../cleaned_data/migration_industry_proxy", replace



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


