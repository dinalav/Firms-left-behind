// labour shortages
// Source: EU Commision Business Survey 
// https://ec.europa.eu/info/business-economy-euro/indicators-statistics/economic-databases/business-and-consumer-surveys/download-business-and-consumer-survey-data/subsector-data_en

global data = "$path\raw_data"
global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$data"


// *** manufacturing

set more off
foreach ind of numlist 10/33 {
import excel "$data/industry_subsectors_sa_q8_nace2.xlsx", sheet(`ind') firstrow clear

rename A date
keep date *F3SQ *F6SQ *F2SQ *F4SQ
drop if mi(date)
save bs`ind', replace

import excel "$data/industry_subsectors_sa_q_nace2.xlsx", sheet(`ind') firstrow clear
rename A date
keep date *13QPSQ *15BSQ
drop if mi(date)
merge 1:1 date using bs`ind'
drop _merge
save bs`ind', replace

renvars, presub("INDU" "")
drop EU_2019`ind'13QPSQ EU_2019`ind'15BSQ EU_2019`ind'8F2SQ ///
EU_2019`ind'8F3SQ EU_2019`ind'8F4SQ EU_2019`ind'8F6SQ

renvars *F3SQ, prefix("ls")
renvars *F6SQ, prefix("fs")
renvars *F2SQ, prefix("de")
renvars *F4SQ, prefix("eq")
renvars *13QPSQ, prefix("cu")
renvars *15BSQ, prefix("co")

renvars, trim(4)

foreach x of varlist fs* ls* de* eq* co* cu* {
//replace "NA" = "" if _n[1] == ["NA"]
destring `x', replace force
}
reshape long ls fs de eq co cu, i(date) j(iso316612) string

gen nace2 = `ind'
order nace2, after(iso*)
save bs`ind', replace
}


// *** services  
foreach ser of numlist 49/53 55/56 58/66 68/75 77/82 90/96 {
set more off 
import excel "$data/services_subsectors_sa_q_nace2.xlsx", sheet(`ser') firstrow clear
rename A date
keep date *F3SQ *F5SQ *F2SQ *F4SQ *QPSQ
drop if mi(date)
renvars,  presub("SERV" "")
renvars, subst("`ser'7" "")
renvars, subst("`ser'8" "")
renvars *F3SQ, prefix("ls")
renvars *F5SQ, prefix("fs")
renvars *F2SQ, prefix("de")
renvars *F4SQ, prefix("eq")
renvars *QPSQ, prefix("cu")

foreach x in F3SQ F5SQ F2SQ F4SQ QPSQ {
renvars, postsub("`x'" "")
}


set more off
foreach x of varlist fs* ls* de* eq* cu* {
//replace "NA" = "" if _n[1] == ["NA"]
destring `x', replace force
}
reshape long ls fs de eq cu, i(date) j(iso316612) string

gen nace2 = `ser'
order nace2, after(iso*)
save bs`ser', replace
}


// *** construction, nace 43, use total
// https://ec.europa.eu/info/business-economy-euro/indicators-statistics/economic-databases/business-and-consumer-surveys/download-business-and-consumer-survey-data/time-series_en#construction
import excel "$data/building_total_sa_nace2.xlsx", sheet("BUILDING MONTHLY") firstrow clear
rename A date
keep date *F4SM *F7SM *F2SM *F5SM
renvars,  presub("BUIL" "")
renvars, subst("TOT2" "")
renvars *F4SM, prefix("ls")
renvars *F7SM, prefix("fs")
renvars *F2SM, prefix("de")
renvars *F5SM, prefix("eq")

foreach x in 2 4 5 7 {
renvars, postsub("F`x'SM" "")
}

set more off
foreach x of varlist fs* ls* de* eq* {
//replace "NA" = "" if _n[1] == ["NA"]
destring `x', replace force
}

drop if missing(date) // to delete missing observations 
reshape long ls fs de eq, i(date) j(iso316612) string

gen nace2 = 43 // it is aggregated over all construction sectors 39/43 (to take into account!!!!), corresponds to isic2=45
order nace2, after(iso*)

//convert from monthly to quarterly
g year = year(date)
g month = month(date)

sort iso316612 date
foreach x in ls fs eq de {
bysort iso* year: gen `x'_q = (`x' + `x'[_n-1] + `x'[_n-2])/3 if _n >2 
}
keep if month == 3 | month == 6 | month == 9 | month == 12
replace month = month/3
drop date
drop if year < 1990
rename month quarter
drop ls fs eq de
renvars, postsub("_q" "")
save bs43, replace


// *** merging together industry and service, construction comes later due //
//  to diff year aggregation
use bs10, clear
foreach x of numlist 11/33 49/53 55/56 58/66 68/75 77/82 90/96 {
append using bs`x', force
save bs_all, replace
}

g year = substr(date, 1, 4)
g quarter = substr(date, 7,7)
destring year quarter, replace
drop date

append using bs43, force
rename iso* eucode
save, replace

// *** create yearly date (average over 4 quarters)
use "bs_all", clear
sort nace2 eucode year quarter
foreach x in ls fs cu co de eq {
replace `x' = . if `x' <0
}
collapse (mean) ls fs cu co de eq, by(eucode nace2 year)
foreach x in ls fs cu co de eq {
replace `x' = 100 if `x' >100 & !mi(`x')
replace `x' = `x'/100
}
sum ls fs
ren eucode iso316612
replace iso = "GB" if iso == "UK"
replace iso = "GR" if iso == "EL"
tab nace2
save "..\cleaned_data\bs_all", replace


// clean up
foreach x of numlist 10/33 43 49/53 55/56 58/66 68/75 77/82 90/96 {
erase bs`x'.dta
}
erase bs_all.dta




