// prepares the conversion tables 

global codes = "$path\codes"
global help = "$path\cleaned_data\help"
global temp = "$path\temp"

cd "$help"

shell "C:\Program Files\7-Zip\7zG.exe" e -y NACEmaps.rar   // this line calls in the 7-Zip using the Stata shell command. As part of 7-zip syntax, e stands for extract and -y for replace file.

shell ren "CZECH REPUBLIC1to2.csv" "CZECHREPUBLIC1to2.csv"
shell ren "CZECH REPUBLIC2to1.csv" "CZECHREPUBLIC2to1.csv"

foreach x in BULGARIA CZECHREPUBLIC HUNGARY CROATIA ESTONIA LITHUANIA LATVIA ///
POLAND ROMANIA SLOVENIA SLOVAKIA ///
AUSTRIA BELGIUM DENMARK FINLAND FRANCE GERMANY GREECE ICELAND IRELAND ITALY ///
NETHERLANDS NORWAY PORTUGAL SPAIN SWEDEN SWITZERLAND UNITED_KINGDOM {
import delimited "`x'1to2.csv", clear
ren (nacerev11primarycode nacerev2primarycode) (nace2_r1 nace2)
reshape wide share, i(nace2_r1) j(nace2)
renvars, subst("share" "nace2_r2_")
gen countryname = strlower("`x'")
save `x', replace
}
use BULGARIA, clear
foreach x in CZECHREPUBLIC HUNGARY CROATIA ESTONIA LITHUANIA LATVIA ///
POLAND ROMANIA SLOVENIA SLOVAKIA ///
AUSTRIA BELGIUM DENMARK FINLAND FRANCE GERMANY GREECE ICELAND IRELAND ITALY ///
NETHERLANDS NORWAY PORTUGAL SPAIN SWEDEN SWITZERLAND UNITED_KINGDOM {
append using `x'
}
order countryname nace2_r1
gen commonname = strupper(substr(countryname,1,1)) + substr(countryname,2,.)
merge m:1 commonname using countries, keepusing(iso316612)
drop if _merge == 2
tab commonname if _merge ==1
replace iso316612 = "CZ" if mi(iso316612)
replace iso316612 = "GB" if commonname == "United_kingdom"
tab iso
drop *name _merge
save nace1to2_matrix, replace


// for some countries-industries there is no conversion available (.e.g there was not enough information from the firms)
// in such cases, use a simple conversion table (same for all countries): it used mapping at a four digit level, collapsed at a two digit level and 
// simply used most occuring industries to convert
use nace1to2, replace

gen w = 1
reshape wide w, i(nace1) j(nace2)
renvars, subst("w" "simple_nace2_r2_")
save "$help/nace1to2_simple", replace

// make sure to have all nace rev 1 industries for all countries
use "$help/cntry_nace_years", replace
keep nace2 origin 
do "$codes/help_codes/country_to_region"
tab origin_r
keep if origin_r < 5 | mi(origin_r) // drop OtherEurope
bys origin_r: tab origin
drop if origin == "MT" | origin == "CY" // not in the analysis

ren origin iso316612
duplicates drop
ren nace2 nace1 
merge m:1 nace1 using "$help/nace1to2_simple"
tab nace1 if _merge == 1 // non-existing nace1  
keep if _merge == 3 // to keep only the existing nace1 industries 
drop _merge 

ren nace1 nace2_r1
merge m:1 nace2_r1 iso316612 using "$help/nace1to2_matrix"
tab nace2_r1 if _merge == 2 // non-relevant industries
drop if _merge == 2
foreach x of varlist nace2_r2_*  {
cap gen simple_`x' = `x'
replace `x' = simple_`x' if _merge == 1 
}

foreach x of varlist simple_nace2_r2_*  {
cap renvars `x', subst("simple_" "") // creates missing industries in nace2_r2_
}
drop simple*
drop _merge

// check dimension and rowsum of nace4_2r == 1
bys iso nace2_r1: gen n = _N
tab n
drop n

egen ch = rowtotal(nace2_r2_*)
sum ch
tab ch // should be all equal to 1 
count

keep iso nace2_r1 nace2_r2*

save "$help/nace1to2_convert", replace

// clean-up the directory 
local filelist: dir . files "*.csv"
foreach file in `filelist' {
  erase "`file'"
}

foreach x in BULGARIA CZECHREPUBLIC HUNGARY CROATIA ESTONIA LITHUANIA LATVIA ///
POLAND ROMANIA SLOVENIA SLOVAKIA ///
AUSTRIA BELGIUM DENMARK FINLAND FRANCE GERMANY GREECE ICELAND IRELAND ITALY ///
NETHERLANDS NORWAY PORTUGAL SPAIN SWEDEN SWITZERLAND UNITED_KINGDOM {
erase `x'.dta
}




 












