global path = "C:\Users\nadzeya\Documents\projects\HCC resubmission\Data to share"
cd "$path"

use "Migration", replace
renvars, subst("migrants" "immigrants")
ren ifmissing_ipol immigrants_Eurostat

label var immigrants_stock "Combined"
label var immigrants_collected "Only nat. offices"
label var immigrants_Eurostat "Only Eurostat/OECD"


// the same but with absolute values
egen idc2 = group(origin nace2)
xtset idc2 year

// 
foreach x in BG RO PL  {
twoway (line immigrants_stock year if origin == "`x'" & nace2 == 61, lwidth(0.7) lpattern(solid) lcolor(cranberry)) ///
( line immigrants_stock year if origin == "`x'" & nace2 == 62 , lwidth(0.7) lpattern(solid) lcolor(navy)) ///
( line immigrants_stock year if origin == "`x'" & nace2 == 63 , lwidth(0.7) lpattern(dash) lcolor(cranberry)) ///
( line immigrants_stock year if origin == "`x'" & nace2 ==27 , lwidth(0.7) lpattern(dash) lcolor(navy)), ///
title("`x'") xtitle("") ytitle("") xline(2001) ///
legend(off) 
graph save "`x'.gph", replace
}

// legend
foreach x in EU28 {
twoway (line immigrants_stock year if origin == "`x'" & nace2 == 61, lwidth(0.7) lpattern(solid) lcolor(cranberry)) ///
( line immigrants_stock year if origin == "`x'" & nace2 == 62 , lwidth(0.7) lpattern(solid) lcolor(navy)) ///
( line immigrants_stock year if origin == "`x'" & nace2 == 63 , lwidth(0.7) lpattern(dash) lcolor(cranberry)) ///
( line immigrants_stock year if origin == "`x'" & nace2 == 27 , lwidth(0.7) lpattern(dash) lcolor(navy)), ///
title("") xtitle("") ytitle("") ysc(off)  xsc(off)  ///
legend(label (1 "61") label (2 "62") ///
label (3 "63") label (4 "27") position(3) rows(2))
graph save "`x'.gph", replace
}
 
 graph combine "BG.gph" "RO.gph" "PL.gph" "EU28.gph", ycommon xcommon
