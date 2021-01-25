// country codes -- to classify by regions

global EU15 = "AT BE DE DK ES FI FR GB GR IE IT LU NL PT SE"
global OtherEurope = "AL BA BY MD RS RU UA ME MK"
global EU10 = "CZ EE HU CY MT LT LV PL SI SK"
global EU3 = "BG HR RO"
global EFTA = "CH IS LI NO"


gen origin_region = .
gen dest_region = .

foreach x in $EU15 {
cap replace origin_region = 1 if origin == "`x'"
cap replace dest_region = 1 if dest == "`x'"
}



foreach x in $EU10 {
cap replace origin_region = 2 if origin == "`x'"
cap replace dest_region = 2 if dest == "`x'"
}

foreach x in $EU3 {
cap replace origin_region = 3 if origin == "`x'"
cap replace dest_region = 3 if dest == "`x'"
}

foreach x in $EFTA {
cap replace origin_region = 4 if origin == "`x'"
cap replace dest_region = 4 if dest == "`x'"
}

foreach x in $OtherEurope {
cap replace origin_region = 5 if origin == "`x'"
cap replace dest_region = 5 if dest == "`x'"
}

label define region 1 "EU15" 2  "NMS10" 3 "NMS3" 4 "EFTA" 5 "OtherEurope" , replace

label value origin_region region
label value dest_region region

