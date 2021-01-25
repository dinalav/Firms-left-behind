// Firms Left Behind: Emigration and Firm Productivity
// Codes used with publicly available data 

/* Before running set the path to the folder here
global path = "..\Data and codes from Firms Left Behind"

*/

/* Contents of the cleaned_data folder
help folder
	nace1to2_convert.dta -- conversion from nace rev 1 to rev 2, at two digit level, 
		see Controls_SBS_Nace1 how to apply
	countries and countries_short -- official country names and codes 
	nace1nace2 -- conversion at four digit level
	usd_eur -- convert USD to EUR (for trade data)
	isco_desc -- occupation codes and assignment to skill level or patent potential

OECD_data, ESTAT_data -- migration stocks and flows from OECD and Eurostat at origin-dest-year level
bilateral_migration_data -- migration stocks at origin-dest-year level (OECD and Eurostat combined to avoid missing)
migration_industry_proxy -- derived data on migration stocks at origin-dest-nace rev 2 - year level

bs_all - labour shortages 

controls_gdp_fdi

sbs_nace_1990-2017 - Structural business statistics at country-nace rev 2 -year level 

trade_final - export and import data for EU MS at origin-dest-nace rev 2-year level
*/

// Help files 
* Correspondence table between Nace rev 1 and Nace rev 2 at a two digit level
do "$path/codes/help_codes/nace_conversion.do"

// Controls and examples 
* 1. Migration stocks and inflows from OECD and Eurostat
* proxy data by origin-dest-nace2-year
do "$path/codes/controls/migration_bilateral"

* 2. Controls GDP and FDI 
* iso316612-year level //iso316612 - two digit country code
do "$path/codes/controls/Controls_GDP_FDI"

* 3. Labour shortages 
* iso316612-nace2-year level
do "$path/codes/controls/labour_shortages"

* 4. Structural business statistics 
* iso316612-nace2-year level
do "$path/codes/controls/Controls_SBS_Master"

* 5. Trade data
// NOTE: Comtrade_MAIN data is very heavy, therefore it wasn't uploaded on Github
// see directly cleaned_data/trade_final 
do "$path/codes/controls/Comtrade_Main"

