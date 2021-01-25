// SBS Controls Master file

// Get SBS data for 2000-2007 years, nace1, convert to nace2 using the conversion table
do "$codes/controls/Controls_SBS_Nace1"

// Get SBS data for after 2008, append earlier data 
do "$codes/controls/Controls_SBS_Nace2"
