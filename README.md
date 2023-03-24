# tas_autocorrs

Make temporal autorrelations for tropics under piControl, G1, and 4xCo2 scenarios in UKESM. Contrast 'stickiness' of weather, as defined by it's grid-point temporal autocorrelation under scenarios of CO2-forced warming and solar geoengineering. 

Code pulls geoMIP and CMIP6 tas data from the CEDA archive, removes linear and seasonal trends, then calculates temproal autocorrelation in the daily tas field for a specified latitude band (e.g. -23, 23). Sub-samples of time are used to generate artificial ensemble members for the long-run single esemble member scenarios piControl and abrupt-4xCO2. 
