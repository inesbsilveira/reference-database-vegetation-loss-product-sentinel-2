// Get unique id_gleba values 
var uniqueGlebas = nvg_dates.distinct('id_gleba'); 
  
// Initialize a list to accumulate all results 
var allResults = ee.FeatureCollection([]); 
  
// Define a function to process each id_gleba 
var processGleba = function(glebaFeature) { 
  var id_gleba = glebaFeature.get('id_gleba'); 
  
  // Filter the nvg_dates table for the current id_gleba 
  var nvg_talhao = nvg_dates.filter(ee.Filter.eq('id_gleba', id_gleba)); 
  
  // Get the first feature to obtain start and end dates 
  var feature = nvg_talhao.first(); 
  var startdate = feature.get('start_date'); 
  var enddate = feature.get('end_date'); 
  
  // Cloud Score+ image collection 
  var csPlus = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED'); 
  var QA_BAND = 'cs_cdf'; 
  var CLEAR_THRESHOLD = 0.65; 
  
  // Load Sentinel-2 TOA reflectance data 
  var dataset = ee.ImageCollection('COPERNICUS/S2_HARMONIZED') 
                    .filterDate(startdate, enddate) 
                    .filterBounds(nvg_talhao.geometry()) 
                    .linkCollection(csPlus, [QA_BAND]) 
                    .map(function(image) { 
                      return image.updateMask(image.select(QA_BAND) 
                      .gte(CLEAR_THRESHOLD)) 
                      .set('system:time_end', image.get('system:time_end')) 
                      .set('system:time_start', image.get('system:time_start')) 
                      .set('system:id', image.get('system:id')) 
                      .set('system:version', image.get('system:version')) 
                      .set('system:asset_size', image.get('system:asset_size')) 
                      .set('system:footprint', image.get('system:footprint')) 
                      .set('system:index', image.get('system:index')); 
                    }); 
  
  // Add NDVI band 
  var addNDVI = function(image) { 
    var ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI'); 
    return image.addBands(ndvi); 
  }; 
  
  // Apply across the whole collection  
  var S2_NDVI = dataset.map(addNDVI); 
  
  // Calculate median NDVI for each image and each polygon 
  var calculateMedianNDVI = function(image) { 
    var medianNDVI = image.select('NDVI').reduceRegions({ 
      collection: nvg_talhao, 
      reducer: ee.Reducer.median(), 
      scale: 10 
    }); 
    return medianNDVI.map(function(feature) { 
      return ee.Feature(feature).set('date', image.date().format('YYYY-MM-dd')); 
    }); 
  }; 
  
  var medianNDVI = S2_NDVI.map(calculateMedianNDVI).flatten(); 
  
  // Add properties to the median NDVI features 
  var medianNDVIWithProperties = medianNDVI.map(function(feature) { 
    var properties = feature.toDictionary(); 
    return ee.Feature(feature.geometry(), properties); 
  }); 
  
  // Accumulate the results in the allResults collection 
  allResults = allResults.merge(medianNDVIWithProperties); 
}; 
  
// Map the processGleba function over all unique id_gleba values 
uniqueGlebas.evaluate(function(glebasList) { 
  glebasList.features.forEach(function(gleba) { 
    processGleba(ee.Feature(gleba)); 
  }); 
  
  // Export the combined results as a single CSV file 
  Export.table.toDrive({ 
    collection: allResults, 
    description: 'Median_NDVI_Per_Polygon_All_Glebas', 
    folder: 'Navigator_NDVI_median', 
    fileFormat: 'CSV', 
    selectors: ['id', 'cod_talhao', 'cod_ug', 'date', 'id_gleba', 'median'] 
  }); 
}); 
