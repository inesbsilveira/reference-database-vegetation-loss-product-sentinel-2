# Development of a forest clear-cut reference database for continental Portugal at Sentinel-2 resolution
Inês Silveira - Instituto Superior de Agronomia, 2024

#### Project description
Master's Thesis Project made within the scope of the S2CHANGE project, in collaboration with Direção Geral do Território (DGT).\
The dataset utilized in this project was provided by The Navigator Company (NVG).\
The study aims to develop a robust reference database to serve as training data for a change detection algorithm that automates the creation of vegetation loss maps, particularly clear-cuts, using Sentinel-2 time series data. The final product will be a high-resolution, vector-based land cover loss product.

#### Content
- scripts: functions and main script

#### Working directory
- myfolder
  - main.py
  - my_functions_main.py
  - input_folder
    - NVG_proprios_2015_2023_clean.gpkg
  - output_folder
  - ccd
    - tiles
      - df_ccd_tile29SNB.shp
      - df_ccd_tile29SNB2.shp
      - S2_T29SNB
      - S2_T29SNB2

where "S2_{tile_name} is a folder containing the S2 images for the tile

#### Requirements
QGIS 3.34.3 or higher\
Python 3.9 or higher
