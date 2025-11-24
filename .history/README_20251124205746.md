# EuroCropsV2

This repository contains supporting code and data for the **EuroCropsV2** dataset ([DOI link]).
The associated data paper can be found [here](LINK).

---

## Repository Structure

```
EuroCropsV2/
├── code/
│   ├── import_db/
│   └── processing/
└── data/
    ├── cropcodemapping/
    └── processing/
```

---

## Code

### `import_db`

Scripts for importing GeoParquet files (from the FTP) and mapping tables ([data/cropcodemapping](https://github.com/Martincccc/EuroCropsV2/tree/main/data/cropcodemapping)) into **PostGIS** or **DuckDB** databases.

### `processing`

Scripts used to retrieve and process the GeoParquet files. Note that the content of this section was partially generated using LLM tool.

---

#### `01_Import_Original_Layers.py`

**Purpose:**
Imports the original EuroCrops source datasets into a **PostGIS** database.

**Description:**
This script automates the ingestion of the original spatial datasets for each NUTS region and year.
It reads import instructions from:

* `data/processing/import_list_original_datasets.csv` (list of datasets and metadata)
* `data/processing/columns_listing.csv` (column selection and filtering)

**Main operations:**

* Copies source files (e.g., shapefiles or other GIS formats) from the configured input directories.
* Checks whether the corresponding database table already exists.
* Imports the data into PostGIS using `ogr2ogr`, projecting geometries to **EPSG:3035** and retaining only selected columns.
* Validates the import by comparing original and final feature counts.
* Produces a summary report (`Import_report_<timestamp>.csv`) with import success ratios.

**Output:**
Tables named according to the pattern:

> **`<NUTS>_<YEAR>`**
> in the schema defined by `postgis_cfg['schema']`.

---

#### `02_Harmonize_Column_Names.py`

**Purpose:**
Standardizes and harmonizes the column structure of imported PostGIS tables.

**Description:**
After importing the original layers, this script ensures a consistent naming and indexing convention across all tables, based on the mapping provided in `data/processing/columns_listing.csv`.

**Main operations:**

* Iterates through all imported tables (`<NUTS>_<YEAR>`).
* Renames columns according to standardized EuroCrops naming rules.
* Renames geometry and primary key fields to `geom` and `cropfield`.
* Adds missing primary keys or area fields (`area_ha`).
* Computes parcel areas from geometry (`ST_Area(geom)/10000`).
* Cleans up and recreates spatial and attribute indexes to improve query performance.

**Output:**
Updated tables with harmonized column names and consistent schema (`geom`, `cropfield`, `area_ha`, etc.), ready for downstream analysis or export.



---

### `03_check_geom.py`

**Purpose:**
Detects and repairs **invalid geometries** in the imported GeoParquet tables stored in PostGIS.

**Description:**
This script validates and corrects geometry issues across the harmonized GSA tables (`<NUTS>_<YEAR>`).
It identifies invalid geometries, stores them in a dedicated schema (`invalidgeom`), and applies multiple repair strategies using **PostGIS geometry functions**.

**Main operations:**

1. **Identify invalid geometries**

   * Scans all tables in the schema `gsa` matching the naming pattern `<NUTS>_<YEAR>`.
   * Counts and extracts invalid geometries (`ST_IsValid(geom) = FALSE`).
   * Saves counts and table names in `invalid_geom.csv`.
   * Creates temporary tables (`invalidgeom.<table>`) with invalid geometries and indexing for faster lookup.

2. **Repair invalid geometries**

   * Applies successive geometry correction methods:

     * `ST_MakeValid()`: primary fix to reconstruct valid geometries.
     * `ST_Buffer(geom, 0)`: fallback method for geometries that remain invalid.
     * `ST_Buffer(ST_Buffer(geom, -1), 1)`: final attempt to correct persistent issues.
   * Updates each table in-place (`gsa.<table>`).
   * Logs the applied correction method per feature.

3. **Output**

   * Generates validation reports after each stage:

     * `invalid_geom.csv`
     * `invalid_geom_ST_MakeValid.csv`
     * `invalid_geom_buffer0.csv`
     * `invalid_geom_buffer1.csv`
   * Saves reports in a timestamped output directory:

     ```
     /eos/jeodpp/data/projects/REFOCUS/clamart/data/cheap/InvalidGeom/<timestamp>/
     ```

**Output:**
Updated and validated tables in the PostGIS schema `gsa`, with problematic features logged and corrected geometries stored in `invalidgeom`.


---

### `04_Generate_Rotation.py`

**Purpose:**
Generates **crop rotation (“stack”) layers** by spatially intersecting yearly parcel geometries for a given NUTS region.

**Description:**
This script creates the **stacked (multi-year)** EuroCrops dataset that captures the crop code (`cYYYY`) and parcel ID (`cfYYYY`) across years for each spatial unit.
It is executed **per NUTS region** and builds the final multi-temporal table used in the EuroCropsV2 dataset distribution.

**Usage:**

```bash
python 04_Generate_Rotation.py <NUTS_CODE>
```

Example:

```bash
python 04_Generate_Rotation.py PT
```

**Main operations:**

1. **Setup and input detection**

   * Reads all available yearly layers for the specified `<NUTS_CODE>` from the schema `gsa`.
   * Determines the geographic bounding box and divides it into 30×30 km processing tiles with a 100 m overlap.

2. **Per-tile processing**

   * Rasterizes each year’s parcel geometries at 2 m resolution.
   * Aggregates yearly rasters into a combined grid using unique cropfield identifiers.
   * Converts aggregated rasters back to vector format (via `gdal_polygonize`).
   * Generates centroids or points inside polygons to enable year-by-year attribute joins.
   * Joins attributes from all yearly layers (`cropfield` and `original_code`).
   * Filters out small polygons (< 0.1 ha).
   * Creates spatial and attribute indexes for optimized performance.

3. **Merge and final assembly**

   * Merges all processed tiles into a single table.
   * Applies a spatial union (`ST_Union`) to dissolve adjacent polygons with identical multi-year attributes.
   * Adds computed area (`area_ha`) and a unique primary key (`cropfield`).
   * Creates a complete **stack layer** per NUTS region:

     > **`gsa.<NUTS>_stack`**

4. **Quality control**

   * Optionally visualizes the resulting grid using `matplotlib` for manual completeness checks.
   * Removes temporary schemas (`gsa_<NUTS>_stack` and `_empty`) once the process is confirmed.

**Output:**

* A single PostGIS table:

  > **`gsa.<NUTS>_stack`**
* Attributes include:

  * `geom` — geometry in EPSG:3035
  * `cf<year>` — parcel ID for each year
  * `c<year>` — original crop code for each year
  * `area_ha` — computed area (in hectares)
* Ready for export as GeoParquet (`stack layers`) in the final EuroCropsV2 dataset.

---


## Data

### `cropcodemapping`

This folder includes mapping tables in CSV format:

| File                         | Description                                               | Columns                                                                                                             |
| ---------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `hcat4.csv`                  | Crop hierarchy and seasonality                            | `hcat4_code`, `hcat4_name`, `seasonality_code`, `seasonality_name`                                                  |
| `Eurocrops.csv`              | Original crop codes and their harmonized mapping          | `nuts`, `original_code`, `original_name`, `translated_name`, `hcat4_code`, `hcat4_name`, `usage_code`, `usage_name` |
| `hcat4_agriprod_mapping.csv` | Link between HCAT4 codes and agricultural products        | `hcat4_code`, `hcat4_name`, `usage_code`, `usage_name`, `link`, `agriprod_code`, `agriprod_name`                    |
| `hcat4_hrl_mapping.csv`      | Link between HCAT4 and HRL (High Resolution Layers) codes | `hcat4_code`, `hcat4_name`, `hrl_code`, `hrl_name`                                                                  |

### `processing`

Files supporting the [processing scripts](https://github.com/Martincccc/EuroCropsV2/tree/main/code/processing).

---

## Geo-data Files

The main geo-data files are hosted externally at:
🔗 [https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/DRLL/EuroCropsV2/](https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/DRLL/EuroCropsV2/)

---

## GSA GeoParquet Data

The EuroCropsV2 dataset is provided as **GeoParquet** files — one per NUTS region and year.
A total of **153 GeoParquet files** are available. 

Each GeoParquet file contains a single table named: **`$<NUTS>_<YEAR>$`**

Each file contains a table with the following attributes:

| Attribute       | Description                                                                                               |
| --------------- | --------------------------------------------------------------------------------------------------------- |
| `geom`          | Geometry in EPSG:3035                                                                                     |
| `cropfield`     | Unique identifier for the parcel within the table                                                         |
| `original_code` | Crop code from the original dataset (or the corrected final code in case of missing/inconsistent entries) |
| `off_area`      | Original reported parcel area, when available                                                             |
| `area_ha`       | Computed parcel area (in hectares)                                                                        |

### Stack Layers

The dataset also includes **stack layers**, representing the intersection of yearly GSAs.

Each GeoParquet file contains a single table named: **`$<NUTS>_stack$`**

A total of **18 GeoParquet files** (one per NUTS region) are provided. Each table includes:

| Attribute   | Description                                        |
| ----------- | -------------------------------------------------- |
| `geom`      | Geometry in EPSG:3035                              |
| `cropfield` | Unique identifier for the parcel                   |
| `cf<year>`  | Corresponding cropfield in the yearly GSA          |
| `c<year>`   | Corresponding original crop code in the yearly GSA |
| `area_ha`   | Computed parcel area (in hectares)                 |

