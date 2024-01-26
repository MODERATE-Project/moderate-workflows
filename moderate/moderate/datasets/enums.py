import enum


class DatasetNames(enum.Enum):
    BUILDING_STOCK_T31_EPC_CLASSIFICATION = "building_stock_t31_epc_classification"
    BUILDING_STOCK_T31_PV_ANALYSIS = "building_stock_t31_pv_analysis"
    BUILDING_STOCK_T32_BUILDING_STOCK = "building_stock_t32_building_stock"


class DataFormats(enum.Enum):
    PARQUET = "parquet"
