from dagster import Definitions, load_assets_from_modules

import moderate.assets

all_assets = load_assets_from_modules([moderate.assets])

defs = Definitions(
    assets=all_assets,
)
