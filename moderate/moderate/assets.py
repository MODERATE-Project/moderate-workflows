from typing import List

import pandas as pd
import requests
from dagster import AssetExecutionContext, MetadataValue, asset, get_dagster_logger


@asset
def top_story_ids(context: AssetExecutionContext) -> List[int]:
    stories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_ids = requests.get(stories_url).json()[:100]
    context.add_output_metadata(metadata={"num_stories": len(top_ids)})
    return top_ids


@asset
def top_stories(
    context: AssetExecutionContext,
    top_story_ids: List[int],
) -> pd.DataFrame:
    logger = get_dagster_logger()

    results = []

    for item_id in top_story_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()

        results.append(item)

        if len(results) % 20 == 0:
            logger.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df
