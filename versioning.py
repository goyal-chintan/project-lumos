import json
import requests
import asyncio
import logging
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetPropertiesClass, ChangeTypeClass
from datahub.emitter.mcp import MetadataChangeProposalWrapper
# --- Configuration ---
DATAHUB_URL = "http://localhost:8080"  # Replace with your DataHub GMS URL
DATAHUB_TOKEN = "your_datahub_access_token"  # Replace with your DataHub personal access token
CURRENT_CLOUD_VERSION = "S-310"  # The current cloud version you are tracking.
VERSION_HISTORY_FILE = "version_history.json"  # File to store the history of updates
LOG_FILE = "debug.log"  # Log file for debugging
# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# --- Initialize GraphQL Client and DataHub Emitter ---
transport = AIOHTTPTransport(
    url=f"{DATAHUB_URL}/api/graphql",
    headers={"Authorization": f"Bearer {DATAHUB_TOKEN}"}
)
client = Client(transport=transport, fetch_schema_from_transport=True)
try:
    emitter = DatahubRestEmitter(DATAHUB_URL, token=DATAHUB_TOKEN)
    logger.info("Successfully connected to DataHub Emitter.")
except Exception as e:
    logger.error(f"Failed to connect to DataHub Emitter: {e}")
    exit(1)
# --- GraphQL Queries ---
datasets_query = gql("""
query scrollAcrossEntities($input: ScrollAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    nextScrollId
    searchResults {
      entity {
        ... on Dataset {
          urn
        }
      }
    }
  }
}
""")
version_query = gql("""
query getSchemaVersionList($input: GetSchemaVersionListInput!) {
  getSchemaVersionList(input: $input) {
    latestVersion {
      semanticVersion
    }
  }
}
""")
# --- Helper Functions ---
async def fetch_datasets():
    """
    Fetches all dataset URNs from DataHub using scroll API.
    """
    datasets = []
    scroll_id = None
    has_more = True
    logger.info("Starting to fetch all datasets from DataHub...")
    while has_more:
        variables = {
            "input": {
                "types": ["DATASET"],
                "count": 500,
                "query": "*",
                "sortInput": {"sortCriteria": [{"field": "urn", "sortOrder": "ASCENDING"}]}
            }
        }
        if scroll_id:
            variables["input"]["scrollId"] = scroll_id
        try:
            result = await client.execute_async(datasets_query, variable_values=variables)
            scroll_result = result["scrollAcrossEntities"]
            current_batch_urns = [r["entity"]["urn"] for r in scroll_result["searchResults"]]
            datasets.extend(current_batch_urns)
            scroll_id = scroll_result["nextScrollId"]
            has_more = scroll_id is not None
            logger.debug(f"Fetched {len(current_batch_urns)} datasets. Total fetched: {len(datasets)}")
        except Exception as e:
            logger.error(f"Error fetching datasets during scroll: {e}")
            break
    logger.info(f"Finished fetching datasets. Total datasets found: {len(datasets)}")
    return datasets
async def fetch_version(urn: str) -> str:
    """
    Fetches the latest semantic version of a dataset's schema.
    """
    try:
        result = await client.execute_async(version_query, variable_values={"input": {"datasetUrn": urn}})
        latest_version = result["getSchemaVersionList"]["latestVersion"]
        semantic_version = latest_version["semanticVersion"] if latest_version else None
        logger.debug(f"Fetched version for {urn}: {semantic_version}")
        return semantic_version
    except Exception as e:
        logger.error(f"Error fetching schema version for {urn}: {e}")
        return ""
def get_existing_cloud_version(urn: str) -> dict:
    """
    Retrieves the 'cloud_version' custom property for a given dataset URN from DataHub.
    Expects 'cloud_version' to be a JSON string.
    """
    try:
        encoded_urn = requests.utils.quote(urn, safe='')
        url = f"{DATAHUB_URL}/entities/{encoded_urn}?aspects=datasetProperties"
        headers = {"Authorization": f"Bearer {DATAHUB_TOKEN}"}
        logger.debug(f"Fetching existing properties for {urn} from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Raw API response for {urn}: {json.dumps(data, indent=2)}")
        cloud_version_str = data['value']['com.linkedin.metadata.snapshot.DatasetSnapshot']['aspects'][1]['com.linkedin.dataset.DatasetProperties']['customProperties']['cloud_version']
        logger.info(f"Retrieved cloud_version string for {urn}: {cloud_version_str}")
        if cloud_version_str:
            try:
                parsed_cloud_version = json.loads(cloud_version_str)
                if isinstance(parsed_cloud_version, dict):
                    logger.info(f"Successfully parsed cloud_version for {urn}: {parsed_cloud_version}")
                    return parsed_cloud_version
                else:
                    logger.warning(f"cloud_version for {urn} is not a valid JSON dictionary: '{cloud_version_str}'. Returning empty dict.")
                    return {}
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse cloud_version JSON for {urn}: '{cloud_version_str}', Error: {e}. Returning empty dict.")
                return {}
        else:
            logger.info(f"No 'cloud_version' custom property found for {urn}.")
            return {}
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 404:
            logger.warning(f"Dataset URN not found in DataHub: {urn}. This dataset might not exist or is new. Returning empty properties.")
        else:
            logger.error(f"HTTP error occurred while fetching properties for {urn}: {http_err}. Status Code: {http_err.response.status_code}. Response: {http_err.response.text}")
        return {}
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred while fetching properties for {urn}: {conn_err}. Is DataHub running and accessible at {DATAHUB_URL}?")
        return {}
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred while fetching properties for {urn}: {timeout_err}")
        return {}
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_existing_cloud_version for {urn}: {e}")
        return {}
# --- Main Logic ---
async def main():
    """
    Main function to orchestrate fetching datasets, their versions,
    updating DataHub, and logging the history.
    """
    logger.info("Starting DataHub Cloud Version Updater script...")
    datasets = await fetch_datasets()
    version_history_records = []
    success_count = 0
    if not datasets:
        logger.info("No datasets found in DataHub. Exiting.")
        return
    for urn in datasets:
        logger.info(f"Processing dataset: {urn}")
        latest_version = await fetch_version(urn)
        logger.info(f"The latest version of {urn} is {latest_version}")
        if not latest_version:
            logger.warning(f"Could not fetch latest schema version for {urn}. Skipping update for this dataset.")
            continue
        existing_cloud_version_map = get_existing_cloud_version(urn)
        logger.info(f"The existing cloud version map for {urn} is {existing_cloud_version_map}")
        if not isinstance(existing_cloud_version_map, dict):
            logger.warning(f"Invalid format for existing cloud_version for {urn}. Re-initializing.")
            existing_cloud_version_map = {}
        updated_cloud_version_map = existing_cloud_version_map.copy()
        updated_cloud_version_map[CURRENT_CLOUD_VERSION] = latest_version
        version_history_records.append({
            "urn": urn,
            "existing_cloud_version": existing_cloud_version_map,
            "updated_cloud_version": updated_cloud_version_map
        })
        custom_properties_to_emit = {"cloud_version": json.dumps(updated_cloud_version_map)}
        logger.info(f"Custom properties to emit for {urn}: {custom_properties_to_emit}")
        dataset_properties_aspect = DatasetPropertiesClass(customProperties=custom_properties_to_emit)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=dataset_properties_aspect,
            aspectName="datasetProperties",
            changeType=ChangeTypeClass.UPSERT
        )
        try:
            emitter.emit(mcp)
            logger.info(f"Successfully updated {urn} with cloud_version: {updated_cloud_version_map}")
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to emit metadata change for {urn}: {e}")
            continue
    try:
        with open(VERSION_HISTORY_FILE, "w") as f:
            json.dump(version_history_records, f, indent=2)
        logger.info(f"Version history successfully saved to {VERSION_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving version history to {VERSION_HISTORY_FILE}: {e}")
    logger.info(f"Script finished. Successfully updated {success_count} out of {len(datasets)} datasets.")
    print(f"\n--- Script Summary ---")
    print(f"Successfully updated {success_count} datasets.")
    print(f"Detailed logs available in {LOG_FILE}")
    print(f"Version history saved to {VERSION_HISTORY_FILE}")
if __name__ == "__main__":
    asyncio.run(main())





















