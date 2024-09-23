# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import logging

# --------------- #
# TaskGroup class #
# --------------- #

class VelibDataIngestion(TaskGroup):
    """A task group to handle the entire Velib data ingestion process."""

    def __init__(
        self,
        task_id,
        task_group=None,
        api_url=None,
        table_name=None,
        check_query=None,
        insert_query=None,
        table_type=None,
        **kwargs
    ):
        """Instantiate a VelibDataIngestion task group."""
        super().__init__(
            group_id=task_id,
            parent_group=task_group,
            ui_color="#00A7FB",
            **kwargs
        )

        self.api_url = api_url
        self.table_name = table_name
        self.check_query = check_query
        self.insert_query = insert_query
        self.table_type = table_type

        # --------------------- #
        # Fetch Data from API   #
        # --------------------- #

        @task(
            task_group=self
        )
        def fetch_data_from_api():
            logger = logging.getLogger("airflow.task")
            logger.info("Fetching data from API...")
            logger.info("API URL: %s", self.api_url)
            try:
                response = requests.get(self.api_url, headers={"accept": "application/json"})
                response.raise_for_status()
                data = response.json()
                if not data.get("records"):
                    raise ValueError("No data fetched from API.")
                logger.info("Data fetched successfully.")
                # Log the first record if it exists
                first_record = data["records"][0] if data["records"] else None
                logger.info("First record: %s", first_record)
                return data
            except requests.exceptions.RequestException as e:
                logger.error("Failed to fetch data from API: %s", e)
                raise

        # --------------------- #
        # Process Data          #
        # --------------------- #

        @task(
            task_group=self
        )
        def process_data(raw_data):
            logger = logging.getLogger("airflow.task")
            records = raw_data.get("records", [])
            processed_data = []
            for record in records:
                fields = record.get("fields", {})
                processed_data.append({
                    "record_timestamp": record.get("record_timestamp", ""),
                    "stationcode": fields.get("stationcode", ""),
                    "ebike": fields.get("ebike", 0),
                    "mechanical": fields.get("mechanical", 0),
                    "duedate": fields.get("duedate", ""),
                    "numbikesavailable": fields.get("numbikesavailable", 0),
                    "numdocksavailable": fields.get("numdocksavailable", 0),
                    "capacity": fields.get("capacity", 0),
                    "is_renting": fields.get("is_renting", ""),
                    "is_installed": fields.get("is_installed", ""),
                    "is_returning": fields.get("is_returning", ""),
                    "name": fields.get("name", ""),
                    "latitude": fields.get("coordonnees_geo", [])[0] if fields.get("coordonnees_geo") else None,
                    "longitude": fields.get("coordonnees_geo", [])[1] if fields.get("coordonnees_geo") else None,
                    "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", "")
                })
            logger.info("Data processed successfully. Processed %d records.", len(processed_data))
            logger.info("First processed record: %s", processed_data[0] if processed_data else None)
            return processed_data

        # --------------------- #
        # Check Data Integrity  #
        # --------------------- #

        @task(
            task_group=self
        )
        def check_data_integrity(processed_data):
            logger = logging.getLogger("airflow.task")
            pg_hook = PostgresHook(postgres_conn_id='user_postgres')
            if self.table_type == "locations":
                # Check for distinct stationcodes in the processed data
                distinct_stationcodes = {record["stationcode"] for record in processed_data}
                if len(distinct_stationcodes) >= 1460:
                    logger.info("Distinct stationcode check passed. Total distinct stationcodes: %d", len(distinct_stationcodes))
                else:
                    raise ValueError(f"Distinct stationcode check failed. Expected at least 1460 distinct stationcodes, but found {len(distinct_stationcodes)}.")
            elif self.table_type == "stations":
                latest_timestamp = processed_data[0]["record_timestamp"]
                result = pg_hook.get_first(self.check_query, parameters=(latest_timestamp,))
                if result:
                    logger.error("Timestamp %s already exists in the database. Failing the task.", latest_timestamp)
                    raise ValueError(f"Timestamp {latest_timestamp} already exists in the database.")
                else:
                    logger.info("Timestamp %s does not exist in the database. Proceeding with data insertion.", latest_timestamp)

        # --------------------- #
        # Insert Data to Postgres #
        # --------------------- #

        @task(
            task_group=self
        )
        def insert_data_to_postgres(processed_data):
            logger = logging.getLogger("airflow.task")
            pg_hook = PostgresHook(postgres_conn_id='user_postgres')
            for record in processed_data:
                logger.info("Inserting record: %s", record)
                pg_hook.run(self.insert_query, parameters=record)
            logger.info("Data inserted successfully.")

        # set dependencies within task group
        raw_data = fetch_data_from_api()
        processed_data = process_data(raw_data)
        check_data_integrity(processed_data) >> insert_data_to_postgres(processed_data)