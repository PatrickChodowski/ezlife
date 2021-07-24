import logging
from google.cloud.exceptions import NotFound
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


# gbq.drop_table('live_ballr.training_runs')
# gbq.send_query_from_file(file=_PATH+'/queries/create_training_runs.sql')

class GBQ:
    def __init__(self,
                 project_id: str,
                 sa_credentials: str,
                 logger: logging.Logger):
        """
        Managing GBQ from python
        :param project_id: gbq project id
        :param sa_credentials: path to service account credentials
        :param logger: project logger
        """

        self.project_id = project_id
        self.sa_credentials = sa_credentials
        self.logger = logger

        self.client = self._init_bq()

    def _init_bq(self) -> bigquery.Client:
        sa_credentials = service_account.Credentials.from_service_account_file(self.sa_credentials)
        client = bigquery.Client(credentials=sa_credentials, project=self.project_id)
        return client

    def drop_table(self, table_path: str) -> None:
        self.client.delete_table(table_path, not_found_ok=True)
        self.logger.info(f"Dropped table {table_path}")

    def truncate_table(self, table_path: str) -> None:
        """
        Deletes all rows from table id
        :param table_path: project, dataset, table
        """
        query = f'DELETE FROM {table_path} WHERE 1=1'
        self.send_query(query)
        self.logger.info(f"Truncated table {table_path}")

    def send_query(self, query: str) -> bigquery.table.RowIterator:
        """
        Sends query from variable
        :param query: query text
        :return: Bigquery query result
        """
        r = self.client.query(query).result()
        self.logger.info(f"Run query {query}")
        return r

    def send_query_from_file(self, file: str) -> bigquery.table.RowIterator:
        """
        Reads query from text file and executes query in gbq
        :param file: file path
        :return: BigQuery query result
        """
        with open(file, 'r') as file:
            query = file.read().replace('\n', '')
        self.logger.info(f"Read query from {file}: {query}")
        r = self.client.query(query).result()
        self.logger.info(f"Run query finished")
        return r

    def get_data(self, query: str) -> pd.DataFrame:
        """
        Get data from gbq
        :param query: Query string to get data
        :return: Pandad dataframe
        """
        rows = self.send_query(query)
        row_list = list()
        for r in rows:
            row_list.append(dict(zip(r.keys(), r.values())))
        df = pd.DataFrame(row_list)
        return df

    def _get_table_schema(self, dataset_id: str, table_id: str) -> pd.DataFrame:
        """
        gets structure of gbq table
        :param: dataset - dataset name
        :param: table - table name
        :return: schema gbq table and columns as pandas dataframe
        """

        self.logger.info(f"Getting table schema for {self.project_id}.{dataset_id}.{table_id}")
        query = f"""SELECT 
                column_name, 
                data_type
         FROM  `{self.project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` 
         WHERE table_name = '{table_id}'"""
        meta_df = self.get_data(query)

        if meta_df.shape[0] > 0:
            self.logger.info(f"{self.project_id}.{dataset_id}.{table_id} schema has {meta_df.shape[0]} rows")

        return meta_df

    def _append_rows(self, df: pd.DataFrame, dataset_id: str, table_id: str) -> None:
        """
        Insert data to existing table
        :param df: Pandas dataframe to be inserted
        :param dataset_id: dataset id
        :param table_id: table id
        """
        table_schema = self._get_table_schema(dataset_id, table_id)
        table_path = f"{self.project_id}.{dataset_id}.{table_id}"
        schema_list = list()

        # basically if table exists on gbq
        if table_schema.shape[0] > 0:
            # writing to table
            for index, row in table_schema.iterrows():
                schema_field = bigquery.SchemaField(row['column_name'], row['data_type'])
                schema_list.append(schema_field)

            job_config = bigquery.LoadJobConfig(schema=schema_list,
                                                write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND)
            job = self.client.load_table_from_dataframe(
                df, table_path, job_config=job_config
            )
            r = job.result()

    def _add_rows_new_table(self, df: pd.DataFrame, dataset_id: str, table_id: str) -> None:
        """
        Creates new table from pandas dataframe
        :param df: Pandas dataframe to be inserted
        :param dataset_id: dataset id
        :param table_id: table id
        """
        table_path = f"{self.project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(autodetect=True)
        job = self.client.load_table_from_dataframe(
            df, table_path, job_config=job_config
        )
        r = job.result()


    def check_if_table_exists(self, dataset_id: str, table_id: str) -> bool:
        """
        Method checking if given table exists
        :param dataset_id: name of dataset
        :param table_id: name of table
        :return: True/False
        """
        try:
            table_path = f'{self.project_id}.{dataset_id}.{table_id}'
            self.client.get_table(table_path)
            table_exists = True
        except NotFound:
            table_exists = False
        return table_exists



    def write_data(self, df: pd.DataFrame, dataset_id: str, table_id: str, if_exists: str = 'append') -> None:
        """
        Safely writes data
        :param df: pandas dataframe to insert
        :param dataset_id: Name of dataset
        :param table_id: Name of table
        :param if_exists: 'append' or 'replace'
        """
        table_exists = self.check_if_table_exists(dataset_id, table_id)
        table_path = f"{self.project_id}.{dataset_id}.{table_id}"
        if table_exists:
            # table exists, check the if_exists argument:
            if if_exists == 'append':
                self.logger.info(f"Table {table_path} exists, appending")
                self._append_rows(df, dataset_id, table_id)
            elif if_exists == 'replace':
                self.logger.info(f"Table {table_path} exists, replacing")
                self.drop_table(table_path)
                self._add_rows_new_table(df, dataset_id, table_id)
        else:
            self.logger.info(f"Table {table_path} doesnt exist, creating")
            self._add_rows_new_table(df, dataset_id, table_id)
