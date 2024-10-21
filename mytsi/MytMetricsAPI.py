import os
import json
import requests
import pandas as pd
from tqdm import tqdm
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine


class MyTMetricsAPI:

    def _generate_endpoint_url(self, endpoint: str, **kwargs) -> (str, dict):
        """
        Generate url endpoints and headers
        """
        headers = {
            "Authorization": f"ApiKey {self.personal_access_token}"
        }

        match endpoint:
            case 'catalog':
                endpoint = f"{self.base_url}/api/v1/projects/{self.project_uuid}/dataCatalog"
            case 'catalog_metadata':
                if kwargs.get('table_name'):
                    endpoint = f"{self.base_url}/api/v1/projects/{self.project_uuid}/dataCatalog/{kwargs['table_name']}/metadata"
                else:
                    raise Exception('table_name argument should be provided for catalog_metadata endpoint')
            case 'query':
                if kwargs.get('table_name'):
                    endpoint = f"{self.base_url}/api/v1/projects/{self.project_uuid}/explores/{kwargs['table_name']}/compileQuery"
                    headers['Content-Type'] = 'application/json'
                else:
                    raise Exception('table_name argument should be provided for query endpoint')

        return endpoint, headers

    def _get_catalog(self) -> None:
        """
        Private method for pulling all catalogs
        """
        endpoint, headers = self._generate_endpoint_url(endpoint='catalog')
        self.tables = pd.DataFrame(requests.get(url=endpoint, headers=headers).json()['results']).sort_values('name',
                                                                                                              ascending=True).name.values

    def _get_metadata(self) -> None:
        """
        Private method for pulling all metadata of catalogs
        """

        final_results = {
            "catalog_name": [],
            "catalog_label": [],
            "catalog_field": [],
            "joined_catalogs": []
        }

        for i in tqdm(range(self.num_catalog_to_load)):
            table_name = self.tables[i]
            endpoint, headers = self._generate_endpoint_url(endpoint='catalog_metadata', table_name=table_name)

            response = requests.get(url=endpoint, headers=headers).json()

            if response['status'] == 'ok':
                response_results = response["results"]
                results = {
                    "catalog_name": [response_results["name"] for _ in range(len(response_results["fields"]))],
                    "catalog_label": [response_results["label"] for _ in range(len(response_results["fields"]))],
                    "catalog_field": response_results["fields"],
                    "joined_catalogs": [response_results["joinedTables"] for _ in
                                        range(len(response_results["fields"]))]
                }

                final_results["catalog_name"].extend(results["catalog_name"])
                final_results["catalog_label"].extend(results["catalog_label"])
                final_results["catalog_field"].extend(results["catalog_field"])
                final_results["joined_catalogs"].extend(results["joined_catalogs"])
            else:
                print(f"error occur when reading {table_name}")

            results_df = pd.DataFrame(final_results)
            expanded_df = results_df['catalog_field'].apply(pd.Series)
            expanded_df.columns = 'field_' + expanded_df.columns

        self.fields = pd.concat([results_df.drop(['catalog_field'], axis=1), expanded_df], axis=1)

    def __init__(self, base_url: str, personal_access_token: str, project_uuid: str,
                 num_catalog_to_load: int = None) -> None:
        self.base_url = base_url
        self.personal_access_token = personal_access_token
        self.project_uuid = project_uuid
        print("Loading Catalog...")
        self._get_catalog()
        self.num_catalog_to_load = num_catalog_to_load if num_catalog_to_load is not None else len(self.tables)
        self._get_metadata()

    def get_all_metrics(self) -> list:
        """
        Get all metrics
        """
        return self.fields[self.fields.field_fieldType == 'metric'][
            ["field_name", "field_basicType", "field_description", "catalog_name"]]

    def get_metric_detail(self, metric_name: str) -> dict:
        """
        Get the details of a specific metric
        """
        return self.fields[self.fields.field_name == metric_name].to_dict(orient='records')

    def get_available_dimensions_for_metric(self, metric_name: str) -> list:
        """
        Get all dimensions available for a specific metric
        """
        metric_of_interest = self.fields[self.fields.field_name == metric_name].to_dict(orient='records')[0]
        metric_of_interest["joined_catalogs"].append(metric_of_interest["catalog_name"])
        return self.fields[(self.fields.field_fieldType == 'dimension') & (
            self.fields.catalog_name.isin(metric_of_interest["joined_catalogs"]))][
            ["field_name", "field_basicType", "field_description", "catalog_name"]]

    def prepare_metrics_for_langchain(self) -> pd.DataFrame:
        metrics = self.get_all_metrics()
        metrics.columns = ['metric_name', 'metric_type', 'metric_description', 'metric_catalog']

        metrics['available_dimensions'] = metrics.metric_name.apply(
            lambda x: self.get_available_dimensions_for_metric(metric_name=x).to_dict(orient='records'))
        metrics['available_dimensions'] = metrics['available_dimensions'].astype(str)
        return metrics

    def get_metrics_query(self, metrics: list, dimensions: list = [], filters: dict = {}, table_calculation: list = [],
                          sorts: list = [], additional_metrics: list = [], limit: int = 1):
        """
        Generate sql query given parameters
        """

        # TODO: Add logics filters and sorting

        metrics_of_interest = self.fields[self.fields.field_name.isin(metrics)]

        if metrics_of_interest.catalog_name.nunique() > 1:
            raise Exception("API doesn't support cross model metrics query")

        catalog_name = metrics_of_interest.catalog_name.unique()[0]

        endpoint, headers = self._generate_endpoint_url(endpoint='query', table_name=catalog_name)

        final_metric_names = list(
            metrics_of_interest.apply(lambda x: f"{x.catalog_name}_{x.field_name}", axis=1).values)

        available_dimensions = self.get_available_dimensions_for_metric(metrics[0])
        dimensions_of_interest = available_dimensions[available_dimensions.field_name.isin(dimensions)]

        if len(dimensions_of_interest) != len(dimensions):
            print("Some dimensions are not valid. Ignoring...")

        final_dimension_names = list(
            dimensions_of_interest.apply(lambda x: f"{x.catalog_name}_{x.field_name}", axis=1).values)

        body = {
            "exploreName": catalog_name,
            "dimensions": final_dimension_names,
            "metrics": final_metric_names,
            "sorts": sorts,
            "filters": filters,
            "limit": limit,
            "tableCalculations": table_calculation,
            "additionalMetrics": additional_metrics
        }

        response = requests.post(url=endpoint, data=json.dumps(body), headers=headers).json()

        if response["status"] == 'ok':
            query = response["results"].lower()
            if limit == 1:
                query = query.split('limit')[0]
            return MetricsQuery(query=query)
        else:
            raise Exception(f"Error occured with the following info: {response['error']}")


class MetricsQuery:
    """
    An object to encapsulate querys
    """

    def __init__(self, query: str) -> None:
        self.query = query

    def __str__(self):
        return f"<Metrics Query> {self.query}"

    def __repr__(self):
        return self.__str__()

    def query_snowflake(self):
        """
        Run query against snowflake
        """
        url = URL(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_EMAIL'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            database=os.environ.get('SNOWFLAKE_DATABASE'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
            role=os.environ.get('SNOWFLAKE_ROLE')
        )
        engine = create_engine(url).execution_options()
        connection = engine.connect()
        df = pd.read_sql_query(sql=self.query, con=connection)
        connection.close()
        return df

    def get_query_string(self):
        return self.query
