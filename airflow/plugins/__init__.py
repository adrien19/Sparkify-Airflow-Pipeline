from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class AdrienPlugin(AirflowPlugin):
    name = "adrien_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTableRedshiftOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.CreateTableSqlQueries
    ]
