import logging
import sys
from datetime import datetime, timedelta


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] : %(message)s'
)
logger = logging.getLogger()


def n_days_ago(n):
    return (datetime.today() - timedelta(days=n)).strftime('%Y-%m-%d')


def cli_variables_parser(cli_variables: list = None) -> dict:
    variables = dict()

    if not cli_variables:
        return variables

    for var in cli_variables:
        key, value = var.split("=")
        variables[key] = value
    return variables
