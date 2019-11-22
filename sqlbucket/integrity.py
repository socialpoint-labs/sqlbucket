from sqlbucket.runners import create_connection
from sqlbucket.runners import logger
from tabulate import tabulate
from sqlbucket.utils import integrity_logo, success
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlbucket.exceptions import PassedFieldNotInQuery


def run_integrity(configuration: dict, prefix: str = '', verbose: bool = False):
    errors = 0
    logger.info(integrity_logo)
    logger.info(
        f'Starting integrity checks for {configuration["project_name"]} '
        f'with connection {configuration["connection_name"]}'
    )
    connection = create_connection(configuration)
    for query_name in configuration["order"]:

        if not query_name.startswith(prefix):
            continue

        query = configuration["queries"][query_name]
        if verbose:
            logger.info(f'Now running:\n\n{query}')

        try:
            integrity = IntegrityCheck(
                rows=[dict(row) for row in connection.execute(text(query))],
                query_name=query_name
            )
            integrity.log_summary(query_name)
            if not integrity.has_passed():
                errors += 1
                logger.info(
                    f"Showing integrity report for {query_name.upper()}: \n")
                integrity.log_rows()

        except SQLAlchemyError as e:
            errors += 1
            logger.info(f'Query {query_name} encountered an error:')
            logger.error(e)
            continue

    # logging summary
    total_tested = len(configuration["order"])
    if not errors:
        logger.info(f'ALL PASSED - ({str(total_tested)}/{str(total_tested)})')
        logger.info(success)

    else:
        logger.error(f'\n\n########## {str(errors)} ERROR(S) ##########'
                     f'\nPARTIALLY PASSED - '
                     f'({str(total_tested - errors)}/{str(total_tested)})\n\n')
    return errors


class IntegrityCheck:

    def __init__(self, rows: list, query_name: str):
        self.rows = rows
        self.query_name = query_name

    def has_passed(self) -> bool:
        for item in self.rows:
            if not item["passed"]:
                return False
        return True

    def log_summary(self, query_name: str) -> str:

        if self.is_passed_field_missing():
            raise PassedFieldNotInQuery('Field "passed" is missing in query.\n'
                                        'Make sure you add it as an alias '
                                        'and that it returns a boolean')

        succeeded = len([item for item in self.rows if item["passed"]])
        status = "success" if self.has_passed() else "FAILED"
        logger.info(
            f'Integrity {status}, ({succeeded}/{len(self.rows)}) '
            f'for {query_name}'
        )
        return status

    def log_rows(self) -> print:
        keys = self.rows[0].keys()
        tabulator = [list(keys)]
        for item in self.rows:
            array_item = list()
            for k in keys:
                array_item.append(item[k])
            tabulator.append(array_item)
        print(tabulate(tabulator, headers="firstrow", tablefmt="pipe") + '\n')

    def is_passed_field_missing(self):
        first_item = self.rows[0]
        if first_item.get('passed') is None:
            return True
        return False

