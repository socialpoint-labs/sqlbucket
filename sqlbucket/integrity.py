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
    number_of_tests_ran = 0
    connection = create_connection(configuration)
    for query_name in configuration["order"]:

        if not query_name.startswith(prefix):
            continue
        number_of_tests_ran += 1
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
                logger.info(query + '\n')

        except SQLAlchemyError as e:
            errors += 1
            logger.info(f'Query {query_name} encountered an error:')
            logger.error(e)
            continue

    # logging summary
    if not errors:
        logger.info(f'ALL PASSED - '
                    f'({str(number_of_tests_ran)}/{str(number_of_tests_ran)})')
        logger.info(success)

    else:
        logger.error(f'\n\n########## {str(errors)} ERROR(S) ##########'
                     f'\nINTEGRITY FAILURE - '
                     f'({str(number_of_tests_ran - errors)}/'
                     f'{str(number_of_tests_ran)})\n\n')
    return errors


class IntegrityCheck:

    def __init__(self, rows: list, query_name: str):
        self.rows = rows
        self.query_name = query_name

    def has_passed(self) -> bool:
        if not self.rows:
            return True
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
        if not self.rows:
            logger.warning(f'No rows returned from {self.query_name} integrity'
                           f' check. We make it pass, but makes sure this is'
                           f' expected behavior.')
        return status

    def log_rows(self) -> print:
        if not self.rows:
            print('No rows to display')
            return
        keys = self.rows[0].keys()
        tabulator = [list(keys)]
        for item in self.rows:
            array_item = list()
            for k in keys:
                array_item.append(item[k])
            tabulator.append(array_item)
        print(tabulate(tabulator, headers="firstrow", tablefmt="pipe") + '\n')

    def is_passed_field_missing(self):
        if not self.rows:
            return False
        first_item = self.rows[0]
        if 'passed' in first_item:
            return False
        return True

