from sqlbucket.runners import create_connection
from sqlbucket.runners import logger
from tabulate import tabulate


def run_integrity(configuration: dict, prefix: str = ''):
    errors = 0
    connection = create_connection(configuration["connection_url"])
    for query_name in configuration["order"]:

        if not query_name.startswith(prefix):
            continue

        query = configuration["queries"][query_name]
        integrity = IntegrityCheck(
            rows=[dict(row) for row in connection.execute(query)],
            query_name=query_name
        )
        integrity.log_summary()
        if not integrity.has_passed():
            errors += 1
            logger.info("Showing integrity rows: ")
            integrity.log_rows()

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

    def log_summary(self) -> str:
        succeeded = len([item for item in self.rows if item["passed"]])
        status = "success" if self.has_passed() else "FAILED"
        logger.info(f'Status: {status}. {succeeded}/{len(self.rows)} '
                    f'successfully passed.')
        return status

    def log_rows(self) -> print:
        keys = self.rows[0].keys()
        tabulator = [list(keys)]
        for item in self.rows:
            array_item = list()
            for k in keys:
                array_item.append(item[k])
            tabulator.append(array_item)
        return print(tabulate(tabulator, headers="firstrow", tablefmt="pipe"))


