import click
from sqlbucket.utils import logger, n_days_ago, cli_variables_parser, success
import sys
import yaml


def load_cli(sqlbucket_object):
    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.obj = sqlbucket_object

    @cli.command()
    @click.option('--name', '-n')
    @click.pass_obj
    def create_project(sqlbucket, name):
        sqlbucket.create_project(name)
        logger.info(f'Project "{name}" successfully created!')

    @cli.command(context_settings=dict(ignore_unknown_options=True))
    @click.option('--name', '-n', required=True, type=str)
    @click.option('--db', '-b', required=False, type=str)
    @click.option('--group', '-g', required=False, type=str)
    @click.option('--fstep', '-fs', required=False, default=1)
    @click.option('--tstep', '-ts', required=False, default=None, type=int)
    @click.option('--from_date', '-f', required=False, default=n_days_ago(4),
                  type=str)
    @click.option('--to_date', '-t', required=False, default=n_days_ago(0),
                  type=str)
    @click.option('--from_days', '-fd', required=False, default=None, type=str)
    @click.option('--to_days', '-td', required=False, default=None, type=str)
    @click.option('--group', '-g', required=False, type=str)
    @click.option('--isolation', '-i', required=False, default=None, type=str)
    @click.option('--verbose', '-v', is_flag=True, help="Print queries")
    @click.option('--rendering', '-r', is_flag=True, help="Only render queries")
    @click.option('--all', '-all', is_flag=True, help="All dbs")
    @click.option('--edb', '-x', required=False, type=str, help="Excluded dbs")
    @click.pass_obj
    @click.argument('args', nargs=-1)
    def run_job(sqlbucket, name, db, fstep, tstep, to_date, from_date,
                from_days, to_days, group, isolation, verbose, rendering,
                all, edb, args):

        submitted_variables = cli_variables_parser(args)

        if from_days is not None:
            from_date = n_days_ago(int(from_days))

        if to_days is not None:
            to_date = n_days_ago(int(to_days))

        submitted_variables["to"] = to_date
        submitted_variables["from"] = from_date

        logger.info('Variables used')
        logger.info(submitted_variables)

        if isolation:
            isolation = isolation.upper()

        # included dbs
        if db:
            dbs = db.split(',')
        elif all:
            dbs = list(sqlbucket.connections.keys())
        else:
            print(f"Either parameter db (-b) or (--all) flag is required")

        # excluded dbs
        if edb:
            ex_dbs = edb.split(',')
            dbs = [item for item in dbs if item not in ex_dbs]

        for dbi in dbs:
            etl = sqlbucket.load_project(
                project_name=name,
                connection_name=dbi,
                variables=submitted_variables
            )
            if rendering:
                etl.render(from_step=fstep, to_step=tstep, group=group)
            else:
                etl.run(
                    from_step=fstep,
                    to_step=tstep,
                    group=group,
                    verbose=verbose,
                    isolation_level=isolation
                )

    @cli.command(context_settings=dict(ignore_unknown_options=True))
    @click.option('--name', '-n', required=True, type=str)
    @click.option('--db', '-b', required=True, type=str)
    @click.option('--prefix', '-p', required=False, default='', type=str)
    @click.option('--verbose', '-v', is_flag=True, help="Print queries")
    @click.pass_obj
    @click.argument('args', nargs=-1)
    def run_integrity(sqlbucket, name, db, prefix, verbose, args):

        submitted_variables = cli_variables_parser(args)

        logger.info('Variables used')
        logger.info(submitted_variables)

        etl = sqlbucket.load_project(
            project_name=name,
            connection_name=db,
            variables=submitted_variables
        )
        errors = etl.run_integrity(prefix=prefix, verbose=verbose)

        if errors:
            sys.exit(3)

    return cli
