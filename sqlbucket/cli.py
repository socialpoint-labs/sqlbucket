import click
from sqlbucket.utils import logger, n_days_ago, cli_variables_parser
from sqlbucket.runners import ProjectRunner


def load_cli(sqlflow_object):

    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.obj = sqlflow_object

    @cli.command()
    @click.option('--name', '-n')
    @click.pass_obj
    def hello(sqlflow, name):
        click.echo(f'Hello {name}')
        click.echo(sqlflow.env_name)

    @cli.command()
    @click.option('--name', '-n')
    @click.pass_obj
    def create_project(sqlflow, name):
        sqlflow.create_project(name)
        logger.info(f'Project "{name}" successfully created!')

    @cli.command(context_settings=dict(ignore_unknown_options=True))
    @click.option('--name', '-n', type=str)
    @click.option('--db', '-b', type=str)
    @click.option('--env', '-e', default=False)
    @click.option('--fstep', '-fs', required=False, default=1)
    @click.option('--tstep', '-ts', required=False, default=None, type=int)
    @click.option('--from_date', '-f', required=False, default=n_days_ago(4),
                  type=str)
    @click.option('--to_date', '-t', required=False, default=n_days_ago(0),
                  type=str)
    @click.option('--from_days', '-fd', required=False, default=None, type=str)
    @click.option('--to_days', '-td', required=False, default=None, type=str)
    @click.argument('args', nargs=-1)
    def run_job(sqlflow, name, db, env, fstep, tstep, to_date, from_date,
                from_days, to_days, args):

        submitted_variables = cli_variables_parser(args)

        if from_days is not None:
            from_date = n_days_ago(int(from_days))

        if to_days is not None:
            to_date = n_days_ago(int(to_days))

        submitted_variables["to"] = to_date
        submitted_variables["from"] = from_date

        logger.info('Variables used')
        logger.info(submitted_variables)

        etl = sqlflow.load_project(
            project=name,
            db=db,
            context=submitted_variables,
            env=env
        )
        configuration = etl.build_etl_configuration()
        runner = ProjectRunner(
            configuration=configuration,
            from_step=fstep,
            to_step=tstep
        )
        runner.run_project()

    return cli
