import click


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
    def create_project(sqlflow, project_name):
        pass

    return cli
