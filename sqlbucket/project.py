from sqlbucket.exceptions import GroupNotFound, OrderNotInRightFormat
from sqlbucket.runners import ProjectRunner
from sqlbucket.integrity import run_integrity
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Optional
import yaml


class Project:

    def __init__(
        self,
        project_path: str,
        connection_url: str,
        context: dict = None,
        macros_path: str = None
    ):

        self.project_path = Path(project_path)
        self.project_config = self.get_project_config()

        self.context = ContextMerger(
            context=context, context_from_config=self.project_config
        ).merge()

        self.env_name = self.context['e']['name']
        self.connection_url = connection_url
        self.connection_name = self.context['c']['name']

        self.macros_path = macros_path

    def configure(self, group: str = None) -> dict:
        # Setting up the jinja environment
        jinja_env = self.create_jinja_env(folder='queries')

        # The 'order' attribute in config can be an array if no group, or a
        # dict where each key is a group name with an array as a value (the
        # group order). If no group name is submitted, but the order is in
        # group format, it will search for default 'main' group.
        query_order = self.project_config["order"]
        if type(query_order) == list:
            if group:
                raise OrderNotInRightFormat(
                    f'Current config not in the right format for group orders'
                )
        elif type(query_order) == dict:
            if not group:
                group = 'main'
            if group not in query_order:
                raise GroupNotFound(
                    f'Group "{group}" not found in order config.'
                )
            query_order = query_order[group]

        # Now rendering the queries.
        queries = dict()
        for query in query_order:
            template = jinja_env.get_template(query)
            queries[query] = template.render(**self.context)

        return {
            "order": query_order,
            "queries": queries,
            "context": self.context,
            "connection_url": self.connection_url,
            "connection_name": self.connection_name,
            "project_name": str(self.project_path).split('/')[-1],
            "connection_query": self.get_connection_query()
        }

    def configure_integrity(self) -> dict:
        # Setting up the jinja environment
        jinja_env = self.create_jinja_env(folder='integrity')

        order = jinja_env.list_templates('sql')
        queries = dict()
        for query in order:
            template = jinja_env.get_template(query)
            queries[query] = template.render(**self.context)

        return {
            "order": order,
            "queries": queries,
            "context": self.context,
            "connection_url": self.connection_url,
            "connection_name": self.connection_name,
            "project_name": str(self.project_path).split('/')[-1],
            "connection_query": self.get_connection_query()
        }

    def run(self, group: str = None, from_step: int = 1, to_step: int = None,
            verbose: bool = False, isolation_level: str = None) -> None:
        configuration = self.configure(group)
        runner = ProjectRunner(
            configuration=configuration,
            from_step=from_step,
            to_step=to_step,
            verbose=verbose,
            isolation_level=isolation_level
        )
        runner.run_project()

    def render(self, group: str = None, from_step: int = 1,
               to_step: int = None) -> None:
        configuration = self.configure(group)
        runner = ProjectRunner(
            configuration=configuration,
            from_step=from_step,
            to_step=to_step,
        )
        runner.render_queries()

    def run_integrity(self, prefix: str = '', verbose: bool = False):
        integrity_configuration = self.configure_integrity()
        return run_integrity(
            configuration=integrity_configuration,
            prefix=prefix,
            verbose=verbose
        )

    def get_project_config(self) -> dict:
        config_path = (self.project_path / 'config.yaml').resolve()
        return yaml.load(open(config_path, 'r').read(), Loader=yaml.FullLoader)

    def get_connection_query(self) -> Optional[str]:
        """
        We create the jinja env from scratch again from the queries folder.
        This is done in case we run the integrity, which will need the
        connection script too. Not really clean, as we create the same
        environment twice.
        :return: the connection query if any.
        """

        if "connection_query" not in self.project_config:
            return None

        jinja_env = self.create_jinja_env(folder='queries')
        template = jinja_env.get_template(
            self.project_config['connection_query']
        )
        return template.render(**self.context)

    def create_jinja_env(self, folder: str) -> Environment:
        macro_folder_from_lib = Path(__file__).parent / 'macros'
        queries_path = (Path(self.project_path) / folder).resolve()
        search_path = [str(queries_path), str(macro_folder_from_lib)]
        if self.macros_path:
            search_path.append(str(self.macros_path))
        jinja_env = Environment(loader=FileSystemLoader(
            searchpath=search_path
        ))
        return jinja_env


class ContextMerger:
    """
    Class to help merging 2 variables contexts. Original context is the one
    submitted when loading a project. It must be merged with the one found in
    config.

    In case of duplicate keys between the 2 context, we only keep one. The
    context that has priority depends on the variable types.

    Environment and connection variables from the config.yaml of a project will
    overwrite the key/value matching pairs of the one submitted by SQLBucket.

    For project variables, this is opposite, we give priority to the ones submitted
    by the SQLBucket, typically the one submitted in Python or via CLI. The
    logic behind it is that we prefer to give priority to the more dynamic
    approach.

    """
    def __init__(self, context: dict, context_from_config: dict):
        """
        :param context: Context send to project by SQLBucket object.
        :param context_from_config: Context found in config.yaml of a project.
        """
        self.context = context
        self.context_from_config = context_from_config

    def overwrite_environment_variables(self):
        config_env_vars = self.context_from_config.get('environment_variables')
        if not config_env_vars:
            return

        env_name = self.context['e']['name']
        if env_name not in config_env_vars:
            return

        for key, value in config_env_vars[env_name].items():
            self.context['e'][key] = value

    def overwrite_connection_variables(self):
        connections_vars = self.context_from_config.get('connection_variables')
        if not connections_vars:
            return

        connection_name = self.context['c']['name']
        if connection_name not in connections_vars:
            return

        for key, value in connections_vars[connection_name].items():
            self.context['c'][key] = value

    def overwrite_project_variables(self):
        project_vars = self.context_from_config.get('project_variables')
        if not project_vars:
            return
        for key, value in project_vars.items():
            self.context[key] = value

    def merge(self):
        self.overwrite_environment_variables()
        self.overwrite_connection_variables()
        self.overwrite_project_variables()
        return self.context
