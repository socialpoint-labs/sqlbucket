from sqlbucket.exceptions import GroupNotFound, OrderNotInRightFormat
from sqlbucket.runners import ProjectRunner
from sqlbucket.integrity import run_integrity
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import yaml


class Project:

    def __init__(
        self,
        project_path: str,
        connection_url: str,
        connection_name: str,
        env_name: str = None,
        variables: dict = None,
        env_variables: dict = None,
        macros_path: str = None
    ):

        self.project_path = Path(project_path)
        self.env_name = env_name
        self.connection_url = connection_url
        self.connection_name = connection_name
        self.project_config = self.get_project_config()

        # To generate the variables to submit to Jinja
        self.context = configure_variables(
            project_config=self.project_config,
            submitted_variables=variables,
            submitted_env_variables=env_variables,
            connection_name=connection_name,
            env_name=env_name
        )
        self.macros_path = macros_path

    def configure(self, group: str = None) -> dict:
        # Setting up the jinja environment
        queries_path = (Path(self.project_path) / 'queries').resolve()
        search_path = [str(queries_path)]
        if self.macros_path:
            search_path.append(str(self.macros_path))
        jinja_env = Environment(loader=FileSystemLoader(
            searchpath=search_path
        ))

        # Defining the queries order. The 'order' attribute
        # in config can be an array if no group, or a dict
        # where each attribute is a group name with an array
        # as a value (the group order).
        query_order = self.project_config["order"]
        if group is not None:
            if type(query_order) != dict:
                raise OrderNotInRightFormat(
                    f'Current config not in the right format for group orders'
                )
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
            "project_name": str(self.project_path).split('/')[-1]
        }

    def configure_integrity(self) -> dict:
        # Setting up the jinja environment
        integrity_path = (Path(self.project_path) / 'integrity').resolve()
        search_path = [str(integrity_path)]
        if self.macros_path:
            search_path.append(str(self.macros_path))
        jinja_env = Environment(loader=FileSystemLoader(
            searchpath=search_path
        ))

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
            "project_name": str(self.project_path).split('/')[-1]
        }

    def run(self, group: str, from_step: int = 1, to_step: int = None) -> None:
        configuration = self.configure(group)
        runner = ProjectRunner(
            configuration=configuration,
            from_step=from_step,
            to_step=to_step
        )
        runner.run_project()

    def run_integrity(self, prefix: str = ''):
        integrity_configuration = self.configure_integrity()
        return run_integrity(
            configuration=integrity_configuration,
            prefix=prefix
        )

    def get_project_config(self) -> dict:
        config_path = (self.project_path / 'config.yaml').resolve()
        return yaml.load(open(config_path, 'r').read(), Loader=yaml.FullLoader)


def configure_variables(project_config: dict, submitted_variables: dict,
                        submitted_env_variables: dict, connection_name: str,
                        env_name: str) -> dict:

        # The following 2 variables found
        # in config.yaml of the project.
        variables_from_config = project_config.get('variables')
        env_variables_from_config = project_config.get('env_variables')

        # submitted variables overwrite the ones from project config.
        variables = dict()
        if variables_from_config:
            variables = variables_from_config[connection_name]
        if submitted_variables:
            for k, v in submitted_variables.items():
                variables[k] = v

        # Ditto for environment vars.
        env_variables = dict()
        if env_variables_from_config:
            env_variables = env_variables_from_config[env_name]
        if submitted_env_variables:
            for k, v in submitted_env_variables.items():
                env_variables[k] = v

        return {"vars": variables, "env": env_variables}

