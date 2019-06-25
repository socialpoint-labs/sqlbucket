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

    def configure(self) -> dict:
        # Setting up the jinja environment
        queries_path = (Path(self.project_path) / 'queries').resolve()
        search_path = [str(queries_path)]
        if self.macros_path:
            search_path.append(str(self.macros_path))
        jinja_env = Environment(loader=FileSystemLoader(
            searchpath=search_path
        ))

        # Now rendering the queries.
        query_order = self.project_config["order"]
        queries = dict()
        for query in query_order:
            template = jinja_env.get_template(query)
            queries[query] = template.render(**self.context)

        return {
            "order": query_order,
            "queries": queries,
            "context": self.context
        }

    def get_project_config(self) -> dict:
        config_path = (self.project_path / 'config.yaml').resolve()
        return yaml.load(open(config_path, 'r').read(), Loader=yaml.FullLoader)

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
            "context": self.context
        }


def configure_variables(project_config: dict, submitted_variables: dict,
                        submitted_env_variables: dict, connection_name: str,
                        env_name: str) -> dict:

        # The following 2 variables found
        # in config.yaml of the project.
        variables_from_config = project_config.get('variables')
        env_variables_from_config = project_config.get('env_variables')

        # quick parsing to be sure variables
        # submitted overwrite the ones from
        # project config.
        variables = dict()
        if variables_from_config:
            variables = variables_from_config[connection_name]
        if submitted_variables:
            for k, v in submitted_variables.items():
                variables[k] = v

        # Ditto for environment vars.
        # Submitted ones overwrite config ones.
        env_variables = dict()
        if env_variables_from_config:
            env_variables = env_variables_from_config[env_name]
        if submitted_env_variables:
            for k, v in submitted_env_variables.items():
                env_variables[k] = v

        return {"vars": variables, "env": env_variables}

