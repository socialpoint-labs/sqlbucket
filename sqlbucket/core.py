from sqlbucket.project import Project
from sqlbucket.exceptions import ProjectNotFound, ConnectionNotFound, \
    ReservedVariableNameError
from sqlbucket.cli import load_cli
from pathlib import Path
from distutils.dir_util import copy_tree
import os
from typing import List, Callable


class SQLBucket:

    def __init__(
        self,
        projects_folder: str = 'projects',
        connections: dict = None,
        connection_variables: dict = None,
        env_name: str = None,       # todo: see if we can remove
        environment_variables: dict = None,
        macro_folder: str = None,
        functions_registry: List[Callable] = None
    ):

        self.projects_path = Path.cwd() / Path(projects_folder)
        self.macro_path = Path(macro_folder) if macro_folder is not None else None
        self.connections = connections
        self.connection_variables = connection_variables or dict()
        self.environment_variables = environment_variables or dict()
        self.env_name = env_name
        self.functions_registry = functions_registry or list()

    def load_project(self, project_name: str, connection_name: str,
                     variables: dict = None) -> Project:
        """
        Load a project for a given project name and a given connection name.
        :param project_name
        :param connection_name
        :param variables: Typically variables submitted via CLI
        :return: Project instance
        """

        if not self.connection_exists(connection_name):
            raise ConnectionNotFound(
                f'Connection "{connection_name}" not found.'
            )

        project_path = (self.projects_path / project_name).resolve()
        if not project_path.exists() or not project_path.is_dir():
            raise ProjectNotFound(
                f'Project "{project_name}" does not exist.'
            )

        context = self.build_context(
            connection_name=connection_name, variables=variables
        )

        return Project(
            project_path=str(project_path),
            connection_url=self.connections[connection_name],
            context=context,
            macros_path=self.macro_path
        )

    def create_project(self, project_name: str):
        if project_name in os.listdir(self.projects_path):
            raise Exception('Project name already used. Find another name')

        sql_template_folder = Path(__file__).parent.joinpath('template')
        new_project_folder = (self.projects_path / project_name).resolve()
        copy_tree(str(sql_template_folder), str(new_project_folder))

    def connection_exists(self, connection_name: str) -> bool:
        if connection_name in self.connections:
            return True
        if os.environ.get(connection_name) is not None:
            return True
        return False

    def cli(self):
        command_line = load_cli(self)
        return command_line()

    def build_context(self, connection_name: str, variables: dict):
        etl_context = dict()

        # first we add connection variables
        etl_context['c'] = self.connection_variables.get(connection_name, dict())
        etl_context['c']['name'] = connection_name

        # then we add global variables
        etl_context['e'] = self.environment_variables or dict()
        etl_context['e']['name'] = self.env_name

        if self.functions_registry:
            etl_context['f'] = dict()
            for func in self.functions_registry:
                etl_context['f'][func.__name__] = func

        if not variables:
            return etl_context

        for key, value in variables.items():
            if key in ('c', 'e', 'f'):
                raise ReservedVariableNameError(f'{key} is a reserved name'
                                                f'Use another key name')
            etl_context[key] = value

        return etl_context

    def register_function(self, func: Callable):
        self.functions_registry.append(func)
