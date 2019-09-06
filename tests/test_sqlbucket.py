from sqlbucket import SQLBucket, Project
from sqlbucket.exceptions import ProjectNotFound, ConnectionNotFound
from pathlib import Path
import pytest
import yaml
import os


class TestSQLBucket:

    sqlbucket = SQLBucket(projects_folder='projects', macro_folder='macros')

    def test_project_folder_exist(self):
        assert self.sqlbucket.projects_path is not None
        assert isinstance(self.sqlbucket.projects_path, Path)

    def test_macro_folder_exist(self):
        assert self.sqlbucket.macro_path is not None
        assert isinstance(self.sqlbucket.macro_path, Path)


class TestProjectLoading:

    path = str((Path(__file__).parent / Path('fixtures/projects')))
    sqlbucket = SQLBucket(
        projects_folder=path,
        connections={'db_name': 'db_url'},
        env_name='dev'
    )

    def test_load_wrong_project_name(self):
        with pytest.raises(ProjectNotFound):
            self.sqlbucket.load_project(
                project_name='fixture_project',
                connection_name='db_name'
            )

    def test_connection_not_found(self):
        with pytest.raises(ConnectionNotFound):
            self.sqlbucket.load_project(
                project_name='project1',
                connection_name='wrong_db_name'
            )

    def test_connection_in_shell_environment(self):
        os.environ['db_in_env'] = 'db_url'
        assert self.sqlbucket.connection_exists('db_in_env')

    def test_successful_project_loading(self):
        project = self.sqlbucket.load_project(
            project_name='project1',
            connection_name='db_name'
        )
        assert isinstance(project, Project)


class TestConnections:
    connections_path = Path(__file__).parent / Path('fixtures') / Path('connections.yaml')
    # connections_path = (path / 'connections.yaml').resolve()
    connections = yaml.load(open(connections_path), Loader=yaml.FullLoader)
    sqlbucket = SQLBucket(
        projects_folder='projects',
        macro_folder='macros',
        connections=connections
    )

    def test_connections_attr(self):
        assert self.sqlbucket.connections is not None

    def test_get_connection(self):
        assert self.sqlbucket.connections['database_1'] == 'psycopg2://database'

