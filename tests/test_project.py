from sqlflow.project import Project
from pathlib import Path


class TestProjectInstance:

    project = Project(
        project_path='fixtures/projects/project1',
        connection_url='something://database',
        connection_name='db',
        env_name='dev'
    )
    configuration = project.configure()

    def test_instance_attributes(self):
        assert self.project.project_path == Path('fixtures/projects/project1')
        assert self.project.connection_url == 'something://database'
        assert self.project.connection_name == 'db'

    def test_project_length(self):
        assert len(self.configuration["queries"]) == 3

    def test_query_rendering(self):
        assert self.configuration["queries"]["query_one.sql"] == "bar"
        assert self.configuration["queries"]["query_two.sql"] == "foobar"
        assert self.configuration["queries"]["folder1/query_three.sql"] == "bar"

    def test_query_order(self):
        assert self.configuration["order"] == [
            "query_one.sql",
            "query_two.sql",
            "folder1/query_three.sql"
        ]

    def test_configuration_context(self):
        context = self.configuration["context"]
        assert context["vars"]["foo"] == "bar"
        assert context["env"]["foo"] == "foobar"


class TestOverwritingVariables:

    project = Project(
        project_path='fixtures/projects/project1',
        connection_url='something://database',
        connection_name='db',
        env_name='dev',
        variables={'foo': 'barbar'},
        env_variables={'foo': 'foofoobar'}
    )
    configuration = project.configure()

    def test_query_rendering(self):
        assert self.configuration["queries"]["query_one.sql"] == "barbar"
        assert self.configuration["queries"]["query_two.sql"] == "foofoobar"

    def test_query_order(self):
        assert self.configuration["order"] == [
            "query_one.sql",
            "query_two.sql",
            "folder1/query_three.sql"
        ]

    def test_configuration_context(self):
        context = self.configuration["context"]
        assert context["vars"]["foo"] == "barbar"
        assert context["env"]["foo"] == "foofoobar"






