from sqlflow.project import Project
from sqlflow.exceptions import GroupNotFound, OrderNotInRightFormat
from pathlib import Path
import pytest


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

    def test_configuration_project_name(self):
        assert self.configuration["project_name"] == "project1"


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
        assert self.configuration["queries"]["folder1/query_three.sql"] == "barbar"

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


class TestProjectOrderGroup:

    project = Project(
        project_path='fixtures/projects/project2',
        connection_url='something://database',
        connection_name='db',
        env_name='dev',
        variables={'foo': 'barbar'},
        env_variables={'foo': 'foofoobar'}
    )

    configuration_main = project.configure(group='main')
    configuration_other = project.configure(group='other')

    def test_query_order_main(self):
        assert self.configuration_main["order"] == ["query_one.sql"]

    def test_query_rendering_main(self):
        assert self.configuration_main["queries"]["query_one.sql"] == "barbar"

    def test_configuration_context_main(self):
        context = self.configuration_main["context"]
        assert context["vars"]["foo"] == "barbar"
        assert context["env"]["foo"] == "foofoobar"

    def test_query_order_other(self):
        assert self.configuration_other["order"] == [
            "query_two.sql",
            "folder1/query_three.sql"
        ]

    def test_query_rendering_other(self):
        assert self.configuration_other["queries"].get("query_one.sql") is None
        assert self.configuration_other["queries"]["query_two.sql"] == "foofoobar"
        assert self.configuration_other["queries"]["folder1/query_three.sql"] == "barbar"

    def test_configuration_context_other(self):
        context = self.configuration_main["context"]
        assert context["vars"]["foo"] == "barbar"
        assert context["env"]["foo"] == "foofoobar"


class TestProjectOrderExceptions:

    def test_group_not_found(self):
        project = Project(
            project_path='fixtures/projects/project2',
            connection_url='something://database',
            connection_name='db',
            env_name='dev',
        )
        with pytest.raises(GroupNotFound):
            project.configure(group='fake')

    def test_wrong_order_format(self):
        project = Project(
            project_path='fixtures/projects/project1',
            connection_url='something://database',
            connection_name='db',
            env_name='dev',
        )
        with pytest.raises(OrderNotInRightFormat):
            project.configure(group='main')
