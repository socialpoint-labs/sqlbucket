from sqlbucket.project import Project
from sqlbucket.exceptions import GroupNotFound, OrderNotInRightFormat
from pathlib import Path
import pytest


class TestProjectInstance:

    path = str((Path(__file__).parent / Path('fixtures/projects/project1')))

    project = Project(
        project_path=path,
        connection_url='something://database',
        context={'c': {'name': 'db'}, 'e': {'name': 'dev'}}
    )
    configuration = project.configure()

    def test_instance_attributes(self):
        assert self.project.project_path == \
               Path(__file__).parent / Path('fixtures/projects/project1')
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
        assert context["foo"] == "bar"
        assert context["e"]["foo"] == "foobar"

    def test_configuration_project_name(self):
        assert self.configuration["project_name"] == "project1"


class TestOverwritingVariables:

    path = str((Path(__file__).parent / Path('fixtures/projects/project1')))

    project = Project(
        project_path=path,
        connection_url='something://database',
        context={
            'foo': 'barbar',
            'c': {'name': 'db'},
            'e': {'name': 'dev', 'foo': 'foofoobar'}
        }
    )
    configuration = project.configure()

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
        assert context["foo"] == "bar"
        assert context["e"]["foo"] == "foobar"
        assert context["e"]["name"] == "dev"


class TestProjectOrderGroup:

    path = str((Path(__file__).parent / Path('fixtures/projects/project2')))

    project = Project(
        project_path=path,
        connection_url='something://database',
        context={
            'c': {'name': 'db'},
            'e': {'name': 'dev', 'foo': 'foofoobar'},
            'foo': 'bar'
        }
    )

    configuration_main = project.configure(group='main')
    configuration_other = project.configure(group='other')

    def test_query_order_main(self):
        assert self.configuration_main["order"] == ["query_one.sql"]

    def test_query_rendering_main_overwrite_by_config(self):
        assert self.configuration_main["queries"]["query_one.sql"] == "barbar"

    def test_configuration_context_main(self):
        context = self.configuration_main["context"]
        assert context["foo"] == "barbar"
        assert context["e"]["foo"] == "foofoobar"

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
        assert context["foo"] == "barbar"
        assert context["e"]["foo"] == "foofoobar"


class TestProjectGroupDefault:

    path = str((Path(__file__).parent / Path('fixtures/projects/project2')))

    project = Project(
        project_path=path,
        connection_url='something://database',
        context={
            'c': {'name': 'db'},
            'e': {'name': 'dev', 'foo': 'foofoobar'},
            'foo': 'barbar'
        }
    )

    configuration_main = project.configure()

    def test_query_order_main(self):
        assert self.configuration_main["order"] == ["query_one.sql"]

    def test_query_rendering_main(self):
        assert self.configuration_main["queries"]["query_one.sql"] == "barbar"

    def test_configuration_context_main(self):
        context = self.configuration_main["context"]
        assert context["foo"] == "barbar"
        assert context["e"]["foo"] == "foofoobar"


class TestProjectOrderExceptions:

    def test_group_not_found(self):
        path = str(
            (Path(__file__).parent / Path('fixtures/projects/project2')))
        project = Project(
            project_path=path,
            connection_url='something://database',
            context={
                'c': {'name': 'db'},
                'e': {'name': 'dev'}
            }
        )
        with pytest.raises(GroupNotFound):
            project.configure(group='fake')

    def test_wrong_order_format(self):
        path = str(
            (Path(__file__).parent / Path('fixtures/projects/project1')))
        project = Project(
            project_path=path,
            connection_url='something://database',
            context={
                'c': {'name': 'db'},
                'e': {'name': 'dev'}
            }
        )
        with pytest.raises(OrderNotInRightFormat):
            project.configure(group='main')


class TestConnectScript:

    path = str(
        (Path(__file__).parent / Path('fixtures/projects/project2')))
    project = Project(
        project_path=path,
        connection_url='something://database',
        context={
            'c': {
                'name': 'db',
                'destination_schema': 'output_schema',
                'source_schema': 'input_schema'
            },
            'e': {'name': 'dev'}
        }
    )
    configuration = project.configure()
    integrity_configuration = project.configure_integrity()

    def test_etl_configuration_with_connect_script(self):
        assert 'connection_script' in self.configuration

    def test_integrity_configuration_with_connect_script(self):
        assert 'connection_script' in self.integrity_configuration

    def test_connect_script_query(self):
        expected_query = 'set search_path to output_schema, input_schema;'
        assert self.configuration['connection_script'] == expected_query
        assert self.integrity_configuration['connection_script'] == expected_query

