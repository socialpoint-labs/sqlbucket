from pathlib import Path
from sqlbucket import Project, SQLBucket


def nrange(start, end):
    return [x for x in range(start, end)]


def nrange2(start, end):
    return [x for x in range(start, end)]


class TestFunctionRendering:

    path = str(
        (Path(__file__).parent / Path('fixtures/projects/project3')))
    project = Project(
        project_path=path,
        connection_url='something://database',
        context={
            'c': {'name': 'db'},
            'e': {'name': 'dev'},
            'f': {'nrange': nrange, 'nrange2': nrange2}
        },
    )
    configuration = project.configure()
    integrity_configuration = project.configure_integrity()
    expected = "select 1, 2, 3;"

    def test_rendered_query_with_filter(self):
        query = self.configuration['queries']['query_one.sql']
        assert query == self.expected


class TestFunctionsRegistering:

    path = str((Path(__file__).parent / Path('fixtures/projects')))
    sqlbucket = SQLBucket(
        projects_folder=path,
        connections={'db_name': 'db_url'},
        env_name='dev',
        functions_registry=[nrange]
    )
    sqlbucket.register_function(nrange2)   # registering one function
    project = sqlbucket.load_project(
        project_name='project3',
        connection_name='db_name'
    )
    configuration = project.configure()

    def test_function_registration(self):
        expected = 'select 1, 2, 3;'
        assert self.configuration['queries']['query_one.sql'] == expected

    def test_register_function_method(self):
        expected = 'select 1, 2, 3, 4;'
        assert self.configuration['queries']['query_two.sql'] == expected
