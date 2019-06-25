from sqlflow import SQLFlow


class TestIntegrityRendering:

    sqlflow = SQLFlow(
        projects_folder='fixtures/projects',
        connections={'db_name': 'db_url'},
        env_name='dev'
    )
    project = sqlflow.load_project(
        project_name='project1', connection_name='db_name'
    )
    integrity = project.configure_integrity()

    def test_format(self):
        assert self.integrity.get("queries") is not None
        assert self.integrity.get("order") is not None
        assert self.integrity.get("context") is not None

    def test_queries(self):
        assert self.integrity.get("queries") == {
            "folder1/integrity2.sql": "foobar",
            "integrity1.sql": "barbar"
        }
        assert self.integrity.get("order") == [
            "folder1/integrity2.sql",
            "integrity1.sql"
        ]

