from sqlbucket import SQLBucket
from sqlbucket.integrity import IntegrityCheck
from sqlbucket.exceptions import PassedFieldNotInQuery
from pathlib import Path
import pytest


class TestIntegrityRendering:

    path = str((Path(__file__).parent / Path('fixtures/projects')).resolve())
    sqlbucket = SQLBucket(
        projects_folder=path,
        connections={'db_name': 'db_url'},
        env_name='dev'
    )
    project = sqlbucket.load_project(
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
            "integrity1.sql": "bar"
        }
        assert self.integrity.get("order") == [
            "folder1/integrity2.sql",
            "integrity1.sql"
        ]


class TestIntegrityCheck:

    integrity = IntegrityCheck(
        rows=[{"passed": True, "field": "value"}],
        query_name='whatever.sql'
    )

    def test_has_passed_true(self):
        rows = [{"passed": True}]
        integrity = IntegrityCheck(rows=rows, query_name='foo.sql')
        assert integrity.has_passed() is True
        assert integrity.log_summary('whatever') == 'success'

    def test_has_passed_false(self):
        rows = [{"passed": False}]
        integrity = IntegrityCheck(rows=rows, query_name='foo.sql')
        assert integrity.has_passed() is False
        assert integrity.log_summary('whatever') == 'FAILED'

    def test_error_missing_passed_field(self):
        rows = [{"whatever": True}]
        integrity = IntegrityCheck(rows=rows, query_name='foo.sql')
        with pytest.raises(PassedFieldNotInQuery):
            integrity.log_summary('whatever')

    def test_null_passed_field(self):
        rows = [{"passed": None}]
        integrity = IntegrityCheck(rows=rows, query_name='foo.sql')
        assert integrity.has_passed() is False
        assert integrity.log_summary('whatever') == 'FAILED'
