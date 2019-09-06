from sqlbucket import SQLBucket
import shutil
import os
import pytest
from pathlib import Path


class TestCreateProject:

    path = str((Path(__file__).parent / Path('fixtures/projects')).resolve())
    sqlbucket = SQLBucket(projects_folder=path)
    sqlbucket.create_project('test_project')
    test_project_folder = (sqlbucket.projects_path / 'test_project').resolve()

    def test_new_project_exist(self):
        assert 'test_project' in os.listdir(self.sqlbucket.projects_path)

    def test_structure(self):
        assert len(os.listdir(self.test_project_folder)) == 3
        assert 'config.yaml' in os.listdir(self.test_project_folder)
        assert 'queries' in os.listdir(self.test_project_folder)

    def test_duplicate_project(self):
        with pytest.raises(Exception):
            self.sqlbucket.create_project('test_project')

    def test_duplicate_existing_project(self):
        with pytest.raises(Exception):
            self.sqlbucket.create_project('project1')

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.test_project_folder)

