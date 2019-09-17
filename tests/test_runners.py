from sqlbucket.runners import ProjectRunner
import pytest


class TestRunnerProject:

    project_config = {
        "order": ["1.sql", "2.sql", "3.sql", "4.sql", "5.sql", "6.sql"]
    }

    def test_steps_default(self):
        runner = ProjectRunner(configuration=self.project_config)
        assert runner.from_step_index == 0
        assert runner.to_step_index == 5

    def test_steps_starts(self):
        runner = ProjectRunner(configuration=self.project_config, from_step=2)
        assert runner.from_step_index == 1
        assert runner.to_step_index == 5

    def test_steps_ends(self):
        runner = ProjectRunner(configuration=self.project_config, to_step=2)
        assert runner.from_step_index == 0
        assert runner.to_step_index == 1

    def test_multiple(self):
        runner = ProjectRunner(
            configuration=self.project_config, from_step=2, to_step=4)
        assert runner.from_step_index == 1
        assert runner.to_step_index == 3

    def test_same_index(self):
        runner = ProjectRunner(
            configuration=self.project_config, from_step=2, to_step=2)
        assert runner.from_step_index == 1
        assert runner.to_step_index == 1

    def test_error_steps(self):
        with pytest.raises(Exception):
            ProjectRunner(configuration=self.project_config,
                          from_step=4,
                          to_step=2)

