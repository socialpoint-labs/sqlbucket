from sqlbucket.project import ContextMerger


class TestVariablesConfiguration:

    def test_empty_config(self):
        project_config = dict()
        context = {'c': {'name': 'conn'}, 'e': {'name': 'env'}}

        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        assert context == {'c': {'name': 'conn'}, 'e': {'name': 'env'}}

    def test_connection_and_env_not_in_config(self):
        project_config = {
            'connection_variables': {'conn1': {'foo': 'bar'}},
            'environment_variables': {'env1': {'foofoo': 'barbar'}}
        }
        context = {'c': {'name': 'conn2'}, 'e': {'name': 'env2'}}

        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        assert context == {'c': {'name': 'conn2'}, 'e': {'name': 'env2'}}

    def test_connection_and_env_not_in_config_with_submitted_vars(self):
        project_config = {
            'connection_variables': {'conn1': {'foo': 'bar'}},
            'environment_variables': {'env1': {'foofoo': 'barbar'}}
        }

        context = {
            'c': {'name': 'conn2'},
            'e': {'name': 'env2', 'submitted_env_foo': 'submitted_env_bar'},
            'submitted_foo': 'submitted_bar'
        }

        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        assert context == {
            'submitted_foo': 'submitted_bar',
            'e': {'name': 'env2', 'submitted_env_foo': 'submitted_env_bar'},
            'c': {'name': 'conn2'}
        }

    def test_connection_and_env_in_config(self):
        project_config = {
            'connection_variables': {'conn1': {'foo': 'bar'}},
            'environment_variables': {'env1': {'foofoo': 'barbar'}}
        }
        context = {
            'c': {'name': 'conn1'},
            'e': {'name': 'env1'},
        }
        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        assert context == {
            'e': {'name': 'env1', 'foofoo': 'barbar'},
            'c': {'name': 'conn1', 'foo': 'bar'}
        }

    def test_variables_overwrite(self):
        project_config = {
            'connection_variables': {'conn1': {'foo': 'bar'}},
            'env_variables': {'env1': {'foofoo': 'barbar'}}
        }

        context = {
            'foo': 'barbar',
            'c': {'name': 'conn1'},
            'e': {'name': 'env1', 'foofoo': 'bar'},
        }

        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        assert context == {
            'foo': 'barbar',
            'e': {'name': 'env1', 'foofoo': 'bar'},
            'c': {'name': 'conn1', 'foo': 'bar'}
        }

    def test_only_submitted_variables(self):
        project_config = dict()

        context = {
            'foo': 'barbar',
            'c': {'name': 'conn1'},
            'e': {'name': 'env1', 'foofoo': 'barbar'},
        }

        context = ContextMerger(
            context=context,
            context_from_config=project_config
        ).merge()

        # context should have not changed.
        assert context == {
            'foo': 'barbar',
            'c': {'name': 'conn1'},
            'e': {'name': 'env1', 'foofoo': 'barbar'},
        }

