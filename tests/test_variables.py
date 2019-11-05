from sqlbucket.project import configure_variables


class TestVariablesConfiguration:

    def test_empty_config(self):
        project_config = dict()
        submitted_variables = dict()
        submitted_env_variables = dict()
        connection_name = 'conn'
        env_name = 'env'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {'vars': dict(), 'env': dict(),
                             'connection_name': 'conn'}

    def test_connection_and_env_not_in_config(self):
        project_config = {
            'variables': {'conn1': {'foo': 'bar'}},
            'env_variables': {'env1': {'foofoo': 'barbar'}}
        }
        submitted_variables = dict()
        submitted_env_variables = dict()
        connection_name = 'conn2'
        env_name = 'env2'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {'vars': dict(),
                             'env': dict(),
                             'connection_name': 'conn2'}

    def test_connection_and_env_not_in_config_with_submitted_vars(self):
        project_config = {
            'variables': {'conn1': {'foo': 'bar'}},
            'env_variables': {'env1': {'foofoo': 'barbar'}}
        }
        submitted_variables = {'submitted_foo': 'submitted_bar'}
        submitted_env_variables = {'submitted_env_foo': 'submitted_env_bar'}
        connection_name = 'conn2'
        env_name = 'env2'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {
            'vars': {'submitted_foo': 'submitted_bar'},
            'env': {'submitted_env_foo': 'submitted_env_bar'},
            'connection_name': 'conn2'
        }

    def test_connection_and_env_in_config(self):
        project_config = {
            'variables': {'conn1': {'foo': 'bar'}},
            'env_variables': {'env1': {'foofoo': 'barbar'}}
        }
        submitted_variables = dict()
        submitted_env_variables = dict()
        connection_name = 'conn1'
        env_name = 'env1'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {
            'vars': {'foo': 'bar'},
            'env': {'foofoo': 'barbar'},
            'connection_name': 'conn1'
        }

    def test_variables_overwrite(self):
        project_config = {
            'variables': {'conn1': {'foo': 'bar'}},
            'env_variables': {'env1': {'foofoo': 'barbar'}}
        }
        submitted_variables = {'foo': 'barbar'}
        submitted_env_variables = {'foofoo': 'bar'}
        connection_name = 'conn1'
        env_name = 'env1'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {
            'vars': {'foo': 'barbar'},
            'env': {'foofoo': 'bar'},
            'connection_name': 'conn1'
        }

    def test_only_submitted_variables(self):
        project_config = dict()
        submitted_variables = {'foo': 'bar'}
        submitted_env_variables = {'foofoo': 'barbar'}
        connection_name = 'conn1'
        env_name = 'env1'
        variables = configure_variables(
            project_config=project_config,
            submitted_variables=submitted_variables,
            submitted_env_variables=submitted_env_variables,
            connection_name=connection_name,
            env_name=env_name
        )

        assert variables == {
            'vars': {'foo': 'bar'},
            'env': {'foofoo': 'barbar'},
            'connection_name': 'conn1'
        }

