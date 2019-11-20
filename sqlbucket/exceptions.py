class ProjectNotFound(Exception):
    pass


class ConnectionNotFound(Exception):
    pass


class GroupNotFound(KeyError):
    pass


class OrderNotInRightFormat(TypeError):
    pass


class ReservedVariableNameError(Exception):
    pass


class PassedFieldNotInQuery(Exception):
    pass
