class DynamoDBError(Exception):
    """
    Base class for all dynamo errors
    """


class NotATable(DynamoDBError):
    """
    This object is not a table.
    """


class ValidationError(DynamoDBError):
    """
    Error when validation fails.
    """
    def __init__(self, message):
        if isinstance(message, dict):
            self.message_dict = message

        elif isinstance(message, list):
            self.messages = message

        else:
            self.messages = [message]

    @property
    def error_messages(self):
        if hasattr(self, 'message_dict'):
            return self.message_dict
        else:
            return self.messages
