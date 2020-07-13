# encoding: utf-8
import six
class ActionError(Exception):
    def __init__(self, error_dict=None):
        if not isinstance(error_dict, dict):
            error_dict = {'success': False, 'message': error_dict}
        self.error_dict = error_dict
        super(ActionError, self).__init__(error_dict)

    def __str__(self):
        msg = self.error_dict
        if not isinstance(msg, six.string_types):
            msg = str(msg)
        return six.ensure_text(msg)

class NotFound(ActionError):
    '''Exception raised by logic functions when a given is not found.'''
    pass


class NotAuthorized(ActionError):
    '''Exception raised when the user is not authorized to call the action.'''
    pass


class ValidationError(ActionError):
    '''Exception raised by action functions when validation fails.'''
    pass

class RequestError(ActionError):
    '''Exception raised by logic functions when a given is not found.'''
    pass

class DatabaseError(ActionError):
    '''Exception raised by logic functions when a given is not found.'''
    pass