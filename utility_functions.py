"""
This module assists with converting a string to an int or float.
Only the convert_string_to_numerical() function is available to outside files
"""


# Strings contained in triple quotes are docstrings.
# This module has a docstring describing the module itself (above) and docstrings describing each function.

# Functions that begin with an underscore ('_') are only available for use inside the file that they are declared
# and defined in. For this module, the _string_is_int() and _string_is_float() functions can only be used inside
# this file; they are not available to outside files that import this module.

def _string_is_int(in_string):
    """ returns True if the incoming parameter is an int, returns False otherwise """
    try:
        int(in_string)
        return True
    except TypeError:
        return False
    except ValueError:
        return False


def _string_is_float(in_string):
    """ returns True if the incoming parameter is a float, returns False otherwise """
    try:
        float(in_string)
        return True
    except TypeError:
        return False
    except ValueError:
        return False


def convert_string_to_numerical(in_string):
    """ this function converts a string to a numerical value (to either an int or float)
        'None' is returned if the incoming string is not in the form of an int or float """
    if _string_is_int(in_string):
        return int(in_string)
    elif _string_is_float(in_string):
        return float(in_string)
    return None
