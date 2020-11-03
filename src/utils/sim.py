def clean_data(x):
    """Function to convert all strings to lower case and strip names of spaces

    Args:
        x ([type]): row of DataFrame

    Returns:
        str|list: result str or list of str
    """
    if isinstance(x, list):
        return [str.lower(i.replace(" ", "")) for i in x]
    else:
        # Check if exists. If not, return empty string
        if isinstance(x, str):
            return str.lower(x.replace(" ", ""))
        else:
            return ''


def create_soup(x, features):
    return ' '.join([x[col] for col in features])
