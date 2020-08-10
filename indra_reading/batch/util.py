from indra.literature import elsevier_client as ec
from indra.literature.elsevier_client import _ensure_api_keys


bucket_name = 'bigmech'


@_ensure_api_keys('remote batch reading', [])
def get_elsevier_api_keys():
    return [
        {'name': ec.API_KEY_ENV_NAME,
         'value': ec.ELSEVIER_KEYS.get('X-ELS-APIKey', '')},
        {'name': ec.INST_KEY_ENV_NAME,
         'value': ec.ELSEVIER_KEYS.get('X-ELS-Insttoken', '')},
    ]


def get_environment():
    # Get the Elsevier keys from the Elsevier client
    environment_vars = get_elsevier_api_keys()

    # Only include values that are not empty.
    return [var_dict for var_dict in environment_vars
            if var_dict['value'] and var_dict['name']]


