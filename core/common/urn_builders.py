# URN builder utilities

from datahub.emitter.mce_builder import make_dataset_urn

def build_dataset_urn(platform, name, env):
    return make_dataset_urn(platform=platform, name=name, env=env)