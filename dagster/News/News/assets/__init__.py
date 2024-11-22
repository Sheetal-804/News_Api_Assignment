from dagster import load_assets_from_package_module
from . import news


NEWS = "news"
example_assets = load_assets_from_package_module(package_module=news, group_name=NEWS)
