# import asyncio

# import yaml_example
# from core.executor import execute_pipelines
# from core.parser import deserialize_yaml, parse_jobs
# from plugins.loader import load_plugins


# if __name__ == "__main__":
#     yaml_str = ...

#     # Deserialiazed yaml
#     parsed_yaml = deserialize_yaml(yaml_str)

#     # Load Plugins
#     plugins = set(parsed_yaml["plugins"])
#     load_plugins(plugins)

#     parsed_jobs = parse_jobs(parsed_yaml["pipelines"])

#     asyncio.run(execute_pipelines(parsed_jobs))
