# import asyncio

# import yaml_example
# from core.executor import execute_jobs
# from core.parser import deserialize_yaml, parse_jobs
# from plugins.loader import load_plugins

# DEFAULT_PLUGINS = ["s3"]

# if __name__ == "__main__":
#     yaml_str = yaml_example.yaml_str

#     # Deserialiazed yaml
#     parsed_yaml = deserialize_yaml(yaml_str)

#     # Load Transition
#     # transition = load_transition(parsed_yaml["transition"])

#     # Load Plugins
#     plugins = set(DEFAULT_PLUGINS + parsed_yaml["plugins"])
#     load_plugins(plugins)

#     parsed_jobs = parse_jobs(parsed_yaml["jobs"])

#     asyncio.run(execute_jobs(parsed_jobs))
