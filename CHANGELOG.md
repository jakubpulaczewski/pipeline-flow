# Version history
This repository follows Semantic Versioning starting from the 1.0.0 release.
Minor version increments introduced new features, while patches are reserved for bug fixes.

## Version 1.0.7
- Reverted the issue by removing extra argument from logging to using %s.

## Version 1.0.6
- Added support for pre and post processing plugins for extract and load steps.

## Version 1.0.5

### Fixed
- Fixed the issue were secrets fetched by aws_secret_manager plugin will not parse if its a json string. Otherwise, it will return the response.

## Version 1.0.4

### Fixed
- Fixd the issue with parsing the actual secrets plugin.

## Version 1.0.3

### Fixed

- Fixed an issue with secrets not being replaced when passing them to plugins.

### Added
- Added support for nested key paths in secrets (e.g., `${{ secrets.SECRET_NAME.username }}`)
- Secrets now support dot notation to access nested values within JSON structures

## Version 1.0.2

### Fixed
- Instead of throwing an exception, just log a message when the plugin is registered.

## Version 1.0.1

### Fixed
- Abstracted the logging configuration if they already exist to not conflict with existing ones.

## Version 1.0.0

Initial release.
