## Versioning Virtual Schemas and SQL Dialects

All dialects have the same version as the master project. In the master `pom.xml` file a property called `product-version` is set. Use this in as the artifact version number in the JDBC adapter and all dialects.

Run the script

```bash
jdbc-adapter/tools/version.sh verify
```

To check that all documentation and templates reference the same version number. This script is also used as a build breaker in the continuous integration script.

To update documentation files run

```bash
jdbc-adapter/tools/version.sh unify
```

Note that the script must be run from the root directory of the virtual schema project.