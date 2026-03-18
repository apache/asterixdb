# Building AsterixDB on Windows

To successfully build the project on Windows, use the following Maven flags to bypass common path and environment issues:

### Build Command
`mvn clean install -DskipTests -Dmdep.analyze.skip=true`

### Troubleshooting
- **Dependency Analysis Errors:** Use `-Dmdep.analyze.skip=true` to avoid Windows path resolution bugs.
- **Java Permissions:** If using Java 17+, add `--add-opens java.base/java.io=ALL-UNNAMED` to your VM options.
- **Dashboard Failures:** If the UI build fails, you can skip it with `-pl !asterix-dashboard`.