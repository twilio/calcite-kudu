1. `mvn versions:set -DnewVersion=1.0.13 -DgenerateBackupPoms=false`

2. Create a release tag: [link](https://github.com/twilio/calcite-kudu/releases)

3. Once the tag is created, travis-ci will start building a release immediately.
   Artifacts should be published to sonatype automatically.

4. edit these instructions and change the new version to `1.0.14`
