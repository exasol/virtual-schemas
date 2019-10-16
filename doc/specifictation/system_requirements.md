# Virtual Schema System Requirement Specification

# Terms and Abbreviations

* Integration Test Environment: technical environment integration tests run on
* Integration Test Framework: framework that sets up, runs and cleans up after integration tests

## Non-functional Requirements

### Requirements for the Integration Test Framework of Exasol's Virtual Schemas

Virtual Schemas build bridges between Exasol installations and 3rd-party data sources. In order to fully test the Virtual Schema software, we need integration tests where the Virtual Schemas access data on test instances of the data sources.

#### Automatic Installation Integration Test Environment

`itfw~non-interactive-installation-integration-test-environment~1`

All parts of the integration test environment are installed without user interaction.

Rationale:

This allows running the integration tests from scripts. For example inside a continuous build.

#### Download of 3rd-party Data Source Images via Internet

`itfw~download-of-3rd-party-data-source-images-via-internet~1¸

The ITFW downloads the images for 3rd-party data source instances via the Internet.

Rationale:

The environment setup must work from any machine connected to the Internet without forcing the user to install anything from other sources (like disks or manually downloaded archives).

#### Fast 3rd-party Data Source Installation Time Goal

`itfw~fast-3rd-party-data-source-installation~1`

A 3rd-party data source should be installed an running within 2 minutes including download.

Comment:

This is a requirement where we control only a small part of the chain. We can neither influence the download speed nor how long setup or startup takes. What we can do though is to pick mechanisms that have the chance of being quick.

### Respect 3rd-party Licenses

`itfw~respect-3rd-party-licenses~1`

We only use 3rd-party tools in those integration tests where the licenses permit it.

Comment:

Especially in case of commercial products there is no guarantee that a developer license exists that allows running the 3rd-party tool in a integration test. In those cases we are out of luck and cannot really provide regression testing. While this will have an impact on the quality of the corresponding SQL dialect adapter, adhering to the licenses is more important.

#### 3rd-party Instance Setup Controlled by Test Case

`itfw~3rd-party-instance-setup-controlled-by-test-case~1`

A test case in the test suite controls installation and removal of the 3rd-party data source instance.

Rationale:

Each test should be able to define what test setup it needs and get the necessary 3rd-party data source up and running. This way you can select test cases and the according setup is only made for the selected ones. This is especially helpful if you work on a SQL dialect and want to run integration tests on that one quickly.

#### Test Data Controlled by Test Case

`itfw~test-data-controlled-by-test-case~1`

A test case in the test suite creates and removes all test data it needs.

Rationale:

Test preparation, execution and evaluation must be in a single place in order to make the tests maintainable. This also removes temporal coupling (the worst kind of coupling) between test setup and execution.

#### Exasol Instance Setup for the Integration Tests

`itfw~exasol-instance-setup-for-integration-tests~1`

The integration test framework automatically sets up an Exasol instance that hosts the Virtual Schema.

#### Exasol Image Configurable for Integration Tests

`itfw~exasol-image-configurable-for-integration-tests~1`

A configuration parameter decides which image of Exasol the ITFW installs to host the Virtual Schema.

Rationale:

This way we can test with different Exasol versions and variants.

### Language Container Configurable for Integration Tests

A configuration parameter decides which language container the ITFW uses for the integration tests.

Rationale:

We develop new language containers in parallel with Exasol versions, so they don't have the same release cycle. This allows us to do matrix tests with development versions of the language containers prior to releasing them.

Comment:

A possible solution could be to pre-build the combinations of Exasol and language container versions and provide this as images. Alternatively we could patch the language container after installing Exasol.

#### Exasol Image Download via Internet

`itfw~exasol-image-download-via-internet~1`

The ITFW downloads the Exasol instance image via the Internet.

Rationale:

This allows bootstrapping the integration test environment on a fresh machine as is usually the case for continuous builds.

#### Language Container Download via Internet

`itfw~language-container-download-via-internet~1`

The ITFW downloads the language container via the Internet.

#### Integration Test Instance Image Download Cache

`itfw~integration-test-instance-image-download-cache~1`

The ITFW offers a cache where previously downloaded images are kept.

Rationale:

This speeds up subsequent integration tests.

#### Integration Test Result Logs are in the Same Format as for Unit Tests

`itfw~integration-test-results-same-format-as-for-unit-tests~1`

The results of the integration tests are logged in the same format and place as for unit tests.

Rationale:

We want the test results to contribute to code coverage statistics and be readable by the same tools as the unit tests (e.g. SonarCloud).

#### Exasol Beta Image Access

`itfw~exasol-beta-image-access~1`

The ITFW supports running the test with beta version images of Exasol.

Comment:

Beta versions are not publicly available on Docker Hub. Access is restricted to authorized users.

#### Testing Against Different Version of 3rd-party Data Sources

`itfw~