<br/>
<p align="center">
    <a href="https://www.agilelab.it/witboost">
        <img src="docs/img/witboost_logo.svg" alt="witboost" width=600 >
    </a>
</p>
<br/>

Designed by [Agile Lab](https://www.agilelab.it/), Witboost is a versatile platform that addresses a wide range of sophisticated data engineering challenges. It enables businesses to discover, enhance, and productize their data, fostering the creation of automated data platforms that adhere to the highest standards of data governance. Want to know more about Witboost? Check it out [here](https://www.agilelab.it/witboost) or [contact us!](https://www.agilelab.it/contacts)

This repository is part of our [Starter Kit](https://github.com/agile-lab-dev/witboost-starter-kit) meant to showcase Witboost's integration capabilities and provide a "batteries-included" product.

# CDP Private HDFS Specific Provisioner

- [Overview](#overview)
- [Building](#building)
- [Running](#running)
- [Configuring](#configuring)
- [Deploying](#deploying)
- [HLD](docs/HLD.md)
- [API specification](docs/API.md)

## Overview

This project implements a Specific Provisioner that provision HDFS Storage for a CDP Private environment.

### What's a Specific Provisioner?

A Specific Provisioner is a microservice which is in charge of deploying components that use a specific technology. When the deployment of a Data Product is triggered, the platform generates it descriptor and orchestrates the deployment of every component contained in the Data Product. For every such component the platform knows which Specific Provisioner is responsible for its deployment, and can thus send a provisioning request with the descriptor to it so that the Specific Provisioner can perform whatever operation is required to fulfill this request and report back the outcome to the platform.

You can learn more about how the Specific Provisioners fit in the broader picture [here](https://docs.witboost.agilelab.it/docs/p2_arch/p1_intro/#deploy-flow).

### CDP Private Cloud Base

CDP Private Cloud Base is a solution that enables organizations to deploy and manage their data infrastructure in a private cloud environment. This private cloud deployment is designed to offer enhanced security, control, and compliance for sensitive data, making it suitable for businesses with strict regulatory requirements or those who prefer to keep their data within their own infrastructure.

Explore CDP Private Cloud further consulting the [official documentation](https://docs.cloudera.com/cdp-private-cloud/latest/index.html).

### HDFS

Hadoop Distributed File System (HDFS) is a Java-based file system for storing large volumes of data. Designed to span large clusters of commodity servers, HDFS provides scalable and reliable data storage.

### Software stack

This microservice is written in Java 17, using SpringBoot for the HTTP layer. Project is built with Apache Maven and supports packaging and Docker image, ideal for Kubernetes deployments (which is the preferred option).

### Git hooks

Hooks are programs you can place in a hooks directory to trigger actions at certain points in git’s execution. Hooks that don’t have the executable bit set are ignored.

The hooks are all stored in the hooks subdirectory of the Git directory. In most projects, that’s `.git/hooks`.

Out of the many available hooks supported by Git, we use `pre-commit` hook in order to check the code changes before each commit. If the hook returns a non-zero exit status, the commit is aborted.


#### Setup Pre-commit hooks

In order to use `pre-commit` hook, you can use [**pre-commit**](https://pre-commit.com/) framework to set up and manage multi-language pre-commit hooks.

To set up pre-commit hooks, follow the below steps:

- Install pre-commit framework either using pip (or) using homebrew (if your Operating System is macOS):

    - Using pip:
      ```bash
      pip install pre-commit
      ```
    - Using homebrew:
      ```bash
      brew install pre-commit
      ```

- Once pre-commit is installed, you can execute the following:

```bash
pre-commit --version
```

If you see something like `pre-commit 3.3.3`, your installation is ready to use!


- To use pre-commit, create a file named `.pre-commit-config.yaml` inside the project directory. This file tells pre-commit which hooks needed to be installed based on your inputs. Below is an example configuration:

```bash
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
```

The above configuration says to download the `pre-commit-hooks` project and run its trailing-whitespace hook on the project.


- Run the below command to install pre-commit into your git hooks. pre-commit will then run on every commit.

```bash
pre-commit install
```

## Building

**Requirements:**

- Java 17
- Apache Maven 3.9+

**Version:** the version is set dynamically via an environment variable, `PROVISIONER_VERSION`. Make sure you have it exported, even for local development. Example:

```bash
export PROVISIONER_VERSION=0.0.0-SNAPHSOT
```

**Build:**

The project uses the `openapi-generator` Maven plugin to generate the API endpoints from the interface specification located in `src/main/resources/interface-specification.yml`. For more information on the documentation, check [API docs](docs/API.md).

```bash
mvn compile
```

**Type check:** is handled by Checkstyle:

```bash
mvn checkstyle:check
```

**Bug checks:** are handled by SpotBugs:

```bash
mvn spotbugs:check
```

**Tests:** are handled by JUnit:

```bash
mvn test
```

**Artifacts & Docker image:** the project leverages Maven for packaging. Build artifacts (normal and fat jar) with:

```bash
mvn package spring-boot:repackage
```

The Docker image can be built with:

```bash
docker build .
```

More details can be found [here](docs/docker.md).

*Note:* when running in the CI/CD pipeline the version for the project is automatically computed using information gathered from Git, using branch name and tags. Unless you are on a release branch `1.2.x` or a tag `v1.2.3` it will end up being `0.0.0`. You can follow this branch/tag convention or update the version computation to match your preferred strategy. When running locally if you do not care about the version (ie, nothing gets published or similar) you can manually set the environment variable `PROVISIONER_VERSION` to avoid warnings and oddly-named artifacts; as an example you can set it to the build time like this:
```bash
export PROVISIONER_VERSION=$(date +%Y%m%d-%H%M%S);
```

**CI/CD:** the pipeline is based on GitLab CI as that's what we use internally. It's configured by the `.gitlab-ci.yaml` file in the root of the repository. You can use that as a starting point for your customizations.

## Running

To run the server locally, use:

```bash
export PROVISIONER_VERSION=0.0.0-SNAPHSOT
mvn -pl cdp-private-hdfs-provisioner spring-boot:run
```

By default, the server binds to port `8888` on localhost. After it's up and running you can make provisioning requests to this address. You can access the running application [here](http://127.0.0.1:8888).

SwaggerUI is configured and hosted on the path `/docs`. You can access it [here](http://127.0.0.1:8888/docs)

## Configuring

Application configuration is handled using the features provided by Spring Boot. You can find the default settings in the `application.yml`. Customize it and use the `spring.config.location` system property or the other options provided by the framework according to your needs.

### Kerberos configuration

| Configuration           | Description                                          | 
|:------------------------|:-----------------------------------------------------|
| kerberos.keytabLocation | Location of the keytab to be used for authentication | 
| kerberos.principal      | Principal                                            | 

The chart provides a `krb5.conf` that needs to be configured properly. This file will be mounted automatically in `/opt/docker/etc/configs/` and the application will load it using the system property `java.security.krb5.conf`.

The chart loads the `keytab` from a secret with key `cdp-private-hdfs-keytab`. The keytab is expected to be base64 encoded (the chart takes care of decoding it).

### Hdfs configuration

| Configuration   | Description                                            | 
|:----------------|:-------------------------------------------------------|
| hdfs.baseUrlNN1 | Base URL for the WEBHDFS Rest API (main NameNode)      |
| hdfs.baseUrlNN2 | Base URL for the WEBHDFS Rest API (secondary NameNode) | 
| hdfs.timeout    | Timeout in milliseconds                                | 

### Ranger configuration

| Configuration             | Description                                                                                                                                                                   | 
|:--------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ranger.baseUrl            | Base URL for the Ranger Rest API                                                                                                                                              | 
| ranger.timeout            | Timeout in milliseconds                                                                                                                                                       |
| ranger.username           | Ranger username                                                                                                                                                               |
| ranger.password           | Ranger password. The default value in the chart is `${RANGER_PASSWORD}`. With this syntax Spring will retrieve the value from an environment variable named `RANGER_PASSWORD` |
| ranger.hdfsServiceName    | HDFS service name                                                                                                                                                             |
| ranger.ownerTechnicalUser | Ranger user that will be admin of the security zone and will be included in the owner role                                                                                    |

### Ldap configuration

| Configuration                   | Description                                                                                                                                                                       | 
|:--------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| mapping.ldap.url                | Ldap url                                                                                                                                                                          |
| mapping.ldap.useTls             | Tls enable flag                                                                                                                                                                   |
| mapping.ldap.timeout            | Timeout in milliseconds                                                                                                                                                           |
| mapping.ldap.bindUsername       | Bind user                                                                                                                                                                         |
| mapping.ldap.bindPassword       | Bind password. The default value in the chart is `${LDAP_BIND_PASSWORD}`. With this syntax Spring will retrieve the value from an environment variable named `LDAP_BIND_PASSWORD` |
| mapping.ldap.searchBaseDN       | Base DN                                                                                                                                                                           |
| mapping.ldap.userSearchFilter   | Ldap filter for user search                                                                                                                                                       |
| mapping.ldap.groupSearchFilter  | Ldap filter for group search                                                                                                                                                      |
| mapping.ldap.userAttributeName  | Ldap attribute name for user Id                                                                                                                                                   |
| mapping.ldap.groupAttributeName | Ldap attribute name for group Id                                                                                                                                                  |

### Custom Root CA

The chart provides the option `customCA.enabled` to add a custom Root Certification Authority to the JVM truststore. If this option is enabled, the chart will load the custom CA from a secret with key `cdp-private-hdfs-custom-ca`. The CA is expected to be in a format compatible with `keytool` utility (PEM works fine).

## Deploying

This microservice is meant to be deployed to a Kubernetes cluster with the included Helm chart and the scripts that can be found in the `helm` subdirectory. You can find more details [here](helm/README.md).

## License

This project is available under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0); see [LICENSE](LICENSE) for full details.

## About us

<br/>
<p align="center">
    <a href="https://www.agilelab.it">
        <img src="docs/img/agilelab_logo.svg" alt="Agile Lab" width=600>
    </a>
</p>
<br/>

Agile Lab creates value for its Clients in data-intensive environments through customizable solutions to establish performance driven processes, sustainable architectures, and automated platforms driven by data governance best practices.

Since 2014 we have implemented 100+ successful Elite Data Engineering initiatives and used that experience to create Witboost: a technology-agnostic, modular platform, that empowers modern enterprises to discover, elevate and productize their data both in traditional environments and on fully compliant Data mesh architectures.

[Contact us](https://www.agilelab.it/contacts) or follow us on:
- [LinkedIn](https://www.linkedin.com/company/agile-lab/)
- [Instagram](https://www.instagram.com/agilelab_official/)
- [YouTube](https://www.youtube.com/channel/UCTWdhr7_4JmZIpZFhMdLzAA)
- [Twitter](https://twitter.com/agile__lab)