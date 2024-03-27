package it.agilelab.witboost.cdp.priv.hdfs.provisioning.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/***
 * Hdfs configuration
 * @param baseUrlNN1 Base URL for the WEBHDFS rest API (main NameNode)
 * @param baseUrlNN2 Base URL for the WEBHDFS rest API (secondary NameNode)
 * @param timeout Timeout in milliseconds
 */
@ConfigurationProperties(prefix = "hdfs")
public record HdfsConfig(String baseUrlNN1, String baseUrlNN2, Integer timeout) {}
