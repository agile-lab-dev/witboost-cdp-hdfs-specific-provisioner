package it.agilelab.witboost.cdp.priv.hdfs.provisioning.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/***
 * Hdfs configuration
 * @param baseUrl Base URL for the WEBHDFS rest API
 * @param timeout Timeout in milliseconds
 */
@ConfigurationProperties(prefix = "hdfs")
public record HdfsConfig(String baseUrl, Integer timeout) {}
