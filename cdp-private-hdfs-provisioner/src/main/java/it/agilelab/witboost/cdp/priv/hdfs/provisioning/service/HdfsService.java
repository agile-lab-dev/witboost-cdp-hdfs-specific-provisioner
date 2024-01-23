package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;

/***
 * Hdfs services
 */
public interface HdfsService {

    /***
     * Create a folder on HDFS
     * @param path path of the folder to create
     * @return the path of the created folder or the error encountered
     */
    Either<FailedOperation, String> createFolder(String path);

    /***
     * Delete a folder on HDFS. If the folder doesn't exist, a success result is returned
     * @param path path of the folder to delete
     * @return the path of the deleted folder or the error encountered
     */
    Either<FailedOperation, String> deleteFolder(String path);
}
