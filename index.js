(function () {

    "use strict";

    var azure = require('azure-storage');
    var Promise = require('bluebird');
    var fs = require('fs');

    var TaskQueue = require('./libs/TaskQueue');
    var ProgressBar = require('progress');

    var checkQueue = new TaskQueue(10);
    var containerQueue = new TaskQueue(10);
    var streamQueue = new TaskQueue(5);
    var syncQueue = new TaskQueue(2);

    var startAt = 0;
    var syncBar, streamBar;
    var querying = {};
    var validating = {};
    var streaming = {};
    var creating = {};
    var source, target;
    var blobStorage = process.argv[2];

    switch (blobStorage)
    {
        case "storage" :
            source = azure.createBlobService(process.env["HILCO_AZURE_SOURCE_STORAGE_ACCOUNT"], process.env["HILCO_AZURE_SOURCE_STORAGE_KEY"]);
            target = azure.createBlobService(process.env["HILCO_AZURE_TARGET_STORAGE_ACCOUNT"], process.env["HILCO_AZURE_TARGET_STORAGE_KEY"]);
            break;
        case "cdn" :
            source = azure.createBlobService(process.env["HILCO_AZURE_SOURCE_CDN_ACCOUNT"], process.env["HILCO_AZURE_SOURCE_CDN_KEY"]);
            target = azure.createBlobService(process.env["HILCO_AZURE_TARGET_CDN_ACCOUNT"], process.env["HILCO_AZURE_TARGET_CDN_KEY"]);
            break;
        default:
            throw new Error("Incorrect blob storage pairing identifier given!");
    }
    
    function extractNumberFromProjectContainerName(name)
    {
        var parts = name.split("-");
        return parseInt(parts[1]);
    }

    /**
     * Promise which retrieves a list of containers from the source Azure Blob Storage
     * 
     * @method getContainers
     * 
     * @returns {Promise}
     */
    function getContainers()
    {
        return new Promise(function (resolve, reject) {
            source.listContainersSegmented(null, function (error, result) {
                if (error)
                    return reject(error);
                resolve(result.entries);
            });

        });
    }

    /**
     * Promise which loops through an array of getContainer results
     * filtering all which do not beging with "proj-" indicating they
     * house asset images.
     * 
     * @method   parseContainers
     * 
     * @param    {Array}  result
     *           
     * @returns  {Promise}
     */
    function parseContainers(result)
    {
        var length = result.length;

        // Check there are containers to parse
        if (length === 0)
        {
            return Promise.resolve({});
        }

        var toSynch = [];

        return new Promise(function (resolve, reject) {
            var completed = 0;

            result.forEach(function (container) {
                var projectNumber = extractNumberFromProjectContainerName(container.name);
                if (container.name.indexOf('proj-') > -1 && (projectNumber >= startAt))
                {
                    toSynch.push(container);
                    var task = function ()
                    {
                        return createContainer(container.name)
                                .then(function (result) {

                                    if (++completed === length)
                                    {
                                        return resolve(toSynch);
                                    }
                                })
                                .catch(reject);
                    };

                    containerQueue.pushTask(task);
                } else
                {
                    ++completed;
                }

            });
        });
    }

    /**
     * Promise creating a new containers if needed to be added to chain.
     * 
     * @method   createContainer
     * 
     * @param    {String}   container
     * 
     * @returns  {Promise}
     */
    function createContainer(container)
    {

        // Check container creation not already started for this instance
        if (creating[container])
        {
            return Promise.resolve({});
        }

        creating[container] = true;

        return new Promise(function (resolve, reject) {
            target.createContainerIfNotExists(container, {publicAccessLevel: 'blob'}, function (error) {
                if (error)
                    return reject(error);

                resolve(container);
            });
        });
    }

    /**
     * @method   syncContainers
     * 
     * @param    {Array} result
     * 
     * @returns  {Promise}
     */
    function syncContainers(result)
    {
        var length = result.length;

        // Check there are containers to sync
        if (length === 0)
        {
            return Promise.resolve({});
        }

        syncBar = new ProgressBar('Azure Storage [:bar] :percent :etas', {
            complete: '=',
            incomplete: ' ',
            width: 80,
            total: length,
            clear: false
        });

        return new Promise(function (resolve, reject) {
            var completed = 0;

            result.forEach(function (container) {

                var task = function ()
                {
                    return getBlobs(container.name)
                            .then(validateBlobs)
                            .then(streamBlobs)
                            .then(function (result) {

                                syncBar.tick(1);

                                if (++completed === length)
                                {
                                    resolve();
                                }
                            })
                            .catch(reject);
                };

                syncQueue.pushTask(task);
            });
        });
    }

    /**
     * @method   getBlobs
     * 
     * @param    {String}   container
     * 
     * @returns  {Promise}
     */
    function getBlobs(container)
    {

        // Check if already querying container for blobs
        if (querying[container])
        {
            return Promise.resolve({});
        }

        querying[container] = true;

        return new Promise(function (resolve, reject) {
            source.listBlobsSegmented(container, null, function (error, result) {
                if (error)
                    return reject(error);

                resolve({
                    "container": container,
                    "blobs": result.entries || []
                });
            });
        });
    }
    ;

    /**
     * @method   blobExists
     * 
     * @param    {String}      container
     * @param    {BlobResult}  blob
     * 
     * @returns  {Promise}
     */
    function blobExists(container, blob)
    {
        // Check blobs existence not alrady executed
        if (validating[blob.name])
        {
            return Promise.resolve({});
        }

        validating[blob.name] = true;

        return new Promise(function (resolve, reject) {
            target.doesBlobExist(container, blob.name, false, function (error, result) {
                if (error)
                    return reject(error);

                // add meaning to result
                resolve({
                    "blob": blob,
                    "destination": result || false
                });
            });
        });
    }
    ;

    /**
     * @method   validateBlobs
     * 
     * @param    {Object}   result
     * 
     * @returns  {Promise}
     */
    function validateBlobs(result) {
        var container = result.container;
        var blobs = result.blobs;
        var length = blobs.length;

        if (length === 0)
        {
            return Promise.resolve({container: false});
        }

        // Start progress bar for blob validation within this container
        var bar = new ProgressBar(container + ' [:bar] :percent :etas', {
            complete: '=',
            incomplete: ' ',
            width: 80,
            total: length,
            clear: true
        });

        var blobsToSynch = [], sizeOfSynch = 0;

        return new Promise(function (resolve, reject) {

            var completed = 0;

            blobs.forEach(function (blob) {

                var task = function ()
                {
                    return blobExists(container, blob)
                            .then(function (result) {

                                bar.tick(1);

                                // Check if blob exists, if not add to synchables
                                if (result.destination && result.destination.exists === false)
                                {
                                    blobsToSynch.push(result.blob);
                                    sizeOfSynch += parseInt(result.blob.contentLength);
                                }

                                // If blob exists, check if content MD5 hash differs
                                else if (result.destination.exists === true && result.blob.contentSettings.contentMD5 !== result.destination.contentSettings.contentMD5)
                                {
                                    blobsToSynch.push(result.blob);
                                    sizeOfSynch += parseInt(result.blob.contentLength);
                                }

                                // Completion check with final resolve for grouping
                                if (++completed === length)
                                {
                                    return resolve({
                                        "container": container,
                                        "blobs": blobsToSynch,
                                        "size": sizeOfSynch
                                    });
                                }
                            })
                            .catch(reject);

                };

                checkQueue.pushTask(task);
            });
        });
    }
    ;


    /**
     * @method   getBlobProperties
     * 
     * @param    {String}   container
     * @param    {String}   blob
     * 
     * @returns  {Promise}
     */
    function getBlobProperties(container, blob)
    {
        return new Promise(function (resolve, reject) {
            source.getBlobProperties(container, blob, function (error, result) {
                if (error)
                    return reject(error);

                resolve(result);
            });
        });
    }

    /**
     * @method   stream
     * 
     * @param    {String}          container
     * @param    {String}          blob
     * @param    {BlobProperties}  properties
     * 
     * @returns  {Promise}
     */
    function stream(container, blob, properties)
    {
        return new Promise(function (resolve, reject) {

            // Manually recreate properties object to be added to read stream
            var options = {
                contentSettings: properties.contentSettings
            };

            source.createReadStream(container, blob)
                    .pipe(target.createWriteStreamToBlockBlob(container, blob, options))
                    .on('data', function (chunk) {
                        streamBar.tick(chunk.length);
                    })
                    .on('error', reject)
                    .on('end', resolve);
        });
    }

    /**
     * @method   synchronizeBlob
     * 
     * @param    {String}   container
     * @param    {String}   blob
     * 
     * @returns  {Promise}
     */
    var synchronizeBlob = function (container, blob)
    {
        if (streaming[blob])
        {
            return Promise.resolve({});
        }

        streaming[blob] = true;

        return getBlobProperties(container, blob)
                .then(function (properties) {
                    return stream(container, blob, properties);
                });
    };

    /**
     * @method   streamBlobs
     * 
     * @param    {Object}   result
     * 
     * @returns  {Promise}
     */
    var streamBlobs = function (result)
    {
        if (!result.container)
        {
            return Promise.resolve({});
        }

        var container = result.container;
        var blobs = result.blobs;
        var length = blobs.length;

        if (length === 0)
        {
            return Promise.resolve();
        }

        streamBar = new ProgressBar(container + ' [:bar] :current bytes of :total bytes :percent :etas', {
            complete: '=',
            incomplete: ' ',
            width: 80,
            total: result.size,
            clear: true
        });

        return new Promise(function (resolve, reject) {

            var completed = 0;

            blobs.forEach(function (blob) {
                var task = function ()
                {
                    return synchronizeBlob(container, blob.name)
                            .then(function () {
                                if (++completed === length)
                                {
                                    resolve();
                                }
                            })
                            .catch(reject);
                };

                streamQueue.pushTask(task);
            });
        });
    };

    /**
     * @property   container
     * @type       {String}
     */
    var container = process.argv[3];
    
    // Confirm starting at container X flag not present
    if (container.indexOf("...") === 0 && blobStorage === "storage")
    {
        startAt = extractNumberFromProjectContainerName(container);
        container = false;
    }

    // If container has value then run blob synchronization against it
    if (container)
    {
        getBlobs(container)
                .then(validateBlobs)
                .then(streamBlobs)
                .then(function (result) {

                    console.log('DONE!');
                    //console.log(result);
                })
                .catch(function (e) {
                    console.log(e);
                });
    }
    else
    {

        // No container provided, run synchronization against all containers
        getContainers()
                .then(parseContainers)
                .then(syncContainers)
                .then(function (result) {
                    console.log(result);
                    console.log('DONE!');
                })
                .catch(function (e) {
                    console.error(e.stack);
                });
    }


})();