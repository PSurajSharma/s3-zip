require('aws-sdk/lib/maintenance_mode_message').suppress = true;
require('events').EventEmitter.prototype._maxListeners = 100;

const Client = require('ssh2-sftp-client');
const sftp = new Client();

const AWS = require("aws-sdk");
const stream = require("stream");
const archiver = require("archiver");
const https = require("https");
const lazystream = require("lazystream");
const zlib = require('zlib');
const {app} = require("serverless/lib/cli/commands-schema/common-options/aws-service");
const path = require('path');
const {sleep} = require("ssh2-sftp-client/src/utils");
const agent = new https.Agent({keepAlive: true, maxSockets: 16});

AWS.config.update({httpOptions: {agent}, region: "eu-south-2"});

const s3 = new AWS.S3();

const transfer = async function sftpTransfer(sftpConfig, params, outputKey) {
    // await sleep(10000)
    console.log(`Initiating file transfer for ${outputKey} to ${sftpConfig.host}`);
    if (!sftp.sftp) {
        console.log("Reconnecting to SFTP server...");
        await sftp.connect(sftpConfig);
    }
    const s3Stream = s3.getObject(params).createReadStream();
    
    try {
        await sftp.put(s3Stream, sftpConfig.dir + "/" + outputKey);
        console.log(`File transfer completed for ${outputKey}`);
    } catch (error) {
        console.error(`Failed to transfer file: ${outputKey}`, error);
        throw error;
    } finally {
        s3Stream.destroy();
    }
}

const start = async function (inputBucket, inputDir, outputBucket, outputKey, format, sftpConfig, instance, context, callback) {
    if (!inputBucket || !outputBucket) {
        throw new Error("Missing bucket name");
    }

    console.log(
        `inputBucket: ${inputBucket}, outputBucket: ${outputBucket}, inputDir: ${inputDir}, outputKey: ${outputKey}, format : ${format}`
    );

    const outputFileName = outputKey + "." + format

    let files;
    if (inputBucket) {
        files = await listObjects(inputBucket, inputDir);
    }
    if (!files || files.length === 0) {
        throw new Error("Missing files, or inputDir is empty");
    }

    console.log(`input files size: ${files.length}`);

    const batches = createBatches(files, parseInt(BATCH_SIZE, 10));
    console.log("number of batches", batches.length)
    
    if (ENABLE_FILE_TRANSFER) {
        await sftp.connect(sftpConfig).then((connection) => {
            console.log("SFTP connection successful")
        })
        await sleep(1000)
    }
    try {
        await Promise.all(batches.map((batch, i) => {
            return uploadBatch(batch, i, batches.length, i === batches.length - 1, inputBucket, outputBucket, inputDir, outputKey, format, instance);
        }));
    } catch (error) {
        console.error("Error during batch upload:", error);
        throw error;
    }
    
    await sleep(1000)
    await cleanupAndStopProcess();

    return {
        statusCode: 200,
        body: JSON.stringify({outputFileName}),
    };
}

const createBatches = (files, batchSize) => {
    const batches = [];
    for (let i = 0; i < files.length; i += batchSize) {
        batches.push(files.slice(i, i + batchSize));
    }
    return batches;
}

const uploadBatch = async (files, batchIndex,  totalNumberOfBatches, lastBatch, inputBucket, outputBucket, inputDir, outputKey, format, instance) => {
    const batchNumber = INSTANCE * totalNumberOfBatches + batchIndex
    let billRunStr = ''
    let billRunID;
    if (instance === 9 && lastBatch) {
        billRunID = inputDir.split(/(?<=\d)(?=_)/)[0];
        billRunStr = "_FIN0" + billRunID;
    }

    let rawBatchFileName = `${outputKey}${batchNumber}${billRunStr}`;
    const batchFileName = `${rawBatchFileName}.${format}`;
    const streamPassThrough = new stream.PassThrough();

    const uploadParams = {
        Body: streamPassThrough,
        ContentType: "application/zip",
        Key: batchFileName,
        Bucket: outputBucket,
    };

    const s3Upload = s3.upload(uploadParams);

    const s3FileDownloadStreams = files.map((file) => {
        return {
            stream: new lazystream.Readable(() => {
                return s3
                    .getObject({Bucket: inputBucket, Key: file.key})
                    .createReadStream();
            }),
            fileName: file.fileName,
        };
    });

    const archive = archiver(format, {
        zlib: {level: zlib.Z_BEST_COMPRESSION},
    });
    archive.on("error", (error) => {
        throw new Error(
            `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
        );
    });

    archive.on("progress", (progress) => {
        let batchSize = parseInt(BATCH_SIZE, 10);
        let logKey = batchSize
        if (batchSize > 100) {
            logKey = 100
        }
        if (progress.entries.processed % logKey === 0) {
            console.log(
                `archive ${batchFileName} progress: ${progress.entries.processed} / ${progress.entries.total}`
            );
        }
    });

    s3Upload.on("httpUploadProgress", (progress) => {
        if (progress.loaded % (1024 * 1024) === 0) {
            console.log(`upload ${outputKey}, loaded size: ${progress.loaded}`);
            console.log(
                `memory usage: ${process.memoryUsage().heapUsed / 1024 / 1024} MB`
            );
        }
    });
    const s3UploadPromise = s3Upload.promise();
    await new Promise(async (resolve, reject) => {
        streamPassThrough.on("close", () => onEvent("close", resolve));
        streamPassThrough.on("end", () => onEvent("end", resolve));
        streamPassThrough.on("error", () => onEvent("error", reject));

        archive.pipe(streamPassThrough);
        s3FileDownloadStreams.forEach((ins) => {
            if (batchFileName === ins.fileName || ins.fileName === (inputDir + "/") || ins.fileName === "/") {
                console.warn(`skipping file: ${ins.fileName}`);
                // skip the output file, may be duplicating zip files
                return;
            }
            archive.append(ins.stream, {name: ins.fileName});
        });

        // await sleep(10000)
        await archive.finalize();
        // await sleep(10000)
    }).catch((error) => {
        throw new Error(`${error.code} ${error.message} ${error.data}`);
    });
    s3FileDownloadStreams.forEach((ins) => {
        ins.stream.end()
    })
    await s3UploadPromise;
    streamPassThrough.end()

    let finalTarFileName = `${rawBatchFileName}.tar`;
    await s3.copyObject({
        Bucket: outputBucket,
        CopySource: `${outputBucket}/${batchFileName}`,
        Key: finalTarFileName
    }).promise()
        .then(() => {
            s3.deleteObject({
                Bucket: outputBucket,
                Key: batchFileName
            }).promise()
                .then(()=> {
                })
                .catch((e) => {
                    console.error(e)
                })
        })
    // await sleep(20000)

    await gzipAndUpload(outputBucket, finalTarFileName, outputBucket);
    
    const params = {Bucket: OUTPUT_BUCKET, Key: `${finalTarFileName}.gz`};
    if (ENABLE_FILE_TRANSFER) {
        await transfer(sftpConfig, params, `${finalTarFileName}.gz`)
    }
}

const gzipAndUpload = async (bucket, key, outputBucket) => {
    const gzipKey = `${key}.gz`;
    const readStream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();
    const gzipStream = zlib.createGzip();
    const writeStream = new stream.PassThrough();

    const uploadParams = {
        Body: writeStream,
        ContentType: "application/gzip",
        Key: gzipKey,
        Bucket: outputBucket,
    };

    try {
        const s3Upload = s3.upload(uploadParams);
        readStream.pipe(gzipStream).pipe(writeStream);

        // Wait for the upload to complete
        await s3Upload.promise();

        console.log(`Gzipped file uploaded: ${gzipKey}`);

        // Delete original file
        await s3.deleteObject({Bucket: bucket, Key: key}).promise();

        console.log(`Original file deleted: ${key}`);
    } catch (error) {
        console.error("Gzip and upload error:", error);
        throw error;
    } finally {
        // Close and cleanup streams
        await readStream.destroy();
        await gzipStream.destroy();
        await writeStream.end();
    }

    await sleep(10000)
};

const listObjects = async (bucket, prefix) => {
    let params = {
        Bucket: bucket,
        Prefix: prefix,
    };
    const result = [];
    while (true) {
        let data = await s3.listObjectsV2(params).promise();
        result.push(...getContents(data, prefix));
        if (!data.IsTruncated) {
            break;
        }
        params.ContinuationToken = data.NextContinuationToken;
    }
    return result;
};

const getContents = (data, prefix) => {
    return data.Contents.map((item) => {
        const fileName = item.Key.replace(prefix, "");
        return {key: item.Key, fileName};
    });
};

const onEvent = (event, reject) => {
    console.log(`on: ${event}`);
    reject();
};

const cleanupAndStopProcess = async () => {
    // Close SFTP connection if open
    if (ENABLE_FILE_TRANSFER && sftp.sftp) {
        await sftp.end();
    }
    console.log("Cleanup completed. Stopping process.");
    process.exit(0); // Exit the Node.js process with success code
}

const sftpConfig = {
    host: process.env.SFTP_PORT || "192.168.1.2",
    port: parseInt(process.env.SFTP_PORT || "22", 10),
    user: process.env.SFTP_USER || "tester",
    password: process.env.SFTP_PASSWORD || "password",
    dir: process.env.SFTP_DIR || "/s3",
}

const INPUT_BUCKET = process.env.INPUT_BUCKET
const INPUT_DIRECTORY = process.env.INPUT_DIRECTORY
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET
const OUTPUT_FILE_NAME = process.env.OUTPUT_FILE_NAME
const INSTANCE = parseInt(process.env.INSTANCE, 10)
const BATCH_SIZE = process.env.BATCH_SIZE || 2;

const ENABLE_FILE_TRANSFER = process.env.ENABLE_FILE_TRANSFER || false;

start(INPUT_BUCKET, INPUT_DIRECTORY, OUTPUT_BUCKET, OUTPUT_FILE_NAME, "zip", sftpConfig, INSTANCE).then(async () => {
});
