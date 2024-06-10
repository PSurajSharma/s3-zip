require('aws-sdk/lib/maintenance_mode_message').suppress = true;

const Client = require('ssh2-sftp-client');
const sftp = new Client();

const AWS = require("aws-sdk");
const stream = require("stream");
const archiver = require("archiver");
const https = require("https");
const lazystream = require("lazystream");
const {app} = require("serverless/lib/cli/commands-schema/common-options/aws-service");
const path = require('path');
const agent = new https.Agent({keepAlive: true, maxSockets: 16});

AWS.config.update({httpOptions: {agent}, region: "eu-south-2"});

const s3 = new AWS.S3();

const transfer = async function sftpTransfer(sftpConfig, params, outputKey) {
    console.log("Doing transfer")
    // return sftp.connect(sftpConfig).then(async () => {
    //     console.log("Connected to SFTP")
    //
    //     const s3Stream = s3.getObject(params).createReadStream();
    //
    //     await sftp.put(s3Stream, sftpConfig.dir + "/" + outputKey);
    //     await sftp.end();
    // });
}

const start = async function (inputBucket, inputDir, outputBucket, outputKey, format, context, callback) {
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

    const batches = createBatches(files, BATCH_SIZE);
    console.log("number of batches",batches.length)
    for (let i = 0; i < batches.length; i++) {
        await uploadBatch(batches[i], i, inputBucket, outputBucket, inputDir, outputKey, format);
    }

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

const uploadBatch = async (files, batchIndex, inputBucket, outputBucket, inputDir, outputKey, format) => {
    const batchFileName = `${outputKey}_${batchIndex}.${format}`;
    const streamPassThrough = new stream.PassThrough();

    const uploadParams = {
        Body: streamPassThrough,
        ContentType: "application/zip",
        Key: batchFileName,
        Bucket: outputBucket,
    };

    const s3Upload = s3.upload(uploadParams, (err) => {
        if (err) {
            console.error("upload error", err);
        } else {
            console.log("upload done");
        }
    });

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
        zlib: {level: 0},
    });
    archive.on("error", (error) => {
        throw new Error(
            `${error.name} ${error.code} ${error.message} ${error.path}  ${error.stack}`
        );
    });

    archive.on("progress", (progress) => {
        if (progress.entries.processed % 10 === 0) {
            console.log(
                `archive ${batchFileName} progress: ${progress.entries.processed} / ${progress.entries.total}`
            );
        }
    });

    s3Upload.on("httpUploadProgress", (progress) => {
        if (progress.loaded % (1024 * 1024) === 0) {
            console.log(`upload ${batchFileName}, loaded size: ${progress.loaded}`);
            console.log(
                `memory usage: ${process.memoryUsage().heapUsed / 1024 / 1024} MB`
            );
        }
    });

    await new Promise((resolve, reject) => {
        streamPassThrough.on("close", () => onEvent("close", resolve));
        streamPassThrough.on("end", () => onEvent("end", resolve));
        streamPassThrough.on("error", () => onEvent("error", reject));

        console.log("Starting upload");

        archive.pipe(streamPassThrough);
        s3FileDownloadStreams.forEach((ins) => {
            if (batchFileName === ins.fileName || ins.fileName === (inputDir + "/") || ins.fileName === "/") {
                console.warn(`skipping file: ${ins.fileName}`);
                // skip the output file, may be duplicating zip files
                return;
            }
            archive.append(ins.stream, {name: ins.fileName});
        });
        archive.finalize();
    }).catch((error) => {
        throw new Error(`${error.code} ${error.message} ${error.data}`);
    });
    console.log("Upload done");
    await s3Upload.promise();
}

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

const sftpConfig = {
    host: process.env.SFTP_HOST,
    port: process.env.SFTP_PORT,
    user: process.env.SFTP_USER,
    password: process.env.SFTP_PASSWORD,
    dir: process.env.SFTP_DIR,
}

const INPUT_BUCKET = 'inputbuckettesting'
const INPUT_DIRECTORY = 'inputFolder'
const OUTPUT_BUCKET = 'outputbuckettesting'
const OUTPUT_FILE_NAME = 'test'
const OUTPUT_FORMAT = 'zip'
const BATCH_SIZE = process.env.BATCH_SIZE || 2;


start(INPUT_BUCKET, INPUT_DIRECTORY, OUTPUT_BUCKET, OUTPUT_FILE_NAME, OUTPUT_FORMAT, sftpConfig).then(async () => {
    const params = {Bucket: OUTPUT_BUCKET, Key: OUTPUT_FILE_NAME};
    transfer(sftpConfig, params, OUTPUT_FILE_NAME)
});