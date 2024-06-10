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

function sftpTransfer(sftpConfig, params, outputKey) {
    return sftp.connect(sftpConfig).then(async () => {
        console.log("Connected to SFTP")

        const s3Stream = s3.getObject(params).createReadStream();

        await sftp.put(s3Stream, sftpConfig.dir + "/" + outputKey);
        await sftp.end();
    });
}

const start = async function (inputBucket, inputDir, outputBucket, outputKey, sftpConfig, context, callback) {
    if (!inputBucket || !outputBucket) {
        throw new Error("Missing bucket name");
    }

    console.log(
        `inputBucket: ${inputBucket}, outputBucket: ${outputBucket}, inputDir: ${inputDir}, outputKey: ${outputKey}`
    );

    let files;
    if (inputBucket) {
        files = await listObjects(inputBucket, inputDir);
    }
    if (!files || files.length === 0) {
        throw new Error("Missing files, or inputDir is empty");
    }

    console.log(`input files size: ${files.length}`);

    const streamPassThrough = new stream.PassThrough();

    const uploadParams = {
        Body: streamPassThrough,
        ContentType: "application/zip",
        Key: outputKey,
        Bucket: outputBucket,
    };

    const s3Upload = s3.upload(uploadParams, (err) => {
        if (err) {
            console.error("upload error", err);
        } else {
            console.log("upload done");
        }
    });

// get all input files streams
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

    const archive = archiver("zip", {
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
                `archive ${outputKey} progress: ${progress.entries.processed} / ${progress.entries.total}`
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

    await new Promise((resolve, reject) => {
        streamPassThrough.on("close", () => onEvent("close", resolve));
        streamPassThrough.on("end", () => onEvent("end", resolve));
        streamPassThrough.on("error", () => onEvent("error", reject));

        console.log("Starting upload");

        archive.pipe(streamPassThrough);
        s3FileDownloadStreams.forEach((ins) => {
            if (outputKey === ins.fileName || ins.fileName === (inputDir + "/") || ins.fileName === "/") {
                console.warn(`skip output file: ${ins.fileName}`);
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

    const params = {Bucket: outputBucket, Key: outputKey};
    await sftpTransfer(sftpConfig, params, outputKey)
    return {
        statusCode: 200,
        body: JSON.stringify({outputKey}),
    };
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

const INPUT_BUCKET = process.env.INPUT_BUCKET
const INPUT_DIRECTORY = process.env.INPUT_DIRECTORY
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET
const OUTPUT_FILE_NAME = process.env.OUTPUT_FILE_NAME

start(INPUT_BUCKET, INPUT_DIRECTORY, OUTPUT_BUCKET, OUTPUT_FILE_NAME, sftpConfig)
