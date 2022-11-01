const net = require('net');
const stream = require('stream');
const pipeline = stream.pipeline;
const zlib = require('zlib');
const fs = require('fs');

let input;
let current_file = new stream.PassThrough;
let transfer_complete = false;
let state = 0;
let buffer = Buffer.alloc(4);
let buffer_index = 0;
let size = 0;
let total_bytes = 0;
let length = 0;

async function run_server() {
    const server = net.createServer(async (client) => {
        try {
            await start_client(client);
        } catch (error) {
            console.error("create server error", error);
            client.destroy(new Error(`Destroying socket due to unexpected start_client error ${error}`));
        }
    });

    server.on('error', err => {});
    let port = 10111;
    console.error("listening on port",port);
    server.listen(port);
}

/**
 * Pipe data to gzip
**/
async function pipeData(file_stream){
    const gzip = zlib.createGzip({level: 0});
    pipeline(file_stream, gzip, err => {});
    const pipeline_writable = new stream.Writable({
        write: function(chunk, encoding, next) {
          next();
        }
    });
    pipeline(gzip, pipeline_writable, err => {});
}


async function start_client(client) {
    input = client;
    client.on('data', (data) => {
        total_bytes += data.length;
        console.log(`parse_chunk, length ${data.length}, total bytes received ${total_bytes}`);
        while (data.length > 0) {
            data = parse(data);
        }
    });

    const writable = new stream.Writable({
        write: function(chunk, encoding, next) {
            next();
        }
    });
    pipeline(client, writable, err => {});
}

/**
 * We are expecting certain data in a certain order, and only some of it is piped to gzip in order
 * to reproduce this bug
**/
function parse(data) {
    if (state === 0) {
        return parse_transfer_complete(data);
    } else if (state === 17) {
        // Read the expected size of the upcoming data
        return parse_size(data);
    } else if (state === 18) {
        // Read the amount of data which we expect from the previous step, and pipe it to gzip
        return parse_file(data);
    } else if (state % 2 === 1) {
        // Read the expected size of the upcoming data
        return parse_uint8(data, state + 1);
    } else if (state % 2 === 0) {
        // Slice off data according to the size we just read, and do nothing with it (this is not getting piped to gzip)
        return parse_string(data, state + 1);
    }
    throw new Error("Unknown message parsing state " + state);
}

/**
 * Read the first byte in the buffer, and return the buffer with it removed
**/
function parse_uint8(data, next) {
    state = next;
    length = data.readUInt8(0);
    return data.slice(1);
}

/**
 * Read a predetermined number of bytes from the buffer, and remove them from the buffer
**/
function parse_string(data, next) {
    let data_size = usable_size(data,length);
    buffer_index += data_size;
    let value = data.slice(0, data_size).toString('utf8');
    if (buffer_index === length) {
        buffer_index = 0;
        state = next;
    }
    return data.slice(data_size);
}

/**
 * Remove the byte from the buffer which indicates that the transfer is complete, and reset the state
**/
function parse_transfer_complete(data) {
    transfer_complete = !!data.readUInt8(0);
    if (transfer_complete) {
        return data.slice(1);
    }
    state = 1;
    return data.slice(1);
}

/**
 * Read the expected size of the upcoming data
**/
function parse_size(data) {
    let data_size = usable_size(data, 4);
    data.copy(buffer, buffer_index, 0, data_size);
    buffer_index += data_size;
    if (buffer_index === 4) {
        size = buffer.readUInt32BE(0);
        buffer_index = 0;
        state = 18;
    }
    return data.slice(data_size);
}

/**
 * Read a predetermined amount of data from the buffer and pipe it to gzip
**/
function parse_file(data) {
    if (buffer_index === 0) {
        current_file = new stream.PassThrough();
        current_file.on('drain', () => {
            input.resume();
        });
        pipeData(current_file);
    }
    let data_size = usable_size(data, size);
    buffer_index += data_size;
    current_file.write(data.slice(0, data_size));
    if (buffer_index === size) {
        if (current_file) current_file.end();
        buffer_index = 0;
        state = 0;
    }
    return data.slice(data_size);
}

/**
 * This function is used to ensure we don't exceed the length of the buffer when we read it (since
 * the data is being sent in chunks)
**/
function usable_size(data, required_size) {
    return Math.min(data.length, required_size - buffer_index);
}

/**
 * Read anonymized data from a file and send it in random sized chunks over a socket
**/
function readAndSendData() {
    const client = net.createConnection({ port: 10111 }, () => {
        let buf = fs.readFileSync('anonymized-data');
        function sendBytes() {
            if (buf.length > 0) {
                setTimeout(() => {
                    let bytes = Math.floor(Math.random() * 5000);
                    console.log(`sending ${bytes} bytes, buf size is ${buf.length}`);
                    if (bytes > buf.length) bytes = buf.length;
                    client.write(buf.slice(0, bytes));
                    buf = buf.slice(bytes);
                    sendBytes();
                });
            } else {
                console.log('program completed successfully');
                process.exit(0);
            }
        }
        sendBytes();
    });
}

async function main() {
    run_server();
    readAndSendData();
}

main();
