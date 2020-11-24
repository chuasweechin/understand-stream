const config = require('./config.js')

const pg = require('pg');
const fastcsv = require('fast-csv');
const express = require('express');
var stringify = require('csv-stringify');
const QueryStream = require('pg-query-stream');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const csvWriter = require("csv-write-stream");

const app = express();
const pool = new pg.Pool(config.DBCONFIG);

var streamDownloadHandler = async function (request, response) {
    response.status(200)
    response.setHeader('Content-disposition', 'attachment; filename=employees_db_stream.csv')
    response.setHeader('Content-type', 'text/csv')

    var writer = csvWriter();

    pool.connect(async (err, client) => {
        if (err) console.log("pool.connect()", err.stack)

        const query = new QueryStream(`SELECT * FROM "Employees" LIMIT 1000000`);

        var stream = client.query(query);
        stream.on('end', () => client.end());

        writer.pipe(response)
        stream.pipe(writer)
    })
}

var bufferDownloadHandler = function (request, response) {
    response.status(200)
    response.setHeader('Content-disposition', 'attachment; filename=employees_memory_buffer.csv')
    response.setHeader('Content-type', 'text/csv')

    pool.query(`SELECT * FROM "Employees" LIMIT 1000000`, (err, result) => {
        if (err) console.log("client.query()", err.stack);

        if (result) {
            // Method #1
            fastcsv
                .write(result.rows, { headers: true })
                .pipe(response)
                .on('end', () => console.log(`Stream complete.`));

            // Method #2
            // stringify(result.rows, { header: true }, (err, output) => 
            //     response.send(output)
            // )

            // Method #3
            // const csvStringifier = createCsvStringifier({
            //     header: [
            //         {id: 'Id', title: 'Id'},
            //         {id: 'Name', title: 'Name'},
            //         {id: 'Department', title: 'Department'},
            //         {id: 'Email', title: 'Email'}
            //     ]
            // });
            // response.send(csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(result.rows))
        }
    })
}

app.get('/stream', streamDownloadHandler);
app.get('/buffer', bufferDownloadHandler);

app.listen(3000);