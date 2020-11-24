const config = require('./config.js')

const pg = require('pg');
const fastcsv = require('fast-csv');
const express = require('express');
var stringify = require('csv-stringify');
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;

const app = express();
const client = new pg.Pool(config.DBCONFIG);

var streamDownloadHandler = function (request, response) {
    response.status(200)
    response.setHeader('Content-disposition', 'attachment; filename=employees.csv')
    response.setHeader('Content-type', 'text/csv')

    client.query(`SELECT * FROM "Employees" LIMIT 300000`, (err, result) => {
        if (err) console.log("client.query()", err.stack)
    
        if (result) {     
            const jsonData = JSON.parse(JSON.stringify(result.rows));

            fastcsv
                .write(jsonData, { headers: true })
                .pipe(response)
                .on('end', () => console.log(`Stream complete.`));
        }
    })
}

var nonStreamDownloadHandler = function (request, response) {
    response.status(200)
    response.setHeader('Content-disposition', 'attachment; filename=employees.csv')
    response.setHeader('Content-type', 'text/csv')

    client.query(`SELECT * FROM "Employees" LIMIT 300000`, (err, result) => {
        if (err) console.log("client.query()", err.stack);

        if (result) {
            // Method 1
            // stringify(result.rows, { header: true }, (err, output) => 
            //     response.send(output)
            // )

            // Method 2
            const csvStringifier = createCsvStringifier({
                header: [
                    {id: 'Id', title: 'Id'},
                    {id: 'Name', title: 'Name'},
                    {id: 'Department', title: 'Department'},
                    {id: 'Email', title: 'Email'}
                ]
            });
            response.send(csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(result.rows))
        }
    })
}

app.get('/', streamDownloadHandler);
app.get('/none', nonStreamDownloadHandler);

app.listen(3000);