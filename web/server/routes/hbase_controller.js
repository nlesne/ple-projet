const hbase = require('hbase');
const config = require('../config/server.config');

function getRowsFromTable(tableName) {
    const client = hbase(config.db_options);
    client
    .table('dauriac-lesne-' + tableName)
    .scan({}, (err, rows) => {
        if (err) {
            res.status(500);
            res.send({err});
        }
        else {
            const parsedRows = rows.map(row => {
                console.log(row);
                let parsedRow = {};
                parsedRow[wrow.row] = row.$;
                return parsedRow;
            });
            res.status(200);
            res.send({result: parsedRows});
        }
    });
}

module.exports = getRowsFromTable;