const hbase = require('hbase');
const config = require('../config/server.config');

function getRowsFromTable(req, res) {
    const client = hbase(config.db_options);
    const paddedDay = req.day.toLocaleString('fr-FR', { minimumIntegerDigits: 2, useGrouping: false });
    const rowFilterValue = paddedDay + "_03.+";
    client
        .table('dauriac-lesne-' + req.tableName)
        .scan({
            filter: {
                "op": "EQUAL",
                "type": "RowFilter",
                "comparator": { "value": rowFilterValue, "type": "RegexStringComparator" }
            },
            maxVersions: 1
        }, (err, rows) => {
            if (err) {
                res.status(500);
                res.send({ err });
            }
            else {
                const parsedRows = rows.map(row => {
                    let parsedRow = {};
                    const realKey = row.key.split('-')[1];
                    parsedRow[realKey] = row.$;
                    return parsedRow;
                });
                res.status(200);
                res.send({ result: parsedRows });
            }
        });
}

exports.getRowsFromTable = getRowsFromTable;