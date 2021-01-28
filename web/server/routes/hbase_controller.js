const hbase = require('hbase');
const config = require('../config/server.config');

function getDataByDay(req, res) {
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
                    const hashtag = row.key.split('-')[1];
                    parsedRow[hashtag] = row.$;
                    return parsedRow;
                });
                res.status(200);
                res.send({ result: parsedRows });
            }
        });


}

function getDataFromRow(req, res) {
    const client = hbase(config.db_options);
    const paddedDay = req.day.padStart(2, "0");
    const completeRowKey = paddedDay + "_03-" + req.rowKey;
    client
        .table('dauriac-lesne-' + req.tableName)
        .row(completeRowKey)
        .get('data', {}, (err, value) => {
            if (err) {
                res.status(500);
                res.send(err);
            }
            else {
                let rowData = [];
                rowData = value[0].$.split(' ');
                res.status(200);
                res.send({ result: rowData });
            }
        });
}

function getOneValueFromTable(req, res) {
    const client = hbase(config.db_options);
    const paddedDay = req.day.padStart(2, "0");
    const completeRowKey = paddedDay + "_03-" + req.rowKey;
    client
        .table('dauriac-lesne-' + req.tableName)
        .row(completeRowKey)
        .get('data', {}, (err, value) => {
            if (err) {
                res.status(500);
                res.send(err);
            }
            else {
                res.status(200);
                res.send({ result: value[0].$ });
            }
        });
}
// Doesn't work because of async Hbase calls
function getTopKHashtagsAll(req, res) {
    const client = hbase(config.db_options);
    let topKResult = new Map();
    for (let i = 1; i <= 31; i++) {
        i = "" + i;
        const paddedDay = i.padStart(2, "0");
        const filterValue = paddedDay + "_03.+";
        const scanOptions = {
            filter: {
                "op": "EQUAL",
                "type": "RowFilter",
                "comparator": { "value": filterValue, "type": "RegexStringComparator" }
            },
            maxVersions: 1
        };
        client.table('dauriac-lesne-' + req.tableName)
            .scan(scanOptions, (err, rows) => {
                if (err) {
                    return;
                }
                console.log(i);
                if (rows !== []) {
                    for (row of rows) {
                        const hashtag = row.key.split('-')[1];
                        const previousValue = topKResult.get(hashtag);
                        if (previousValue) {
                            topKResult.set(hashtag, previousValue + row.$);
                        }
                        else {
                            topKResult.set(hashtag, Number(row.$));
                        }
                    }
                }
            });
    }
    const topKArray = Array.from(topKResult, (name, value) => ({name, value}));
    console.log(topKArray);
    res.status(200);
    res.send({ result: topKArray });

}

exports.getTopKHashtagsAll = getTopKHashtagsAll;
exports.getValueFromRow = getOneValueFromTable;
exports.getDataByDay = getDataByDay;
exports.getDataFromRow = getDataFromRow;