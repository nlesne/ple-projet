const router = require('express-promise-router')();
const controller = require('./hbase_controller');

router.get('/topk-day', (req, res, next) => {
    req.tableName = 'topKHashtags';
    req.day = req.query.day;
    next();
}, controller.getDataByDay);
router.get('/topk', (req, res, next) => {
    req.tableName = 'topKHashtags';
    next();
}, controller.getDataByDay)
router.get('/count', (req, res, next) => {
    req.tableName = 'countHashtags';
    req.day = req.query.day;
    req.rowKey = req.query.hashtag;
    next();
}, controller.getValueFromRow);
router.get('/users', (req, res, next) => {
    req.tableName = 'hashtagUsers';
    req.day = req.query.day;
    req.rowKey = req.query.hashtag;
    next();
}, controller.getDataFromRow);

module.exports = router;
