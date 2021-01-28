const router = require('express-promise-router')();
const controller = require('./hbase_controller');

router.get('/count', (req, res, next) => {
    req.tableName = 'tweetCount';
    req.day = req.query.day;
    req.rowKey = req.query.user;
    next();
}, controller.getDataFromRow);
router.get('/hashtags', (req, res, next) => {
    req.tableName = 'userHashtags';
    req.day = req.query.day;
    req.rowKey = req.query.user;
    next();
}, controller.getDataFromRow);
router.get('/country', (req, res, next) => {
    req.tableName = 'tweetsByCountry';
    req.day = req.query.day;
    next();
}, controller.getDataByDay);
router.get('/lang', (req, res, next) => {
    req.tableName = 'tweetsByLang';
    req.day = req.query.day;
    next();
}, controller.getDataByDay);

module.exports = router;
