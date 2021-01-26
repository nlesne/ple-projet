const router = require('express-promise-router')();
const controller = require('./hbase_controller');

router.get('/count', controller('tweetCount'));
router.get('/country', controller('tweetsByCountry'));
router.get('/lang', controller('tweetsByLang'));

module.exports = router;