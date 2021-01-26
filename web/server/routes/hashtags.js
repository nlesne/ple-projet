const router = require('express-promise-router')();
const controller = require('./hbase_controller');

router.get('/topk', controller('topKHashtags'));
router.get('/topk-day', (req, res) => {
    const paddedDay = req.params.day.toLocalString('fr-FR', {minimumIntegerDigits: 2, useGrouping: false});
    return controller('topKHashtags-' + paddedDay + '-03');
});
router.get('/hashtagCount', (req, res) => {
    const client = hbase(config.db_options);
    const hashtag = req.params.hashtag;
    client
    .table('dauriac-lesne-hashtagCount')
    .row(hashtag)
    .get('count', (err, value) => {
        if (err) {
            res.status(500);
            res.send({err});
        }
        else {
            res.status(200);
            res.send({result: {hashtag: value}});
        }
    });
});

router.get('/hashtagUsers', controller('hashtagUsers'));

module.exports = router;