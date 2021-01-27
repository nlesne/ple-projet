const express = require('express');
const bodyParser = require('body-parser');
const config = require('./config/server.config.js');
const hbase = require('hbase');

const app = express();
app.use(bodyParser.json());
app.use(cors({origin: [serverConfig.client_url]}));

app.use('/api/users', require('./routes/users'));
app.use('/api/hashtags', require('./routes/hashtags'));
//app.use('/api/influencers', require('./routes/influencers'));

const port = config.api_port;
app.listen(port, () => {
  console.log(`server listening on ${port}`);
});
