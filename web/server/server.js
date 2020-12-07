const express = require('express');
const bodyParser = require('body-parser');
const config = require('./config/server.config.js');

const app = express();
app.use(bodyParser.json());



const port = config.api_port;
app.listen(port, () => {
  console.log(`server listening on ${port}`);
});
