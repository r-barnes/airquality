var routes     = require('./routes');

var express = require('express');
var app     = express();
app.use(express.bodyParser());

var allowCrossDomain = function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type,      Accept");
  next();
};

app.use(allowCrossDomain);
app.use(app.router);

app.listen(process.env.PORT || 4730);

app.use('/', express.static(__dirname + '../frontend'));

app.get('/v0/measurements/:stationId',   routes.measurements  );

app.get('/v0/stationList',         routes.stationList );
app.get('/v0/stationNear/:lat/:lon',         routes.stationNear );

app.post('/sms', routes.sms );

//app.get('/v0/station/:station', routes.station  );

app.get('/v0/last/:stationid',      routes.last );

process.on('uncaughtException', function (error) {
  console.log(error.stack);
});
