var routes     = require('./routes');

var express = require('express');
var app     = express();

app.listen(process.env.PORT || 4730);

// Listen for any options request
app.options("*", function (req, res) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  // Finish preflight request.
  res.writeHead(204);
  res.end();
});

app.get('/v0/measurements/:stationId',   routes.measurements  );
app.get('/v0/stations',         routes.stations );
app.get('/v0/last/:limit',      routes.last );

process.on('uncaughtException', function (error) {
  console.log(error.stack);
});
