var request = require('request');
var pg = require('pg');
var url = require('url');

var conString = "postgres://airq:nach0s@localhost:3333/airquality";

var client = new pg.Client(conString);

client.connect(function(err){
  if(err) {
    return console.error('error fetching client from pool', err);
  }
});

//get a list of measurements, needs a specific station ID
exports.measurements = function(req, res) {
  var station = req.params.stationId;
  var querySpec = url.parse(req.url, true).query;
  var limit = querySpec.limit === undefined ? 1000 : querySpec.limit;

  client.query('SELECT stationid, system, datetime, definition,' +
    'value FROM measurements,params WHERE param=paramID AND stationid=TEXT($1::TEXT) ORDER BY datetime DESC LIMIT $2::INT;', [station, limit], function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }
    res.json(result.rows);
  });
};

//get the last x amount of measurements from all stations
exports.last = function(req, res) {
  var limit = req.params.limit;
  client.query('SELECT stationid, system, datetime, definition,' +
    'value FROM measurements,params WHERE param=paramID ORDER BY datetime DESC LIMIT $1::INT;', [limit], function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }
    res.json(result.rows);
  });
};

//get a list of all stations
exports.stationList = function(req, res) {
  client.query('SELECT stationid, system, name, lat, lon, elev, pt FROM stations;', function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }
    res.json(result.rows);
  });
};

//get a list of stations close to a specific longitude and latitude - with a limit
exports.stationNear = function(req, res) {
  var querySpec = url.parse(req.url, true).query;
  var limit = querySpec.limit === undefined ? 10 : querySpec.limit;

  client.query("SELECT * FROM stations ORDER BY pt <-> POINT("+req.params.lon+','+req.params.lat+") LIMIT $1::INT;", [limit], function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }

    res.json(result.rows);
  });
};