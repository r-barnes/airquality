var request       = require('request');
var pg = require('pg');

var conString = "postgres://airq:nach0s@localhost:3333/airquality";

var client = new pg.Client(conString);

client.connect(function(err){
  if(err) {
    return console.error('error fetching client from pool', err);
  }
});


/*
client.query('SELECT $1::int AS number', ['1'], function(err, result) {

    if(err) {
      return console.error('error running query', err);
    }
    console.log(result.rows[0].number);
    //output: 1
});*/

exports.station = function(req, res) {
  var station = req.params.station;
  var limit = req.params.limit;
};

exports.last = function(req, res) {
  var limit = req.params.limit;

  client.query('SELECT stationid, system, datetime, definition,' +
    'value FROM measurements,params WHERE param=paramID  limit $1::INT', [limit], function(err, result) {

    if(err) {
      return console.error('error running query', err);
    }
    console.log(result);
  });

};

exports.stations = function(req, res) {
  // Add CORS headers
  res.header('Access-Control-Allow-Origin',  '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  client.query("SELECT * FROM stations ORDER BY pt <-> POINT("+req.params.lon+','+req.params.lat+") limit 10;", [], function(err, result) {
    if(err) {
      return console.error('error running query', err);
    }

    res.json(result.rows);
  });
};