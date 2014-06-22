var request = require('request');
var pg      = require('pg');
var url     = require('url');
var Q       = require('q');

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

  var jsonOut = {};

  jsonOut.stationInfo = {};
  jsonOut.measurements = [];

  client.query('SELECT stationid, system, datetime, definition,' +
    'value FROM measurements,params WHERE param=paramID AND stationid=TEXT($1::TEXT) ORDER BY datetime DESC LIMIT $2::INT;', [station, limit], function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }

    jsonOut.measurements = result.rows;

    client.query('SELECT stationid, system, name, lat, lon, elev, pt FROM stations WHERE stationid=TEXT($1::TEXT);', [station], function(err2, result2) {
      if(err2) {
        res.json(err2);
        return console.error('error running query', err2);
      }

      jsonOut.stationInfo = result2.rows[0];
      res.json(jsonOut);
    });
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

function StationNear(lat, lon, limit){
  var deferred = Q.defer();
  client.query("SELECT * FROM stations ORDER BY pt <-> POINT("+lon+','+lat+") LIMIT $1::INT;", [limit], function(err, result) {
    if(err) {
      res.json(err);
      return console.error('error running query', err);
    }

    deferred.resolve(result.rows);
  });

  return deferred.promise;
}

function xmlResponse(res, message) {
  
  xml = '<?xml version="1.0" encoding="UTF-8"?><Response>' +
        '<Message>' + message + '</Message></Response>';

  res.writeHead(200, {'Content-Type': 'text/html' });
  console.log('Response: ', xml);
  res.end(xml); 
}

function getMessage(res, stationid) {
  console.log('Start LastMeas');
  LastMeas(stationid).then(function(last_measurement) {
    // console.log('End LastMeas');
    // console.log('last_measurement = ', last_measurement);
    var answer = JSON.stringify(last_measurement);
    console.log('answer = ', answer);
    xmlResponse(res, JSON.stringify(last_measurement));
  });
}

//get a list of stations close to a specific longitude and latitude - with a limit
exports.stationNear = function(req, res) {
  var querySpec = url.parse(req.url, true).query;
  var limit = querySpec.limit === undefined ? 10 : querySpec.limit;

  StationNear(req.params.lat, req.params.lon, limit).then(function(result){
    res.json(result);
  });
};

//send an SMS
exports.sms = function(req, res) {
  console.log('Received ' + req.method + ' request');
  if (req.method == 'POST') {
    var from = req.body.From;
    var to = req.body.To;
    var zipcode = req.body.Body;
    var media_url = req.body.MediaUrl;
    var lat;
    var lon;
    var message;  
    if (zipcode.match(/^\d\d\d\d\d$/)) {
      client.query('SELECT lat, lon FROM zips where zip = $1 LIMIT 1;', [zipcode],  function(err, result) {
        if(err) {
          res.json(err);
          return console.error('error running query', err);
        }
        console.log('result.rows is ', result.rows);
        if (result.rows.length > 0) {
          lat = result.rows[0].lat;
          lon = result.rows[0].lon;
          console.log('lat ' + lat + ' lon ' + lon);
          StationNear(lat, lon  , 1).then(function(nearest_station){
            nearest_station = nearest_station[0];
            if (typeof(nearest_station) !== 'undefined') {
              stationid = nearest_station.stationid;
              console.log('stationid is', stationid);
              getMessage(res, stationid);  
            } else {
              xmlResponse(res, "Nearest station not found.");
            }
          });
        } else {
          console.log("zip code ", zipcode, " not found");
        }   
      });
    } else {
      console.log(zipcode, " is not a valid zip code");
    }
  }
}

