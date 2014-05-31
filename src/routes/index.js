var elasticsearch = require('elasticsearch');
var redis         = require("redis");
var request       = require('request');

var redisclient = redis.createClient();

var esClient = new elasticsearch.Client({
  apiVersion: '0.90' //,log: 'trace'
});


exports.station = function(req, res) {
  var station = req.params.station;

  redisclient.get('aqs-'+station,
    function(err, reply) {
      if(err){
        console.error("Error fetching station '"+station+"': "+err);
        deferred.resolve(false);
        return;
      } else if (reply) {
        deferred.resolve(JSON.parse(reply));
        return;
      } else {
        self._fetchRealtime().then(function(result){
          redisclient.set   (self.rediskey, JSON.stringify(result));
          redisclient.expire(self.rediskey, 50); //TODO(Richard): Make sure this is set to a value specific for the transit provider
          deferred.resolve( result );
        });
      }
    }
  );
};

exports.stations = function(req, res) {
  // Add CORS headers
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  var filter = {
    'geo_distance': {
      //'distance': '10mi',
      'location': [req.query.lon, req.query.lat]
    }
  };
  var query = {
    match_all : {}
  };

  esClient.search({
    index: 'aqs_stations',
    body: {
      query: query,
      filter: filter,
      sort: [
        {
          '_geo_distance': {
            'location': [req.query.lon, req.query.lat],
            'unit': 'mi'
          }
        }
      ]
    }
  }).then(function(resp) {
    // We got a good response, let's format it for api use.
    console.log(resp);
    res.json( "hi" );
  }, function(err) {
    // There was an error. Return unprocessable 401.  Will want to revist this.
    res.writeHead(401);
    res.end();
  });
};