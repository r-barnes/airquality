var elasticsearch   = require('elasticsearch');
var esClient = new elasticsearch.Client({
  apiVersion: '0.90' //,log: 'trace'
});

var mappings={};

var redis       = require("redis");
var redisclient = redis.createClient();

var csv       = require("fast-csv");
var ftpclient = require('jsftp');

mappings.measurement={
  "properties": {
    "datetime": {
      "type": "long"
    },
    "station_id": {
      "type": "string"
    },
    "name": {
      "type": "string"
    },
    "parameter": {
      "type": "string"
    },
    "value": {
      "type": "double"
    },
    "source": {
      "type": "string"
    }
  }
};

mappings.station={
  "properties": {
    "location": {
      "type": "geo_point"
    },
    "station_id": {
      "type": "string"
    },
    "name": {
      "type": "string"
    },
    "agency": {
      "type": "string"
    },
    "elevation": {
      "type": "double"
    }
  }
};


/*
esClient.indices.create({
  index: 'measurements',
  body: {
    "mappings": {
      "stop": mappings.stop
    }
  }
});*/

esClient.indices.create({
  index: 'aqs_stations',
  body: {
    "mappings": {
      "station": mappings.station
    }
  }
});

var ftp = new ftpclient({
  host: "ftp.airnowapi.org",
  port: 21,
  user: "leapingleopard", 
  pass: "xi7MxOiwrRR_vKeT"
});

ftp.get('Locations/monitoring_site_locations.dat', function(err,socket){
  if(err) return;

  var str = "";
  socket.on("data", function(d) { str += d.toString(); });
  socket.on("close", function(hadErr) {
    if (hadErr){
      console.error('There was an error retrieving the file.');
      return;
    }

    var sites = {};

    var headers = ["aqsid",  "parameter_name", "site_code", "site_name", "status",  "agency_id",  "agency_name",  "epa_region", "latitude",  "longitude",  "elevation",  "gmt_offset", "country_code",  "cmsa_code",  "cmsa_name",  "msa_code", "msa_name",  "state_code", "state_name",  "county_code",  "county_name",  "city_code",  "city_name"];

    var good_sites = 0;
    var bad_sites  = 0;

    csv.fromString(str, {headers: headers, delimiter: '|'})
      .on("record", function(data){

        if(data.status!='Active')
          return;

        if(typeof(sites[data.aqsid])!=='undefined')
          return;

        data.longitude = parseFloat(data.longitude);
        data.latitude  = parseFloat(data.latitude);
        data.elevation = parseFloat(data.elevation);
        if(data.longitude>180 || data.latitude>90){
          console.error('Bad location', {aqsid:data.aqsid, name:data.site_name,lat:data.latitude,lon:data.longitude});
          bad_sites += 1;
          return;
        }

        var site = {
          id:        data.aqsid,
          name:      data.site_name,
          location:  [data.longitude,data.latitude],
          agency:    data.agency_name,
          elevation: data.elevation
        };

        sites[data.aqsid]=site;

        redisclient.set('aqs_station-'+data.aqsid, JSON.stringify(site));
        good_sites += 1;
      }).on("end", function(){
        console.log('Good',good_sites,'Bad',bad_sites);
      });
  }); 

  socket.resume();

});

process.on('uncaughtException', function (error) {
  console.log(error.stack);
});

//process.exit(code=0)