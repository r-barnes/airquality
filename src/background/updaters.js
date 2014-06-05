var redis         = require("redis");
var request   = require('request');
var csv       = require("fast-csv");
var ftpclient = require('jsftp');
var moment    = require('moment');
var _         = require('lodash');

var redisclient = redis.createClient();

var pg            = require('pg');
var pg_con_string = "postgres://airq:nach0s@localhost/airquality";




function AirNow(){
  var headers=["valid_date", "valid_time", "aqsid", "sitename", "GMT_offset", "parameter_name", "reporting_units", "value", "data_source"];

  var ftp = new ftpclient({
    host: "ftp.airnowapi.org",
    port: 21,
    user: "leapingleopard",
    pass: "xi7MxOiwrRR_vKeT"
  });

  console.log('Fetching list of hourly files.');

  ftp.list('HourlyData', function(err, res){
    if(err){
      console.error('Error fetching list of hourly files.', err);
      return;
    }

    console.log('Fetched HourlyData file list.');

    res = res.split("\r\n");
    for(var i=0;i<res.length;i++)
      res[i]=res[i].substr(res[i].lastIndexOf(' ')+1);

    var files = [];
    for(var i=0;i<res.length;i++)
      if(res[i].substr(-3)=='dat')
        files.push(res[i]);

    files.sort();

    var download_file=files.slice(-1)[0];

    console.log('Acquiring latest file...');

    ftp.get('HourlyData/'+download_file, function(err,socket){
      if(err){
        console.error('Failed to get latest hourly file.', err);
        return;
      }

      console.log('Latest hourly file acquired.');

      var str = "";
      socket.on("data", function(d) { str += d.toString(); });
      socket.on("close", function(err) {
        if (err){
          console.error('There was an error retrieving the file.', err);
          return;
        }

        var sites = {};

        csv.fromString(str, {headers: headers, delimiter: '|'})
          .on("record", function(data){
              var meas_datetime = data.valid_date+' '+data.valid_time;
              meas_datetime=moment.utc(meas_datetime,'MM/DD/YY HH:mm');
              var param={
                time:  meas_datetime.unix(),
                value: parseFloat(data.value)
              };

              if(typeof(sites[data.aqsid])==='undefined')
                sites[data.aqsid] = {};

             sites[data.aqsid][data.parameter_name.toLowerCase()] = param;
          })
          .on("end", function(){
            _.each(sites, function(site, i){
              redisclient.get('aqs_station-'+i, function(err, stationinfo) {
                if(err){
                  console.error('Error: Could not find station',i);
                  return;
                }

                stationinfo=JSON.parse(stationinfo);

                //console.log('si',stationinfo);

                sites = _.extend(stationinfo, site)

                redisclient.set('aqsmeas-'+i, JSON.stringify(site));
                console.log(sites);
              });
            });
          });

      });

      socket.resume();

    });
  });
}
/*
  request(self.url, function (error, response, body) {
    if(error || response.statusCode!=200){
      console.error('Error fetching AirNow.gov hourly data: ', error);
      deferred.resolve(false)
      return deferred.promise;
    }


};
*/
AirNow();
//setInterval(AirNow, 65*60*1000);
