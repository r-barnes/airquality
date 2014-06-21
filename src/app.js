var express    = require('express');
var routes     = require('./routes');
//var background = require('./background/updaters');

var app = express();


// all environments
app.set('port', process.env.PORT || 3001);
//app.set('views', path.join(__dirname, 'views'));
//app.set('view engine', 'jade');
//app.use(express.favicon());
//app.use(express.logger('dev'));
app.use(express.json());
app.use(express.urlencoded());
app.use(express.methodOverride());
app.use(express.compress());
app.use(app.router);
app.enable('trust proxy');
//app.use(express.static(path.join(__dirname, 'public')));

// Listen for any options request
app.options("*", function (req, res) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Requested-With');

  // Finish preflight request.
  res.writeHead(204);
  res.end();
});

//                      Index Route
app.get('/v0/station/:station', routes.station  );
app.get('/v0/stations',         routes.stations );

process.on('uncaughtException', function (error) {
  console.log(error.stack);
});