var vent = {}; // or App.vent depending how you want to do this
_.extend(vent, Backbone.Events);

var AppConfig = {
	station_url: 'http://localhost:4730/v0/stationNear/:lat/:lon?limit=5',
  masurements_url: 'http://localhost:4730/v0/measurements/:stationid',
	bounds_url:  'http://localhost:4730/bounds/:north/:south/:east/:west'
};

var MapView = Backbone.View.extend({
  el: '#mapview',

  events: {
    "click #flipleft":  "flipLeft",
    "click #flipright": "flipRight",
  },

  initialize: function(){
    var self=this;
    this.stations = {};

    self.default_marker_img  = 'img/blue-pin.png';
    self.selected_marker_img = 'img/black-pin.png';

		var map_options = {
      center:             new google.maps.LatLng(47.57, -122.31),
      zoom:               7,
      mapTypeId:          google.maps.MapTypeId.ROADMAP,
      panControl:         false,
      mapTypeControl:     false,
      zoomControl:        true,
      zoomControlOptions: { position: google.maps.ControlPosition.LEFT_CENTER },
      streetViewControl:  false
		};
		this.map = new google.maps.Map(document.getElementById("map-canvas"), map_options);

		//idle event fires once when the user stops panning/zooming
		google.maps.event.addListener( this.map, "idle", this.mapBoundsChanged.bind(this) );

    //this.spiderfy = new OverlappingMarkerSpiderfier(this.map, {keepSpiderfied:true, nearbyDistance:10});

    //this.spiderfy.addListener('click', function(marker, event) {
    //  self.markerClicked(marker);
    //});
  },

  mapBoundsChanged: function(){
    var bounds  = this.map.getBounds();
    var mcenter = this.map.getCenter();

    var ne        = bounds.getNorthEast();
    var sw        = bounds.getSouthWest();
    var boundsobj = {n:ne.lat(),s:sw.lat(),e:ne.lng(),w:sw.lng(), centerLat: mcenter.lat(), centerLng: mcenter.lng() };
    var self      = this;

    //Get locations of stops which are visible
    var station_url = AppConfig.station_url
                        .replace(':lat', mcenter.lat())
                        .replace(':lon', mcenter.lng())
    										.replace(':north', ne.lat())
    										.replace(':south', sw.lat())
    										.replace(':east', ne.lng())
    										.replace(':west', sw.lng());
    $.get(station_url, {}, function(data, textStatus, jqXHR) {
	    for(var j=0, len=data.length; j<len; j++) {
	      self.addStation(data[j]);
	    }
    });
  },

  addStation: function(new_station) {
    var self = this;

    //Search stops array to see if an object for this stop is already present
    var look_up=false;
    for(var i in this.stations){
      if( this.stations[i].id == new_station.id ){
        look_up=i;
        break;
      }
    }

    //Does a marker for this stop already exist on the map?
    if(look_up!==false) {
      return; //Yes, it already has a marker. Don't make another!
    }

    console.log('adding station', new_station);



    //Make a new marker
    var marker = new google.maps.Marker({
      position:    new google.maps.LatLng(new_station.lat, new_station.lon),
      map:         this.map,
      draggable:   false,
      icon:        self.default_marker_img,
      //animation: google.maps.Animation.DROP,
      stationid:   new_station.stationid,
      zIndex:      1,
      //visible:   !self.markers_hidden,
      //optimized: false
    });

    marker.station = new_station;

		google.maps.event.addListener(marker, 'click', function() {
		 	self.markerClicked(marker);
		});

    //if(look_up) //Already present in stops array
    //  this.stops[look_up].marker = marker;
    //else {  //The stop is not in the array, so add it
      new_station.marker = marker;
      //this.stations.push(new_station);
    //}
  },

  markerClicked: function(marker) {
    var self = this;

    //Reset old marker to have its normal icon back
    if (this.selected_marker){
    	this.selected_marker.set("color","#ff0000");
    	this.selected_marker.setIcon(self.default_marker_img);
      this.selected_marker.setOptions({zIndex:1});
      //this.selected_marker.setIcon( AppConfig.transit_mode_icons[this.selected_marker.stop_type].gicon );
    }

    //Set this marker to use its hover icon
    this.selected_marker=marker;
    this.selected_marker.setIcon(self.selected_marker_img);

    vent.trigger("data", marker.stationid);
  },

});


//var testseries=[{date:"05/21/14 16:00", measurement:49}, {date:"05/21/14 17:00", measurement:50}, {date:"05/21/14 18:00", measurement:54}, {date:"05/21/14 19:00", measurement:53}, {date:"05/21/14 20:00", measurement:52}, {date:"05/21/14 21:00", measurement:51}, {date:"05/21/14 22:00", measurement:49}, {date:"05/21/14 23:00", measurement:48}, {date:"05/22/14 00:00", measurement:46}, {date:"05/22/14 01:00", measurement:42}, {date:"05/22/14 02:00", measurement:40}, {date:"05/22/14 03:00", measurement:41}, {date:"05/22/14 04:00", measurement:43}, {date:"05/22/14 05:00", measurement:41}, {date:"05/22/14 06:00", measurement:42}, {date:"05/22/14 07:00", measurement:46}, {date:"05/22/14 13:00", measurement:49}, {date:"05/22/14 14:00", measurement:55}, {date:"05/22/14 15:00", measurement:56}, {date:"05/22/14 16:00", measurement:50}, {date:"05/22/14 17:00", measurement:55}, {date:"05/22/14 18:00", measurement:53}, {date:"05/22/14 19:00", measurement:56}, {date:"05/22/14 20:00", measurement:59}, {date:"05/22/14 21:00", measurement:62}, {date:"05/22/14 22:00", measurement:66}, {date:"05/22/14 23:00", measurement:64}, {date:"05/23/14 00:00", measurement:57}, {date:"05/23/14 01:00", measurement:53}, {date:"05/23/14 02:00", measurement:48}, {date:"05/23/14 03:00", measurement:46}, {date:"05/23/14 04:00", measurement:46}, {date:"05/23/14 05:00", measurement:45}, {date:"05/23/14 06:00", measurement:44}, {date:"05/23/14 07:00", measurement:39}, {date:"05/23/14 08:00", measurement:31}, {date:"05/23/14 09:00", measurement:15}, {date:"05/23/14 10:00", measurement:12}, {date:"05/23/14 11:00", measurement:28}, {date:"05/23/14 12:00", measurement:41}, {date:"05/23/14 13:00", measurement:49}, {date:"05/23/14 14:00", measurement:49}, {date:"05/23/14 15:00", measurement:49}, {date:"05/23/14 16:00", measurement:52}, {date:"05/23/14 17:00", measurement:51}, {date:"05/23/14 18:00", measurement:50}, {date:"05/23/14 19:00", measurement:50}, {date:"05/23/14 20:00", measurement:49}, {date:"05/23/14 21:00", measurement:51}, {date:"05/23/14 22:00", measurement:54}, {date:"05/23/14 23:00", measurement:52}, {date:"05/24/14 00:00", measurement:48}, {date:"05/24/14 01:00", measurement:43}, {date:"05/24/14 02:00", measurement:35}, {date:"05/24/14 03:00", measurement:32}, {date:"05/24/14 04:00", measurement:31}, {date:"05/24/14 05:00", measurement:33}, {date:"05/24/14 06:00", measurement:36}, {date:"05/24/14 07:00", measurement:38}, {date:"05/24/14 08:00", measurement:37}, {date:"05/24/14 09:00", measurement:37}, {date:"05/24/14 10:00", measurement:35}, {date:"05/24/14 11:00", measurement:34}, {date:"05/24/14 12:00", measurement:36}, {date:"05/24/14 13:00", measurement:38}, {date:"05/24/14 14:00", measurement:39}, {date:"05/24/14 15:00", measurement:41}, {date:"05/24/14 16:00", measurement:41}, {date:"05/24/14 17:00", measurement:42}, {date:"05/24/14 18:00", measurement:43}, {date:"05/24/14 19:00", measurement:44}, {date:"05/24/14 20:00", measurement:46}, {date:"05/24/14 21:00", measurement:48}, {date:"05/24/14 22:00", measurement:49}, {date:"05/24/14 23:00", measurement:49}, {date:"05/25/14 00:00", measurement:50}, {date:"05/25/14 01:00", measurement:45}, {date:"05/25/14 02:00", measurement:39}, {date:"05/25/14 03:00", measurement:41}, {date:"05/25/14 04:00", measurement:35}, {date:"05/25/14 05:00", measurement:36}, {date:"05/25/14 06:00", measurement:24}, {date:"05/25/14 07:00", measurement:15}, {date:"05/25/14 08:00", measurement:10}, {date:"05/25/14 09:00", measurement:7}, {date:"05/25/14 10:00", measurement:7}, {date:"05/25/14 11:00", measurement:9}, {date:"05/25/14 12:00", measurement:20}, {date:"05/25/14 13:00", measurement:42}, {date:"05/25/14 14:00", measurement:48}, {date:"05/25/14 15:00", measurement:51}, {date:"05/25/14 16:00", measurement:54}, {date:"05/25/14 17:00", measurement:55}, {date:"05/25/14 18:00", measurement:53}, {date:"05/25/14 19:00", measurement:53}, {date:"05/25/14 20:00", measurement:53}, {date:"05/25/14 21:00", measurement:52}, {date:"05/25/14 22:00", measurement:54}, {date:"05/25/14 23:00", measurement:55}, {date:"05/26/14 00:00", measurement:56}, {date:"05/26/14 01:00", measurement:42}, {date:"05/26/14 02:00", measurement:35}, {date:"05/26/14 03:00", measurement:27}, {date:"05/26/14 04:00", measurement:23}, {date:"05/26/14 05:00", measurement:17}, {date:"05/26/14 06:00", measurement:13}, {date:"05/26/14 07:00", measurement:13}, {date:"05/26/14 08:00", measurement:10}, {date:"05/26/14 09:00", measurement:8}, {date:"05/26/14 10:00", measurement:8}, {date:"05/26/14 11:00", measurement:8}, {date:"05/26/14 12:00", measurement:27}, {date:"05/26/14 13:00", measurement:48}, {date:"05/26/14 14:00", measurement:57}, {date:"05/26/14 15:00", measurement:63}, {date:"05/26/14 16:00", measurement:64}, {date:"05/26/14 17:00", measurement:61}, {date:"05/26/14 18:00", measurement:59}, {date:"05/26/14 19:00", measurement:58}, {date:"05/26/14 20:00", measurement:62}, {date:"05/26/14 21:00", measurement:66}, {date:"05/26/14 22:00", measurement:68}, {date:"05/26/14 23:00", measurement:67}, {date:"05/27/14 00:00", measurement:63}, {date:"05/27/14 01:00", measurement:58}, {date:"05/27/14 02:00", measurement:53}, {date:"05/27/14 03:00", measurement:47}, {date:"05/27/14 04:00", measurement:46}, {date:"05/27/14 05:00", measurement:49}, {date:"05/27/14 06:00", measurement:51}, {date:"05/27/14 07:00", measurement:48}, {date:"05/27/14 08:00", measurement:47}, {date:"05/27/14 09:00", measurement:45}, {date:"05/27/14 10:00", measurement:43}, {date:"05/27/14 11:00", measurement:42}, {date:"05/27/14 12:00", measurement:46}, {date:"05/27/14 13:00", measurement:47}, {date:"05/27/14 14:00", measurement:49}, {date:"05/27/14 15:00", measurement:56}, {date:"05/27/14 16:00", measurement:62}, {date:"05/27/14 17:00", measurement:62}, {date:"05/27/14 18:00", measurement:60}, {date:"05/27/14 19:00", measurement:60}, {date:"05/27/14 20:00", measurement:49}, {date:"05/27/14 21:00", measurement:43}, {date:"05/27/14 22:00", measurement:35}, {date:"05/27/14 23:00", measurement:35}, {date:"05/28/14 00:00", measurement:38}, {date:"05/28/14 01:00", measurement:37}, {date:"05/28/14 02:00", measurement:39}, {date:"05/28/14 03:00", measurement:42}, {date:"05/28/14 04:00", measurement:38}, {date:"05/28/14 05:00", measurement:35}, {date:"05/28/14 06:00", measurement:35}, {date:"05/28/14 07:00", measurement:31}, {date:"05/28/14 08:00", measurement:33}, {date:"05/28/14 09:00", measurement:35}, {date:"05/28/14 10:00", measurement:36}, {date:"05/28/14 11:00", measurement:34}, {date:"05/28/14 12:00", measurement:35}, {date:"05/28/14 13:00", measurement:37}, {date:"05/28/14 14:00", measurement:42}, {date:"05/28/14 15:00", measurement:50}, {date:"05/28/14 16:00", measurement:52}, {date:"05/28/14 18:00", measurement:51}, {date:"05/28/14 19:00", measurement:46}, {date:"05/28/14 20:00", measurement:43}, {date:"05/28/14 21:00", measurement:42}, {date:"05/28/14 22:00", measurement:41}, {date:"05/28/14 23:00", measurement:39}, {date:"05/29/14 00:00", measurement:37}, {date:"05/29/14 01:00", measurement:33}, {date:"05/29/14 02:00", measurement:29}, {date:"05/29/14 03:00", measurement:32}, {date:"05/29/14 04:00", measurement:31}, {date:"05/29/14 05:00", measurement:27}, {date:"05/29/14 06:00", measurement:19}, {date:"05/29/14 07:00", measurement:24}, {date:"05/29/14 08:00", measurement:21}, {date:"05/29/14 09:00", measurement:14}, {date:"05/29/14 10:00", measurement:12}, {date:"05/29/14 11:00", measurement:11}];


var VizView = Backbone.View.extend({
  el: '#vizview',

  events: {
    "click .close":  "hide",
  },

  initialize: function(){
  	this.listenTo(vent, "data", this.displayGraph, this);
    this.listenTo(vent, "vizview:show", this.show, this);
    this.listenTo(vent, "vizview:hide", this.hide, this);
/*
	  var dateline = svg.append('line')
		                    .attr({
		                        'x1': 0,
		                        'y1': 8,
		                        'x2': 0,
		                        'y2': 20
		                    })
		                    .attr("stroke", "black")
		                    .attr('class', 'verticalLine');*/
  },

  show: function(){
    console.log('Showing vizview');
    this.$el.addClass('active');
  },

  hide: function(){
    this.$el.removeClass('active');
  },

    displayGraph: function(station){
    var measurement_data = AppConfig.masurements_url.replace(':stationid', station);

    var chartAttr = {};
    chartAttr.width = 400;
    chartAttr.height = 200;
    chartAttr.offsetX = 30;
    chartAttr.offsetY = 50;

    $.get(measurement_data, {}, function(data, textStatus, jqXHR) {
      //console.log(data);

      vent.trigger('vizview:show');

      //remove the old svg and redraw it
      d3.select("#vizSvg").remove();

      var svg = d3.select("#vizview").append("svg").attr("id", "vizSvg");

      svg.append("text").attr({
        x: 30,
        y: 40,
        'font-size': 30,
        'font-weight': 'bold',
        'font-family': '\'Open Sans\', sans-serif'
      }).classed('stationName', true).text(data.stationInfo.name);

     // console.log(new Date(Date.parse(data.measurements[0].datetime)).getHours());

      var dateRange = d3.extent(data.measurements, function(d){ return Date.parse(d.datetime);});
      var nestedData = d3.nest().key(function(d) {return d.definition.trim();}).entries(data.measurements);

      var x = d3.time.scale().domain(dateRange).rangeRound([0, chartAttr.width]);

      var xAxis = d3.svg.axis()
        .scale(x)
        .orient('bottom')
        .ticks(d3.time.days, 1)
        .tickFormat(d3.time.format('%m/%d'))
        .tickSize(0)
        .tickPadding(8);

      svg.append('g')
        .attr('class', 'x axis')
        .attr('transform', 'translate(' + chartAttr.offsetX + ', ' + (chartAttr.height + chartAttr.offsetY) + ')')
        .call(xAxis)
        .selectAll("text")  
          .style("text-anchor", "end")
          .attr("dx", "-.8em")
          .attr("dy", ".15em")
          .attr("transform", function(d) {
              return "rotate(-65)" ;
              });

      var colorScale = d3.scale.linear().domain([0, nestedData.length]).range(["red", "blue"]);

        for (var i = 0; i < nestedData.length; i++) {
          //we're only going to chart the first dectected thing, keep charting the others in a loop later
          var dataOfInt = nestedData[i].values;
          var maxVal = d3.max(dataOfInt, function(d){return d.value;});
          var y = d3.scale.linear().domain([0, maxVal]).range([0, chartAttr.height]);
          console.log(dataOfInt);

          var chartGrp = svg.append("g");

          var line = d3.svg.line()
              .x(function(d) { return x(Date.parse(d.datetime)); })
              .y(function(d) { return chartAttr.height - y(d.value) + chartAttr.offsetY; });

          chartGrp.append("path").datum(dataOfInt).attr("d", function(d){return line(d);})
          .attr("fill", "none")
          .attr("stroke-width", "1")
          .attr("stroke", colorScale(i)); 
        }
    });
    /*var datavar=[];
    var paramtypes=["OZONE","NO","NO2","NO2Y","NOX","NOY","OC","OZONE","PM10","PM2.5","RHUM","SO2"];
    for(var i=0;i<paramtypes.length;i++){
      var param=paramtypes[i];
      if(typeof(data[station][param])==="undefined") continue;
      var temp={};
      temp.type         = "line";
      temp.xValueType   = "dateTime";
      temp.legendText   = param;
      temp.showInLegend = true;
      temp.dataPoints   = _.map(data[station][param],function(obj){return {x: moment(obj.date).unix()*1000, y: parseFloat(obj.value) }});
      datavar.push(temp);
    }

    console.log(datavar);

    var chart = new CanvasJS.Chart("vizview",
    {
      zoomEnabled: true,
      title:{
      text: stations[station].name
      },
      data: datavar
    });

    chart.render();*/
  }
});



var AppRouter = Backbone.Router.extend({
  routes: {
    "station/:id": "loadBookPage",
  },

  loadBookPage: function(book, page) {
    vent.trigger('loadbook',book,page);
  }
});

var MapView = new MapView();
var vizview = new VizView();

function closeWindow(){
  d3.select("#vizview").attr({
    style: "display: none"
  });
}