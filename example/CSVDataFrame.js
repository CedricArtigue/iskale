var util = require('util');
var thenify = require('thenify');
var Table = require('cli-table');
// var plot = require('plotter').plot;
var Plot = require('plotly-notebook-js');

function DataFrame(dataset, fields) {
	this.dataset = dataset;
	this.fields = fields;
	this.schema = {};
}

DataFrame.prototype.printHTML = thenify(function(n, done) {
	var style = "<style>table#t01 {width: 100%; background-color: #f1f1c1;}\ntable#t01 tr:nth-child(even) {background-color: #f2f2f3;}\ntable#t01 tr:nth-child(odd) {background-color: #fff;}\ntable#t01 th {color: white;background-color: #44496e;}\n</style>";
	var header = '<tr>', rows = '';
	for (var i in this.fields)
		header += '<th>' + this.fields[i] + '</th>';
	header += '</tr>';
	this.dataset.take(n).then(function(result) {
		for (var r in result) {
			rows += '<tr>';
			for (var c in result[r])
				rows += '<td>' + result[r][c] + '</td>';
			rows += '</tr>';		
		}
		done(null, style + '<table id="t01">' + header + rows + '</table>');
	});
});

DataFrame.prototype.show = thenify(function(n, done) {
	var table = new Table({head: this.fields, colWidths: this.fields.map(n => 12)});
	this.dataset.take(n).then(function(result) {
		result.map(d => table.push(d));
		console.log(table.toString());
		done(null);
	});
});

DataFrame.prototype.extractSchema = thenify(function(done) {
	var self = this, schema = {};
	this.fields.map((f, i) => schema[f] = {idx: i, isReal: true, categories: []});

	function reducer(schema, data) {
		for (var field in schema) {
			var idx = schema[field].idx, value = data[idx];
			if (isNaN(Number(value))) {
				schema[field].isReal = false;
				if (schema[field].categories.indexOf(value) == -1)
					schema[field].categories.push(value);
			}
		}
		return schema;
	}

	function combiner(schema1, schema2) {
		for (var field in schema1) {
			if (schema1[field].isReal == undefined) schema1[field].isReal = schema2[field].isReal
			else schema1[field].isReal = schema1[field].isReal && schema2[field].isReal;
			for (var i in schema2[field].categories)
				if (schema1[field].categories.indexOf(schema2[field].categories[i]) == -1)
					schema1[field].categories.push(schema2[field].categories[i])
		}
		return schema1;
	}

	this.dataset.aggregate(reducer, combiner, schema).then(function(schema) {
		for (var i in schema) self.schema[i] = schema[i];	// apply schema
		done(null, schema);
	})
});

// Ploting distribution as feature_name.png
// il faut renvoyer un tableau avec colonne = min, max, mean, stddev, type(real, categorical)
// DataFrame.prototype.describe = thenify(function(field, done) {
// 	var idx = this.fields.indexOf(field);
// 	this.dataset.map((data, idx) => data[idx], idx)
// 		.map(function(feature) {return isNaN(Number(feature)) ? feature : Number(feature);})
// 		.countByValue().then(function(tmp) {
// 			if (isNaN(Number(tmp[0][0]))) {					// Discrete feature
// 				tmp.sort(function(a, b) {return b[1] - a[1]});	// Sort descent
// 				var xy = [];
// 				for (var i in tmp) xy[i] = tmp[i][1];
// 			} else {											// Continuous feature
// 				tmp.sort();
// 				var xy = {};
// 				for (var i in tmp) xy[tmp[i][0]] = tmp[i][1];
// 			}

// 			var data = {'': xy};

// 			plot({
// 				title: field + ' distribution',
// 				data: data,
// 				style: 'boxes',
// 				filename: field + '.png',
// 				finish: function() {
// 					console.log('Creating ' + field + '.png');
// 					done(null);
// 				}
// 			});
// 		});
// });

DataFrame.prototype.plotDistribution = thenify(function(field, done) {
	var idx = this.fields.indexOf(field);
	this.dataset.map((data, idx) => data[idx], idx)
		.map(function(feature) {return isNaN(Number(feature)) ? feature : Number(feature);})
		.countByValue().then(function(tmp) {
			var series = [{x: [], y: [], type: 'bar', name: field}];

			if (isNaN(Number(tmp[0][0]))) {					// Discrete feature
				tmp.sort(function(a, b) {return b[1] - a[1]});	// Sort descent
				series[0].tickmode = 'array';
				var ticktext = [], tickvals = [];
				for (var i in tmp) {
				    series[0].x.push(Number(i));
				    series[0].y.push(tmp[i][1]);
				    tickvals.push(Number(i));
				    ticktext.push(tmp[i][0]);				    
				}
				done(null, Plot.createPlot(series, {
					autotick: false,
					title: field + ' distribution',
					xaxis: {tickvals: tickvals, ticktext: ticktext}
				}).render());
			} else {											// Continuous feature
				tmp.sort();
				for (var i in tmp) {
				    series[0].x.push(tmp[i][0]);
				    series[0].y.push(tmp[i][1]);
				}				
				done(null, Plot.createPlot(series, {title: field + ' distribution'}).render());
			}
		});
});

DataFrame.prototype.number_encode_features = function() {
	function reducer(schema, data) {
		for (var field in schema) {
			var idx = schema[field].idx, value = data[idx];
			if (isNaN(Number(value))) {
				schema[field].isReal = false;
				if (schema[field].categories.indexOf(value) == -1)
					schema[field].categories.push(value);
			}
		}
		return schema;
	}

	function combiner(schema1, schema2) {
		for (var field in schema1) {
			if (schema1[field].isReal == undefined) schema1[field].isReal = schema2[field].isReal
			else schema1[field].isReal = schema1[field].isReal && schema2[field].isReal;
			for (var i in schema2[field].categories)
				if (schema1[field].categories.indexOf(schema2[field].categories[i]) == -1)
					schema1[field].categories.push(schema2[field].categories[i])
		}
		return schema1;
	}

	var schema = {};
	this.fields.map((f, i) => schema[f] = {idx: i, isReal: true, categories: []});
	var dataset = this.dataset
		.map(a => [1, a])
		.aggregateByKey(reducer, combiner, schema)
		.map(a => a[1])
		.cartesian(this.dataset)
		.map(function(data) {
			var schema = data[0], features = data[1];
			var tmp = [];
			for (var field in schema) {
				var value = features[schema[field].idx];
				tmp.push(schema[field].isReal ? Number(value) : schema[field].categories.indexOf(value));
			}
			return tmp;			
		});
	
	return new DataFrame(dataset, this.fields);
};

DataFrame.prototype.select = function(fields) {
	if (!Array.isArray(fields)) throw new Error('DataFrame.select(): fields argument must be an instance of Array.');
	var fields_idx = [];
	for (var i in fields) {
		var idx = this.fields.indexOf(fields[i]);
		if (idx == -1) throw new Error('DataFrame.select(): field ' + fields[i] + ' does not exist.');
		fields_idx.push(idx);
	}

	return new DataFrame(this.dataset
		.map(function(data, fields_idx) {
			var tmp = [];
			for (var i in fields_idx) tmp.push(data[fields_idx[i]]);
			return tmp;
	}, fields_idx), fields);
}

DataFrame.prototype.drop = function(fields) {
	for (var i in fields)
		if (this.fields.indexOf(fields[i]) == -1)
			throw new Error('DataFrame.drop(): field ' + fields[i] + ' does not exist.')

	var newFields = [], newFields_idx = [];
	for (var i in this.fields)
		if (fields.indexOf(this.fields[i]) == -1) {
			newFields_idx.push(i);
			newFields.push(this.fields[i]);
		}

	return new DataFrame(this.dataset.map(function(data, newFields_idx) {
		var tmp = [];
		for (var i in newFields_idx) tmp.push(data[newFields_idx[i]]);
		return tmp;
	}, newFields_idx), newFields);
}

DataFrame.prototype.toLabeledPoint = function(label, features) {
	// Check if features is an array
	if (!Array.isArray(features))
		throw new Error('DataFrame.toLabeledPoint(): features argument must be an instance of Array.');
	// Check if label et features exist in data frame fields
	if (this.fields.indexOf(label) == -1) 
		throw new Error('toLabeledPoint(): field ' + label + ' does not exist.')
	for (var i in features)
		if ((features[i] != '*') && this.fields.indexOf(features[i]) == -1) 
			throw new Error('toLabeledPoint(): field ' + features[i] + ' does not exist.')
	// check if label is not in features
	if (features.indexOf(label) != -1)
		throw new Error('toLabeledPoint(): features must not include label.')
	// if * is used as features, build a vector containing all fields except label
	var tmp = [];
	if ((features.length == 1) && (features[0] == "*")) {
		for (var i in this.fields)
			if (this.fields[i] != label) tmp.push(this.fields[i]);
		features = tmp;
	}

	return this.dataset.map(function(data, args) {
		var features = [];
		for (var i in args.features)
			features.push(Number(data[args.fields.indexOf(args.features[i])]));
		return [data[args.fields.indexOf(args.label)] * 2 - 1, features]	// ICI on force à -1/1
	}, {fields: this.fields, label: label, features: features})
}

DataFrame.prototype.take = thenify(function(n, done) {
	this.dataset.take(n, done);
});

function CSVDataFrame(sc, fields, file, sep, na_values) {
	DataFrame.call(this, sc.textFile(file)
		.map((line, sep) => line.split(sep).map(str => str.trim()), sep)			// split csv lines on separator
		.filter((data, na_values) => data.indexOf(na_values) == -1, na_values),		// ignore lines containing na_values
	fields);

	// DataFrame.call(this, sc.textFile(file).map(CSVtoArray), fields);

	// function CSVtoArray (csvString) {
	//     var fieldEndMarker  = /([,\015\012] *)/g; /* Comma is assumed as field separator */
	//     var qFieldEndMarker = /("")*"([,\015\012] *)/g; /* Double quotes are assumed as the quote character */
	//     var startIndex = 0;
	//     var records = [], currentRecord = [];
	//     do {
	//         // If the to-be-matched substring starts with a double-quote, use the qFieldMarker regex, otherwise use fieldMarker.
	//         var endMarkerRE = (csvString.charAt (startIndex) == '"')  ? qFieldEndMarker : fieldEndMarker;
	//         endMarkerRE.lastIndex = startIndex;
	//         var matchArray = endMarkerRE.exec (csvString);
	//         if (!matchArray || !matchArray.length) {
	//             break;
	//         }
	//         var endIndex = endMarkerRE.lastIndex - matchArray[matchArray.length-1].length;
	//         var match = csvString.substring (startIndex, endIndex);
	//         if (match.charAt(0) == '"') { // The matching field starts with a quoting character, so remove the quotes
	//             match = match.substring (1, match.length-1).replace (/""/g, '"');
	//         }
	//         currentRecord.push (match);
	//         var marker = matchArray[0];
	//         if (marker.indexOf (',') < 0) { // Field ends with newline, not comma
	//             records.push (currentRecord);
	//             currentRecord = [];
	//         }
	//         startIndex = endMarkerRE.lastIndex;
	//     } while (true);
	//     if (startIndex < csvString.length) { // Maybe something left over?
	//         var remaining = csvString.substring (startIndex).trim();
	//         if (remaining) currentRecord.push (remaining);
	//     }
	//     if (currentRecord.length > 0) { // Account for the last record
	//         records.push (currentRecord);
	//     }
	//     return records[0];
	// };

	// this.select2 = thenify(function(name, number, done) {
	// 	var idx = self.features.indexOf(name), tmp = [];
	// 	self.data.count().on('data', function(count) {
	// 		self.data
	// 			.map((data, args) => data[args.idx], {idx: idx})
	// 			.map(function(feature) {return isNaN(Number(feature)) ? feature : Number(feature);})
	// 			.countByValue()
	// 			.on('data', function(data) {
	// 				tmp.push([data[0], Math.round(data[1] / count * 1000000 ) / 1000000]);
	// 			})
	// 			.on('end', function() {
	// 				tmp.sort(function(a, b) {return b[1] - a[1]});	// Sort descent
	// 				tmp = tmp.slice(0, number);
	// 				var table = new Table({head: [name, 'Percentage'], colWidths: [20, 20]});
	// 				tmp.map(d => table.push(d));
	// 				console.log(table.toString());
	// 				done(null);
	// 			});
	// 		});
	// });
}

util.inherits(CSVDataFrame, DataFrame);

module.exports = CSVDataFrame;