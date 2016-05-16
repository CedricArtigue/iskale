#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize(['Hello world'])
	.collect()
	.then(function(data) {
		console.log(data);
		sc.end();
	})
