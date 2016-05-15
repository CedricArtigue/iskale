#!/usr/bin/env node

var sc = require('skale-engine').context();

sc.parallelize(['Hello world'])
  .collect()
  .on('data', console.log)
  .on('end', sc.end);
