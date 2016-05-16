const vm = require('vm');

function log(data) {require('fs').appendFileSync('/tmp/log', JSON.stringify(data) + "\n");}
var $$html$$;

// initialize VM context
vm.runInThisContext('(function (_require) {require = _require; co = require("co");})')(require);

var onSuccess = function (data) {
    log('SUCCESS: ')
    log(data);

    if ($$html$$ != undefined) var result = {mime: {"text/html": $$html$$}};
    else var result = {mime: {"text/plain": data != undefined ? JSON.stringify(data) : "undefined"}};
    process.send({status: 'success', data: result});
    $$html$$ = undefined;
}

/*
    onError is raised by catching errors encapsulated by co
    repl is always successful but write error messages to stderr
*/
var onError = function (err) {
    log("ERROR: ");
    log(err);
    console.error(err);
    process.send({status: 'success', data: {mime: {"text/plain": "undefined"}}});     
}

/*
TODO 1:
    source code is encapsulated in co to authorized use of yield, as a consequence scoping of global vars are lost.
    Workaround: declare global variables not as 'var my_var =' but as 'global["my_var"] ='
    Solution: use esprima to parse code, find global variable declaration and automatic substitution of var by global.
TODO 2: To retrieve latest source code result in Jupyter we need to return it inside co encpasulation
    Workaround: write return 'expr' in notebook
    Solution add a return just before the last source code statement 
*/
process.on('message', msg => {
    msg.code = 'co(function *() {' +
        msg.code.replace(/\s*var\s*(\w+)\s*=\s*yield\s/g, (match, p1) => ';' + p1 + '=yield ') + 
        '}).then(onSuccess).catch(onError);';

    if (msg.code) vm.runInThisContext(msg.code);
});
