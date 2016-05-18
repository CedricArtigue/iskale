var vm = require('vm');
var esprima = require('esprima');

function log(data) {require('fs').appendFileSync('/tmp/log', JSON.stringify(data) + "\n");}
var $$html$$, $$png$$;

// initialize VM context
vm.runInThisContext('(function (_require) {require = _require; co = require("co");})')(require);

var onSuccess = function (data) {
    log('SUCCESS: ')
    log(data);

    if ($$html$$ != undefined) var result = {mime: {"text/html": $$html$$}};
    else if ($$png$$ != undefined) var result = {mime: {"image/png": $$png$$}};
    else var result = {mime: {"text/plain": data != undefined ? JSON.stringify(data) : "undefined"}};
    process.send({status: 'success', data: result});
    $$html$$ = undefined;
    $$png$$ = undefined;
}

/*
    onError is raised by catching errors encapsulated by co
    repl is always successful from kernel side but write error messages to stderr
*/
var onError = function (err) {
    log("ERROR: ");
    log(err);
    console.error(err);
    process.send({status: 'success', data: {mime: {"text/plain": "undefined"}}});     
}

/*
TODO 2: To retrieve latest source code result in Jupyter we need to return it inside co encpasulation
    Workaround: write return 'expr' in notebook
    Solution add a return just before the last source code statement 
*/
process.on('message', msg => {
    // msg.code = 'co(function *() {' +
    //     msg.code.replace(/\s*var\s*(\w+)\s*=\s*yield\s/g, (match, p1) => ';' + p1 + '=yield ') + 
    //     '}).then(onSuccess).catch(onError);';

    msg.code = 'co(function *() {' + msg.code + '}).then(onSuccess).catch(onError);';
    msg.code = transpile(msg.code);
    if (msg.code) vm.runInThisContext(msg.code);
});

function transpile(code) {
    var ast = esprima.parse(code, {range: true, sourceType: 'script'});
    var tree = ast.body[0].expression.callee.object.callee.object.arguments[0].body.body;
    var transpiled_code = '';
    var cursor = 0;

    for (var i in tree) {
        if (tree[i].type == "VariableDeclaration") {
            var begin = tree[i].range[0];
            var end = tree[i].declarations[0].range[0];
            transpiled_code += code.substr(cursor, begin - cursor);
            transpiled_code += 'global.';
            cursor = end;
        }       
    }   
    transpiled_code += code.substr(cursor); 
    return transpiled_code;
}
