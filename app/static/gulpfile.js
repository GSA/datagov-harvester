const { src, pipe, dest, series, parallel, watch } = require('gulp');
const uswds = require("@uswds/compile");

var browserify = require('browserify');
var source = require('vinyl-source-stream');
var buffer = require('vinyl-buffer');

// file path vars
const paths = {
    js: {
        src: './js/index.js',
        dest: 'assets/js/bundle.js'
    }
}

function jsTask() {
    return browserify(`${paths.js.src}`)
        .transform('babelify', {
            presets: ['@babel/preset-env'],
            plugins: ['@babel/plugin-transform-runtime']
        })
        .bundle()
        .pipe(source(paths.js.dest))
        .pipe(buffer())
        .pipe(dest("./"));
};

const defaultTask = parallel(
    series(
        jsTask,
        uswds.copyAssets,
        uswds.compile,
    )
)

exports.default = defaultTask

// 3. Compile USWDS

/**
* USWDS version
* Set the major version of USWDS you're using
* (Current options are the numbers 2 or 3)
*/
uswds.settings.version = 3;

/**
* Path settings
* Set as many as you need
*/
uswds.paths.dist.css = './assets/uswds/css';
uswds.paths.dist.js = './assets/uswds/js';
uswds.paths.dist.img = './assets/uswds/img';
uswds.paths.dist.fonts = './assets/uswds/fonts';
uswds.paths.dist.theme = './_scss';

/**
* Exports
* Add as many as you need
*/
exports.compile = uswds.compile;
exports.watch = uswds.watch;
exports.init = uswds.init;
exports.copyAll = uswds.copyAll;
exports.copyAssets = uswds.copyAssets;
exports.updateUswds = uswds.updateUswds;
// exports.default = uswds.watch;
