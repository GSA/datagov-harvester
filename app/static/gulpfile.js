const { src, pipe, dest, series, parallel, watch } = require('gulp');
const uswds = require("@uswds/compile");

const defaultTask = parallel(
    series(
        uswds.compile,
        uswds.copyAssets,
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
